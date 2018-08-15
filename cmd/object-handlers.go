/*
 * Minio Cloud Storage, (C) 2015-2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"crypto/hmac"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	goioutil "io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	snappy "github.com/golang/snappy"
	"github.com/gorilla/mux"
	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/policy"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

// supportedHeadGetReqParams - supported request parameters for GET and HEAD presigned request.
var supportedHeadGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
	"response-content-encoding":    "Content-Encoding",
	"response-content-language":    "Content-Language",
	"response-content-disposition": "Content-Disposition",
}

const (
	compressionAlgorithmV1 = "golang/snappy/LZ77"
)

// setHeadGetRespHeaders - set any requested parameters as response headers.
func setHeadGetRespHeaders(w http.ResponseWriter, reqParams url.Values) {
	for k, v := range reqParams {
		if header, ok := supportedHeadGetReqParams[k]; ok {
			w.Header()[header] = v
		}
	}
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api objectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObject")

	var object, bucket string
	var wg sync.WaitGroup

	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfo = api.CacheAPI().GetObjectInfo
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
			// If the object you request does not exist, the error Amazon S3 returns depends on whether you also have the s3:ListBucket permission.
			// * If you have the s3:ListBucket permission on the bucket, Amazon S3 will return an HTTP status code 404 ("no such key") error.
			// * if you don’t have the s3:ListBucket permission, Amazon S3 will return an HTTP status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.Args{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, ""),
				IsOwner:         false,
			}) {
				_, err := getObjectInfo(ctx, bucket, object)
				if toAPIErrorCode(err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	objInfo, err := getObjectInfo(ctx, bucket, object)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	if objectAPI.IsEncryptionSupported() {
		if apiErr, _ := DecryptObjectInfo(&objInfo, r.Header); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL)
			return
		}
	}

	var actualSize int64
	var compressedSize int64
	if objInfo.IsCompressed() {
		// Read the decompressed size from the meta.json.
		actualSize = objInfo.GetActualSize()
		if actualSize < 0 {
			writeErrorResponse(w, ErrInvalidDecompressedSize, r.URL)
			return
		}
		// Preserve the compressed size.
		compressedSize = objInfo.Size
		// Set the info.Size to the actualSize.
		objInfo.Size = actualSize
	}

	// Get request range.
	var hrange *httpRange
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if hrange, err = parseRequestRange(rangeHeader, objInfo.Size); err != nil {
			// Handle only errInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == errInvalidRange {
				writeErrorResponse(w, ErrInvalidRange, r.URL)
				return
			}

			// log the error.
			logger.LogIf(ctx, err)
		}
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Get the object.
	var startOffset int64
	length := objInfo.Size
	if hrange != nil {
		startOffset = hrange.offsetBegin
		length = hrange.getLength()
	}

	getObject := objectAPI.GetObject
	if api.CacheAPI() != nil && !hasSSECustomerHeader(r.Header) {
		getObject = api.CacheAPI().GetObject
	}

	var writer io.Writer

	if objInfo.IsCompressed() {
		var pipeErr error

		// Defaulting the length and startOffset.
		// Incase of range based queries, the entire data is read to the pipeWriter.
		// So that range is set on the decompressed data.
		snappyStartOffset := startOffset
		snappyLength := length
		length = compressedSize
		startOffset = 0

		// Validate the range.
		if hrange != nil {
			// For negative length we read everything.
			if snappyLength < 0 {
				snappyLength = actualSize - snappyStartOffset
			}

			// Reply back invalid range if the input offset and length fall out of range.
			if snappyStartOffset > actualSize || snappyStartOffset+snappyLength > actualSize {
				writeErrorResponse(w, ErrInvalidRange, r.URL)
				return
			}
		}
		objInfo.Size = snappyLength
		setObjectHeaders(w, objInfo, hrange)
		setHeadGetRespHeaders(w, r.URL.Query())

		// Open a pipe for compression
		// Where pipeWriter is actually passed to the getObject
		pipeReader, pipeWriter := io.Pipe()
		snappyReader := snappy.NewReader(pipeReader)

		go func() {
			wg.Add(1)
			defer wg.Done()
			if hrange != nil {
				// Discarding the data till the startOffset, if the range is enabled.
				// goioutil.Discard imitates a dummy writer.
				if _, pipeErr = io.CopyN(goioutil.Discard, snappyReader, snappyStartOffset); pipeErr != nil {
					return
				}
			}
			// Finally, writes to the client.
			// The limit is set to the decompressed size if the range is disabled.
			if _, pipeErr = io.CopyN(w, snappyReader, snappyLength); pipeErr != nil {
				return
			}
			// Close the pipe writer if the data is read already.
			// This will happen incase of range queries, If the range is satisfied.
			// Closing the pipe, releases the writer passed to the getObject.
			pipeWriter.Close()
		}()
		writer = pipeWriter
	} else {
		writer = w
		setObjectHeaders(w, objInfo, hrange)
		setHeadGetRespHeaders(w, r.URL.Query())
	}
	if objectAPI.IsEncryptionSupported() {
		if hasSSECustomerHeader(r.Header) {
			// Response writer should be limited early on for decryption upto required length,
			// additionally also skipping mod(offset)64KiB boundaries.
			writer = ioutil.LimitedWriter(writer, startOffset%(64*1024), length)

			writer, startOffset, length, err = DecryptBlocksRequest(writer, r, bucket, object, startOffset, length, objInfo, false)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			w.Header().Set(SSECustomerAlgorithm, r.Header.Get(SSECustomerAlgorithm))
			w.Header().Set(SSECustomerKeyMD5, r.Header.Get(SSECustomerKeyMD5))
		}
	}

	statusCodeWritten := false
	httpWriter := ioutil.WriteOnClose(writer)

	if hrange != nil && hrange.offsetBegin > -1 {
		statusCodeWritten = true
		w.WriteHeader(http.StatusPartialContent)
	}

	// Reads the object at startOffset and writes to objectWriter.
	if err = getObject(ctx, bucket, object, startOffset, length, httpWriter, objInfo.ETag); err != nil {
		if objInfo.IsCompressed() {
			httpWriter.Close()
			wg.Wait()
		} else {
			httpWriter.Close()
		}
		if !httpWriter.HasWritten() && !statusCodeWritten { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		}
		return
	}
	if err = httpWriter.Close(); err != nil {
		if !httpWriter.HasWritten() && !statusCodeWritten { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}

	if objInfo.IsCompressed() {
		wg.Wait() // Wait till the copy go-routines retire.
	}

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a GET request.
	sendEvent(eventArgs{
		EventName:  event.ObjectAccessedGet,
		BucketName: bucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       host,
		Port:       port,
	})
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api objectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadObject")

	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfo = api.CacheAPI().GetObjectInfo
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
			// If the object you request does not exist, the error Amazon S3 returns depends on whether you also have the s3:ListBucket permission.
			// * If you have the s3:ListBucket permission on the bucket, Amazon S3 will return an HTTP status code 404 ("no such key") error.
			// * if you don’t have the s3:ListBucket permission, Amazon S3 will return an HTTP status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.Args{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, ""),
				IsOwner:         false,
			}) {
				_, err := getObjectInfo(ctx, bucket, object)
				if toAPIErrorCode(err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponseHeadersOnly(w, s3Error)
		return
	}

	objInfo, err := getObjectInfo(ctx, bucket, object)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(err))
		return
	}

	if objInfo.IsCompressed() {
		// Read the decompressed size from the meta.json.
		actualSize := objInfo.GetActualSize()
		if actualSize < 0 {
			writeErrorResponse(w, ErrInvalidDecompressedSize, r.URL)
			return
		}
		// Set the info.Size to the actualSize.
		objInfo.Size = actualSize
	}

	if objectAPI.IsEncryptionSupported() {
		if apiErr, encrypted := DecryptObjectInfo(&objInfo, r.Header); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL)
			return
		} else if encrypted {
			if _, err = DecryptRequest(w, r, bucket, object, objInfo.UserDefined); err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			w.Header().Set(SSECustomerAlgorithm, r.Header.Get(SSECustomerAlgorithm))
			w.Header().Set(SSECustomerKeyMD5, r.Header.Get(SSECustomerKeyMD5))
		}
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Set standard object headers.
	setObjectHeaders(w, objInfo, nil)

	// Set any additional requested response headers.
	setHeadGetRespHeaders(w, r.URL.Query())

	// Successful response.
	w.WriteHeader(http.StatusOK)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a HEAD request.
	sendEvent(eventArgs{
		EventName:  event.ObjectAccessedHead,
		BucketName: bucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       host,
		Port:       port,
	})
}

// Extract metadata relevant for an CopyObject operation based on conditional
// header values specified in X-Amz-Metadata-Directive.
func getCpObjMetadataFromHeader(ctx context.Context, r *http.Request, userMeta map[string]string) (map[string]string, error) {
	// Make a copy of the supplied metadata to avoid
	// to change the original one.
	defaultMeta := make(map[string]string, len(userMeta))
	for k, v := range userMeta {
		defaultMeta[k] = v
	}

	// if x-amz-metadata-directive says REPLACE then
	// we extract metadata from the input headers.
	if isMetadataReplace(r.Header) {
		return extractMetadata(ctx, r)
	}

	// if x-amz-metadata-directive says COPY then we
	// return the default metadata.
	if isMetadataCopy(r.Header) {
		return defaultMeta, nil
	}

	// Copy is default behavior if not x-amz-metadata-directive is set.
	return defaultMeta, nil
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
func (api objectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CopyObject")

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, dstBucket, dstObject); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// TODO: Reject requests where body/payload is present, for now we don't even read it.

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := path2BucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL)
		return
	}

	// Check if metadata directive is valid.
	if !isMetadataDirectiveValid(r.Header) {
		writeErrorResponse(w, ErrInvalidMetadataDirective, r.URL)
		return
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	srcInfo, err := objectAPI.GetObjectInfo(ctx, srcBucket, srcObject)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, dstBucket, dstObject); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	if objectAPI.IsEncryptionSupported() {
		if apiErr, _ := DecryptCopyObjectInfo(&srcInfo, r.Header); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL)
			return
		}
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if checkCopyObjectPreconditions(w, r, srcInfo) {
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(srcInfo.Size) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	// We have to copy metadata only if source and destination are same.
	// this changes for encryption which can be observed below.
	if cpSrcDstSame {
		srcInfo.metadataOnly = true
	}

	var writer io.WriteCloser = pipeWriter
	var reader io.Reader = pipeReader

	srcInfo.Reader, err = hash.NewReader(reader, srcInfo.Size, "", "", srcInfo.Size)
	if err != nil {
		pipeWriter.CloseWithError(err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Save the original size for later use when we want to copy
	// encrypted file into an unencrypted one.
	size := srcInfo.Size
	var actualSize int64
	if srcInfo.IsCompressed() {
		// Validate the fitness of decompression.
		actualSize = srcInfo.GetActualSize()
		if actualSize < 0 {
			writeErrorResponse(w, ErrInvalidDecompressedSize, r.URL)
			return
		}
		srcInfo.UserDefined[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(actualSize, 10)
	}

	var encMetadata = make(map[string]string)
	if objectAPI.IsEncryptionSupported() {
		var oldKey, newKey []byte
		sseCopyC := hasSSECopyCustomerHeader(r.Header)
		sseC := hasSSECustomerHeader(r.Header)
		if sseC {
			newKey, err = ParseSSECustomerRequest(r)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
		// AWS S3 implementation requires us to only rotate keys
		// when/ both keys are provided and destination is same
		// otherwise we proceed to encrypt/decrypt.
		if sseCopyC && sseC && cpSrcDstSame {
			// Get the old key which needs to be rotated.
			oldKey, err = ParseSSECopyCustomerRequest(r)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			for k, v := range srcInfo.UserDefined {
				encMetadata[k] = v
			}
			if err = rotateKey(oldKey, newKey, srcBucket, srcObject, encMetadata); err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			// Since we are rotating the keys, make sure to update the metadata.
			srcInfo.metadataOnly = true
		} else {
			if sseCopyC {
				// Source is encrypted make sure to save the encrypted size.
				writer = ioutil.LimitedWriter(writer, 0, srcInfo.Size)
				writer, srcInfo.Size, err = DecryptAllBlocksCopyRequest(writer, r, srcBucket, srcObject, srcInfo)
				if err != nil {
					pipeWriter.CloseWithError(err)
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
				if sseC {
					size = srcInfo.Size
				}
			}
			if sseC {
				reader, err = newEncryptReader(reader, newKey, dstBucket, dstObject, encMetadata)
				if err != nil {
					pipeWriter.CloseWithError(err)
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
				if !sseCopyC {
					size = srcInfo.EncryptedSize()
				}
			}
			srcInfo.Reader, err = hash.NewReader(reader, size, "", "", srcInfo.Size) // do not try to verify encrypted content
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}
	srcInfo.Writer = writer

	srcInfo.UserDefined, err = getCpObjMetadataFromHeader(ctx, r, srcInfo.UserDefined)
	if err != nil {
		pipeWriter.CloseWithError(err)
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	for k, v := range encMetadata {
		srcInfo.UserDefined[k] = v
	}

	// Check if x-amz-metadata-directive was not set to REPLACE and source,
	// desination are same objects. Apply this restriction also when
	// metadataOnly is true indicating that we are not overwriting the object.
	// if encryption is enabled we do not need explicit "REPLACE" metadata to
	// be enabled as well - this is to allow for key-rotation.
	if !isMetadataReplace(r.Header) && srcInfo.metadataOnly && !srcInfo.IsEncrypted() {
		pipeWriter.CloseWithError(fmt.Errorf("invalid copy dest"))
		// If x-amz-metadata-directive is not set to REPLACE then we need
		// to error out if source and destination are same.
		writeErrorResponse(w, ErrInvalidCopyDest, r.URL)
		return
	}

	var objInfo ObjectInfo

	// Checks if a remote putobject call is needed for CopyObject operation
	// 1. If source and destination bucket names are same, it means no call needed to etcd to get destination info
	// 2. If destination bucket doesn't exist locally, only then a etcd call is needed
	var isRemoteCallRequired = func(ctx context.Context, src, dst string, objAPI ObjectLayer) bool {
		if src == dst {
			return false
		}
		_, berr := objAPI.GetBucketInfo(ctx, dst)
		return berr == toObjectErr(errVolumeNotFound, dst)
	}

	// Returns a minio-go Client configured to access remote host described by destDNSRecord
	// Applicable only in a federated deployment
	var getRemoteInstanceClient = func(host string, port int) (*miniogo.Core, error) {
		// In a federated deployment, all the instances share config files and hence expected to have same
		// credentials. So, access current instances creds and use it to create client for remote instance
		endpoint := net.JoinHostPort(host, strconv.Itoa(port))
		accessKey := globalServerConfig.Credential.AccessKey
		secretKey := globalServerConfig.Credential.SecretKey
		return miniogo.NewCore(endpoint, accessKey, secretKey, globalIsSSL)
	}

	if isRemoteCallRequired(ctx, srcBucket, dstBucket, objectAPI) {
		if globalDNSConfig == nil {
			writeErrorResponse(w, ErrNoSuchBucket, r.URL)
			return
		}
		var dstRecords []dns.SrvRecord
		if dstRecords, err = globalDNSConfig.Get(dstBucket); err == nil {
			go func() {
				if gerr := objectAPI.GetObject(ctx, srcBucket, srcObject, 0, srcInfo.Size, srcInfo.Writer, srcInfo.ETag); gerr != nil {
					pipeWriter.CloseWithError(gerr)
					writeErrorResponse(w, ErrInternalError, r.URL)
					return
				}
				// Close writer explicitly to indicate data has been written
				srcInfo.Writer.Close()
			}()

			// Send PutObject request to appropriate instance (in federated deployment)
			host, port := getRandomHostPort(dstRecords)
			client, rerr := getRemoteInstanceClient(host, port)
			if rerr != nil {
				pipeWriter.CloseWithError(rerr)
				writeErrorResponse(w, ErrInternalError, r.URL)
				return
			}
			remoteObjInfo, rerr := client.PutObject(dstBucket, dstObject, srcInfo.Reader, srcInfo.Size, "", "", srcInfo.UserDefined)
			if rerr != nil {
				pipeWriter.CloseWithError(rerr)
				writeErrorResponse(w, ErrInternalError, r.URL)
				return
			}
			objInfo.ETag = remoteObjInfo.ETag
			objInfo.ModTime = remoteObjInfo.LastModified
		}
	} else {
		// Copy source object to destination, if source and destination
		// object is same then only metadata is updated.
		objInfo, err = objectAPI.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo)
		if err != nil {
			pipeWriter.CloseWithError(err)
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}

	pipeReader.Close()

	response := generateCopyObjectResponse(objInfo.ETag, objInfo.ModTime)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	if srcInfo.IsCompressed() {
		objInfo.Size = actualSize
	}

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:  event.ObjectCreatedCopy,
		BucketName: dstBucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       host,
		Port:       port,
	})
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api objectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObject")

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Validate storage class metadata if present
	if _, ok := r.Header[amzStorageClassCanonical]; ok {
		if !isValidStorageClassMeta(r.Header.Get(amzStorageClassCanonical)) {
			writeErrorResponse(w, ErrInvalidStorageClass, r.URL)
			return
		}
	}

	// Get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header)
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL)
		return
	}

	/// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	rAuthType := getRequestAuthType(r)
	if rAuthType == authTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				writeErrorResponse(w, ErrMissingContentLength, r.URL)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}
	if size == -1 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}

	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	metadata, err := extractMetadata(ctx, r)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	if rAuthType == authTypeStreamingSigned {
		if contentEncoding, ok := metadata["content-encoding"]; ok {
			contentEncoding = trimAwsChunkedContentEncoding(contentEncoding)
			if contentEncoding != "" {
				// Make sure to trim and save the content-encoding
				// parameter for a streaming signature which is set
				// to a custom value for example: "aws-chunked,gzip".
				metadata["content-encoding"] = contentEncoding
			} else {
				// Trimmed content encoding is empty when the header
				// value is set to "aws-chunked" only.

				// Make sure to delete the content-encoding parameter
				// for a streaming signature which is set to value
				// for example: "aws-chunked"
				delete(metadata, "content-encoding")
			}
		}
	}

	var (
		md5hex    = hex.EncodeToString(md5Bytes)
		sha256hex = ""
		reader    io.Reader
		s3Err     APIErrorCode
		putObject = objectAPI.PutObject
	)
	reader = r.Body
	switch rAuthType {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, ErrAccessDenied, r.URL)
		return
	case authTypeAnonymous:
		if !globalPolicySys.IsAllowed(policy.Args{
			Action:          policy.PutObjectAction,
			BucketName:      bucket,
			ConditionValues: getConditionValues(r, ""),
			IsOwner:         false,
			ObjectName:      object,
		}) {
			writeErrorResponse(w, ErrAccessDenied, r.URL)
			return
		}
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Err = newSignV4ChunkedReader(r)
		if s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Err = isReqAuthenticatedV2(r)
		if s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL)
			return
		}

	case authTypePresigned, authTypeSigned:
		if s3Err = reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL)
			return
		}
		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r)
		}
	}

	var hashError error
	actualSize := size

	if objectAPI.IsCompressionSupported() && isCompressible(r.Header, bucket, object) && size > 0 {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV1
		metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(size, 10)

		pipeReader, pipeWriter := io.Pipe()
		snappyWriter := snappy.NewWriter(pipeWriter)

		actualReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		go func() {
			defer pipeWriter.Close()
			defer snappyWriter.Close()
			// Writing to the compressed writer.
			_, err = io.CopyN(snappyWriter, actualReader, actualSize)
			if err != nil {
				hashError = err
				return
			}
		}()
		// Set compression metrics.
		size = -1   // Since compressed size is un-predictable.
		md5hex = "" // Do not try to verify the content.
		sha256hex = ""
		reader = pipeReader
	}

	hashReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	if objectAPI.IsEncryptionSupported() {
		if hasSSECustomerHeader(r.Header) && !hasSuffix(object, slashSeparator) { // handle SSE-C requests
			reader, err = EncryptRequest(hashReader, r, bucket, object, metadata)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			info := ObjectInfo{Size: size}
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", size) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}

	if api.CacheAPI() != nil && !hasSSECustomerHeader(r.Header) {
		putObject = api.CacheAPI().PutObject
	}

	// Create the object..
	objInfo, err := putObject(ctx, bucket, object, hashReader, metadata)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	if objInfo.IsCompressed() {
		// Ignore compressed ETag.
		objInfo.ETag = objInfo.ETag + "-1"
	}

	w.Header().Set("ETag", "\""+objInfo.ETag+"\"")
	if objectAPI.IsEncryptionSupported() {
		if hasSSECustomerHeader(r.Header) {
			w.Header().Set(SSECustomerAlgorithm, r.Header.Get(SSECustomerAlgorithm))
			w.Header().Set(SSECustomerKeyMD5, r.Header.Get(SSECustomerKeyMD5))
		}
	}

	if hashError != nil {
		if hashError == io.ErrUnexpectedEOF {
			writeErrorResponse(w, ErrIncompleteBody, r.URL)
		} else {
			writeErrorResponse(w, toAPIErrorCode(hashError), r.URL)
		}
		return
	}

	writeSuccessResponseHeadersOnly(w)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:  event.ObjectCreatedPut,
		BucketName: bucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       host,
		Port:       port,
	})
}

/// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload.
func (api objectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NewMultipartUpload")

	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	// Validate storage class metadata if present
	if _, ok := r.Header[amzStorageClassCanonical]; ok {
		if !isValidStorageClassMeta(r.Header.Get(amzStorageClassCanonical)) {
			writeErrorResponse(w, ErrInvalidStorageClass, r.URL)
			return
		}
	}

	var encMetadata = map[string]string{}

	if objectAPI.IsEncryptionSupported() {
		if hasSSECustomerHeader(r.Header) {
			key, err := ParseSSECustomerRequest(r)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			_, err = newEncryptMetadata(key, bucket, object, encMetadata)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			// Set this for multipart only operations, we need to differentiate during
			// decryption if the file was actually multipart or not.
			encMetadata[ReservedMetadataPrefix+"Encrypted-Multipart"] = ""
		}
	}

	// Extract metadata that needs to be saved.
	metadata, err := extractMetadata(ctx, r)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	for k, v := range encMetadata {
		metadata[k] = v
	}

	if objectAPI.IsCompressionSupported() && isCompressible(r.Header, bucket, object) {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV1
	}

	newMultipartUpload := objectAPI.NewMultipartUpload
	if api.CacheAPI() != nil {
		newMultipartUpload = api.CacheAPI().NewMultipartUpload
	}
	uploadID, err := newMultipartUpload(ctx, bucket, object, metadata)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	response := generateInitiateMultipartUploadResponse(bucket, object, uploadID)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// CopyObjectPartHandler - uploads a part by copying data from an existing object as data source.
func (api objectAPIHandlers) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CopyObjectPart")

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, dstBucket, dstObject); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := path2BucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, ErrInvalidPart, r.URL)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL)
		return
	}

	srcInfo, err := objectAPI.GetObjectInfo(ctx, srcBucket, srcObject)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Reading the actualSize from meta json.
	var actualPartSize int64
	for _, part := range srcInfo.Parts {
		if part.Number == partID {
			actualPartSize = part.ActualSize
			break
		}
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, dstBucket, dstObject); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	if objectAPI.IsEncryptionSupported() {
		if apiErr, _ := DecryptCopyObjectInfo(&srcInfo, r.Header); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL)
			return
		}
	}

	rangeHeader := r.Header.Get("x-amz-copy-source-range")

	var actualSize int64
	var compressedSize int64
	if srcInfo.IsCompressed() && rangeHeader != "" {
		// Read the decompressed size from the meta.json.
		actualSize = srcInfo.GetActualSize()
		if actualSize < 0 {
			writeErrorResponse(w, ErrInvalidDecompressedSize, r.URL)
			return
		}
		// Preserve the compressed size.
		compressedSize = srcInfo.Size
		// Set the info.Size to the actualSize.
		srcInfo.Size = actualSize
	}

	// Get request range.
	var hrange *httpRange
	if rangeHeader != "" {
		if hrange, err = parseCopyPartRange(rangeHeader, srcInfo.Size); err != nil {
			// Handle only errInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			logger.GetReqInfo(ctx).AppendTags("rangeHeader", rangeHeader)
			logger.LogIf(ctx, err)
			writeCopyPartErr(w, err, r.URL)
			return
		}
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if checkCopyObjectPartPreconditions(w, r, srcInfo) {
		return
	}

	// Get the object.
	var startOffset int64
	length := srcInfo.Size
	if hrange != nil {
		length = hrange.getLength()
		startOffset = hrange.offsetBegin
		actualPartSize = length
	}

	/// maximum copy size for multipart objects in a single operation
	if isMaxAllowedPartSize(length) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	var writer io.WriteCloser = pipeWriter
	var reader io.Reader
	var getLength int64
	// Need to decompress only for range-enabled copy parts.
	// Hijacks the pipeWriter, decompresses the data to fit the range.
	// And configures the srcInfo.Reader to be passed to the object layer.
	if srcInfo.IsCompressed() && hrange != nil {
		var sreader io.Reader
		var swriter io.Writer

		snappyStartOffset := startOffset
		snappyLength := length
		getLength = compressedSize
		startOffset = 0

		// Validate the range.
		// For negative length we read everything.
		if snappyLength < 0 {
			snappyLength = actualSize - snappyStartOffset
		}
		// Reply back invalid range if the input offset and length fall out of range.
		if snappyStartOffset > actualSize || snappyStartOffset+snappyLength > actualSize {
			writeErrorResponse(w, ErrInvalidRange, r.URL)
			return
		}

		// Open a pipe for compression.
		// Where snappyWriter is piped to srcInfo.Reader.
		// pipeReader writes to snappyWriter.
		snappyReader, snappyWriter := io.Pipe()
		reader = snappyReader
		length = -1

		swriter = snappy.NewWriter(snappyWriter)
		sreader = snappy.NewReader(pipeReader)

		go func() {
			defer writer.Close()
			defer snappyWriter.Close()
			if _, err = io.CopyN(goioutil.Discard, sreader, snappyStartOffset); err != nil {
				return
			}
			if _, err = io.CopyN(swriter, sreader, snappyLength); err != nil {
				return
			}
		}()
	} else {
		reader = pipeReader
		getLength = length
	}

	srcInfo.Reader, err = hash.NewReader(reader, length, "", "", actualPartSize)
	if err != nil {
		pipeWriter.CloseWithError(err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	if objectAPI.IsEncryptionSupported() && !srcInfo.IsCompressed() {
		var li ListPartsInfo
		li, err = objectAPI.ListObjectParts(ctx, dstBucket, dstObject, uploadID, 0, 1)
		if err != nil {
			pipeWriter.CloseWithError(err)
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		sseCopyC := hasSSECopyCustomerHeader(r.Header)
		if sseCopyC {
			// Response writer should be limited early on for decryption upto required length,
			// additionally also skipping mod(offset)64KiB boundaries.
			writer = ioutil.LimitedWriter(writer, startOffset%(64*1024), length)
			writer, startOffset, getLength, err = DecryptBlocksRequest(writer, r, srcBucket, srcObject, startOffset, length, srcInfo, true)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
		if li.IsEncrypted() {
			if !hasSSECustomerHeader(r.Header) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL)
				return
			}
			var key []byte
			key, err = ParseSSECustomerRequest(r)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			var objectEncryptionKey []byte
			objectEncryptionKey, err = decryptObjectInfo(key, dstBucket, dstObject, li.UserDefined)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			var partIDbin [4]byte
			binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

			mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
			mac.Write(partIDbin[:])
			partEncryptionKey := mac.Sum(nil)
			reader, err = sio.EncryptReader(reader, sio.Config{Key: partEncryptionKey})
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			info := ObjectInfo{Size: length}
			size := info.EncryptedSize()
			srcInfo.Reader, err = hash.NewReader(reader, size, "", "", actualPartSize)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}
	srcInfo.Writer = writer

	// Copy source object to destination, if source and destination
	// object is same then only metadata is updated.
	partInfo, err := objectAPI.CopyObjectPart(ctx, srcBucket, srcObject, dstBucket,
		dstObject, uploadID, partID, startOffset, getLength, srcInfo)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Close the pipe after successful operation.
	pipeReader.Close()

	response := generateCopyObjectPartResponse(partInfo.ETag, partInfo.LastModified)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// PutObjectPartHandler - uploads an incoming part for an ongoing multipart operation.
func (api objectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectPart")

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL)
		return
	}

	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header)
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL)
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength

	rAuthType := getRequestAuthType(r)
	// For auth type streaming signature, we need to gather a different content length.
	if rAuthType == authTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				writeErrorResponse(w, ErrMissingContentLength, r.URL)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}
	if size == -1 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxAllowedPartSize(size) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, ErrInvalidPart, r.URL)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL)
		return
	}

	var (
		md5hex    = hex.EncodeToString(md5Bytes)
		sha256hex = ""
		reader    io.Reader
	)
	reader = r.Body

	switch rAuthType {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, ErrAccessDenied, r.URL)
		return
	case authTypeAnonymous:
		if !globalPolicySys.IsAllowed(policy.Args{
			Action:          policy.PutObjectAction,
			BucketName:      bucket,
			ConditionValues: getConditionValues(r, ""),
			IsOwner:         false,
			ObjectName:      object,
		}) {
			writeErrorResponse(w, ErrAccessDenied, r.URL)
			return
		}
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		var s3Error APIErrorCode
		reader, s3Error = newSignV4ChunkedReader(r)
		if s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL)
			return
		}

		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r)
		}
	}

	actualSize := size
	var pipeReader *io.PipeReader
	var pipeWriter *io.PipeWriter
	// Disabling compression for encrypted enabled requests.
	// Using compression and encryption together enables room for side channel attacks.
	isCompressed := false
	if !hasSSECustomerHeader(r.Header) && objectAPI.IsCompressionSupported() {
		pipeReader, pipeWriter = io.Pipe()
		snappyWriter := snappy.NewWriter(pipeWriter)
		actualReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		go func() {
			defer pipeWriter.Close()
			defer snappyWriter.Close()
			// Writing to the compressed writer.
			_, err = io.CopyN(snappyWriter, actualReader, actualSize)
			if err != nil {
				// The ErrorResponse is already written in putObjectPart Handle.
				return
			}
		}()
		// Set compression metrics.
		size = -1   // Since compressed size is un-predictable.
		md5hex = "" // Do not try to verify the content.
		sha256hex = ""
		reader = pipeReader
		isCompressed = true
	}

	hashReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	if objectAPI.IsEncryptionSupported() && !isCompressed {
		var li ListPartsInfo
		li, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, 0, 1)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		if li.IsEncrypted() {
			if !hasSSECustomerHeader(r.Header) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL)
				return
			}
			var key []byte
			key, err = ParseSSECustomerRequest(r)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			// Calculating object encryption key
			var objectEncryptionKey []byte
			objectEncryptionKey, err = decryptObjectInfo(key, bucket, object, li.UserDefined)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			var partIDbin [4]byte
			binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

			mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
			mac.Write(partIDbin[:])
			partEncryptionKey := mac.Sum(nil)

			reader, err = sio.EncryptReader(reader, sio.Config{Key: partEncryptionKey})
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			info := ObjectInfo{Size: size}
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", size) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}

	putObjectPart := objectAPI.PutObjectPart
	if api.CacheAPI() != nil {
		putObjectPart = api.CacheAPI().PutObjectPart
	}
	partInfo, err := putObjectPart(ctx, bucket, object, uploadID, partID, hashReader)
	if err != nil {
		// Verify if the underlying error is signature mismatch.
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	if isCompressed {
		pipeWriter.Close()
		// Suppress compressed ETag.
		partInfo.ETag = partInfo.ETag + "-1"
	}
	if partInfo.ETag != "" {
		w.Header().Set("ETag", "\""+partInfo.ETag+"\"")
	}

	writeSuccessResponseHeadersOnly(w)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api objectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AbortMultipartUpload")

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	abortMultipartUpload := objectAPI.AbortMultipartUpload
	if api.CacheAPI() != nil {
		abortMultipartUpload = api.CacheAPI().AbortMultipartUpload
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.AbortMultipartUploadAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	if err := abortMultipartUpload(ctx, bucket, object, uploadID); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api objectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectParts")

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListMultipartUploadPartsAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	uploadID, partNumberMarker, maxParts, _ := getObjectResources(r.URL.Query())
	if partNumberMarker < 0 {
		writeErrorResponse(w, ErrInvalidPartNumberMarker, r.URL)
		return
	}
	if maxParts < 0 {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL)
		return
	}
	listPartsInfo, err := objectAPI.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	response := generateListPartsResponse(listPartsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload.
func (api objectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CompleteMultipartUpload")

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	completeMultipartBytes, err := goioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}
	complMultipartUpload := &CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}
	if !sort.IsSorted(CompletedParts(complMultipartUpload.Parts)) {
		writeErrorResponse(w, ErrInvalidPartOrder, r.URL)
		return
	}

	// Complete parts.
	var completeParts []CompletePart
	for _, part := range complMultipartUpload.Parts {
		// Avoiding for gateway parts.
		// `strings.TrimPrefix` does not work here as intended. So `Replace` is used instead.
		if objectAPI.IsCompressionSupported() {
			part.ETag = strings.Replace(part.ETag, "-1", "", -1) // For compressed multiparts, We append '-1' for part.ETag.
		}
		part.ETag = canonicalizeETag(part.ETag)
		completeParts = append(completeParts, part)
	}

	completeMultiPartUpload := objectAPI.CompleteMultipartUpload
	if api.CacheAPI() != nil {
		completeMultiPartUpload = api.CacheAPI().CompleteMultipartUpload
	}
	objInfo, err := completeMultiPartUpload(ctx, bucket, object, uploadID, completeParts)
	if err != nil {
		switch oErr := err.(type) {
		case PartTooSmall:
			// Write part too small error.
			writePartSmallErrorResponse(w, r, oErr)
		default:
			// Handle all other generic issues.
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		}
		return
	}

	// Get object location.
	location := getObjectLocation(r, globalDomainName, bucket, object)
	// Generate complete multipart response.
	response := generateCompleteMultpartUploadResponse(bucket, object, location, objInfo.ETag)
	encodedSuccessResponse := encodeResponse(response)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// Set etag.
	w.Header().Set("ETag", "\""+objInfo.ETag+"\"")

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:  event.ObjectCreatedCompleteMultipartUpload,
		BucketName: bucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       host,
		Port:       port,
	})
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api objectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteObject")

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		// Not required to check whether given object exists or not, because
		// DeleteObject is always successful irrespective of object existence.
		writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
		return
	}

	if globalDNSConfig != nil {
		_, err := globalDNSConfig.Get(bucket)
		if err != nil {
			if err == dns.ErrNoEntriesFound {
				writeErrorResponse(w, ErrNoSuchBucket, r.URL)
			} else {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			}
			return
		}
	} else {
		getBucketInfo := objectAPI.GetBucketInfo
		if api.CacheAPI() != nil {
			getBucketInfo = api.CacheAPI().GetBucketInfo
		}
		if _, err := getBucketInfo(ctx, bucket); err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}

	// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	// Ignore delete object errors while replying to client, since we are
	// suppposed to reply only 204. Additionally log the error for
	// investigation.
	deleteObject(ctx, objectAPI, api.CacheAPI(), bucket, object, r)
	writeSuccessNoContent(w)
}
