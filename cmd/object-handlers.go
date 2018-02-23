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

	mux "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/ioutil"
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

// setHeadGetRespHeaders - set any requested parameters as response headers.
func setHeadGetRespHeaders(w http.ResponseWriter, reqParams url.Values) {
	for k, v := range reqParams {
		if header, ok := supportedHeadGetReqParams[k]; ok {
			w.Header()[header] = v
		}
	}
}

// getSourceIPAddress - get the source ip address of the request.
func getSourceIPAddress(r *http.Request) string {
	var ip string
	// Attempt to get ip from standard headers.
	// Do not support X-Forwarded-For because it is easy to spoof.
	ip = r.Header.Get("X-Real-Ip")
	parsedIP := net.ParseIP(ip)
	// Skip non valid IP address.
	if parsedIP != nil {
		return ip
	}
	// Default to remote address if headers not set.
	ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	return ip
}

// errAllowableNotFound - For an anon user, return 404 if have ListBucket, 403 otherwise
// this is in keeping with the permissions sections of the docs of both:
//   HEAD Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//   GET Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func errAllowableObjectNotFound(bucket string, r *http.Request) APIErrorCode {
	if getRequestAuthType(r) == authTypeAnonymous {
		// We care about the bucket as a whole, not a particular resource.
		resource := "/" + bucket
		sourceIP := getSourceIPAddress(r)
		if s3Error := enforceBucketPolicy(bucket, "s3:ListBucket", resource,
			r.Referer(), sourceIP, r.URL.Query()); s3Error != ErrNone {
			return ErrAccessDenied
		}
	}
	return ErrNoSuchKey
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api objectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:GetObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	objInfo, err := objectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		apiErr := toAPIErrorCode(err)
		if apiErr == ErrNoSuchKey {
			apiErr = errAllowableObjectNotFound(bucket, r)
		}
		writeErrorResponse(w, apiErr, r.URL)
		return
	}

	if objectAPI.IsEncryptionSupported() {
		if apiErr, _ := DecryptObjectInfo(&objInfo, r.Header); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL)
			return
		}
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
			errorIf(err, "Invalid request range")
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

	var writer io.Writer
	writer = w
	if objectAPI.IsEncryptionSupported() {
		if IsSSECustomerRequest(r.Header) {
			// Response writer should be limited early on for decryption upto required length,
			// additionally also skipping mod(offset)64KiB boundaries.
			writer = ioutil.LimitedWriter(writer, startOffset%(64*1024), length)

			var sequenceNumber uint32
			sequenceNumber, startOffset, length = getStartOffset(startOffset, length)
			if length > objInfo.EncryptedSize() {
				length = objInfo.EncryptedSize()
			}

			writer, err = DecryptRequestWithSequenceNumber(writer, r, sequenceNumber, objInfo.UserDefined)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			w.Header().Set(SSECustomerAlgorithm, r.Header.Get(SSECustomerAlgorithm))
			w.Header().Set(SSECustomerKeyMD5, r.Header.Get(SSECustomerKeyMD5))
		}
	}

	setObjectHeaders(w, objInfo, hrange)
	setHeadGetRespHeaders(w, r.URL.Query())
	httpWriter := ioutil.WriteOnClose(writer)

	// Reads the object at startOffset and writes to httpWriter.
	if err = objectAPI.GetObject(bucket, object, startOffset, length, httpWriter, objInfo.ETag); err != nil {
		errorIf(err, "Unable to write to client.")
		if !httpWriter.HasWritten() { // write error response only if no data has been written to client yet
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		}
		httpWriter.Close()
		return
	}

	if err = httpWriter.Close(); err != nil {
		if !httpWriter.HasWritten() { // write error response only if no data has been written to client yet
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a GET request.
	eventNotify(eventData{
		Type:      ObjectAccessedGet,
		Bucket:    bucket,
		ObjInfo:   objInfo,
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api objectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:GetObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(w, s3Error)
		return
	}

	objInfo, err := objectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		apiErr := toAPIErrorCode(err)
		if apiErr == ErrNoSuchKey {
			apiErr = errAllowableObjectNotFound(bucket, r)
		}
		writeErrorResponseHeadersOnly(w, apiErr)
		return
	}

	if objectAPI.IsEncryptionSupported() {
		if apiErr, encrypted := DecryptObjectInfo(&objInfo, r.Header); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL)
			return
		} else if encrypted {
			if _, err = DecryptRequest(w, r, objInfo.UserDefined); err != nil {
				writeErrorResponse(w, ErrSSEEncryptedObject, r.URL)
				return
			}
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
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a HEAD request.
	eventNotify(eventData{
		Type:      ObjectAccessedHead,
		Bucket:    bucket,
		ObjInfo:   objInfo,
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})
}

// Extract metadata relevant for an CopyObject operation based on conditional
// header values specified in X-Amz-Metadata-Directive.
func getCpObjMetadataFromHeader(header http.Header, userMeta map[string]string) (map[string]string, error) {
	// Make a copy of the supplied metadata to avoid
	// to change the original one.
	defaultMeta := make(map[string]string, len(userMeta))
	for k, v := range userMeta {
		defaultMeta[k] = v
	}

	// if x-amz-metadata-directive says REPLACE then
	// we extract metadata from the input headers.
	if isMetadataReplace(header) {
		return extractMetadataFromHeader(header)
	}

	// if x-amz-metadata-directive says COPY then we
	// return the default metadata.
	if isMetadataCopy(header) {
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
	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, dstBucket, "s3:PutObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
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
	srcInfo, err := objectAPI.GetObjectInfo(srcBucket, srcObject)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
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
	var encMetadata = make(map[string]string)
	if objectAPI.IsEncryptionSupported() {
		var oldKey, newKey []byte
		sseCopyC := IsSSECopyCustomerRequest(r.Header)
		sseC := IsSSECustomerRequest(r.Header)
		if sseCopyC {
			oldKey, err = ParseSSECopyCustomerRequest(r)
			if err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
		if sseC {
			newKey, err = ParseSSECustomerRequest(r)
			if err != nil {
				pipeReader.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
		// AWS S3 implementation requires us to only rotate keys
		// when/ both keys are provided and destination is same
		// otherwise we proceed to encrypt/decrypt.
		if len(oldKey) > 0 && len(newKey) > 0 && cpSrcDstSame {
			for k, v := range srcInfo.UserDefined {
				encMetadata[k] = v
			}
			if err = rotateKey(oldKey, newKey, encMetadata); err != nil {
				pipeWriter.CloseWithError(err)
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		} else {
			if sseCopyC {
				// Source is encrypted make sure to save the encrypted size.
				if srcInfo.IsEncrypted() {
					srcInfo.Size = srcInfo.EncryptedSize()
				}
				writer, err = newDecryptWriter(pipeWriter, oldKey, 0, srcInfo.UserDefined)
				if err != nil {
					pipeWriter.CloseWithError(err)
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
			}
			if sseC {
				reader, err = newEncryptReader(pipeReader, newKey, encMetadata)
				if err != nil {
					pipeReader.CloseWithError(err)
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
			}
		}
	}
	srcInfo.Writer = writer

	srcInfo.UserDefined, err = getCpObjMetadataFromHeader(r.Header, srcInfo.UserDefined)
	if err != nil {
		pipeReader.CloseWithError(err)
		errorIf(err, "found invalid http request header")
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	for k, v := range encMetadata {
		srcInfo.UserDefined[k] = v
	}

	// Make sure to remove saved etag if any, CopyObject calculates a new one.
	delete(srcInfo.UserDefined, "etag")

	// Check if x-amz-metadata-directive was not set to REPLACE and source,
	// desination are same objects. Apply this restriction also when
	// metadataOnly is true indicating that we are not overwriting the object.
	if !isMetadataReplace(r.Header) && srcInfo.metadataOnly {
		pipeReader.CloseWithError(fmt.Errorf("invalid copy dest"))
		// If x-amz-metadata-directive is not set to REPLACE then we need
		// to error out if source and destination are same.
		writeErrorResponse(w, ErrInvalidCopyDest, r.URL)
		return
	}

	hashReader, err := hash.NewReader(reader, srcInfo.Size, "", "") // do not try to verify encrypted content
	if err != nil {
		pipeReader.CloseWithError(err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	srcInfo.Reader = hashReader

	// Copy source object to destination, if source and destination
	// object is same then only metadata is updated.
	objInfo, err := objectAPI.CopyObject(srcBucket, srcObject, dstBucket, dstObject, srcInfo)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	pipeReader.Close()

	response := generateCopyObjectResponse(objInfo.ETag, objInfo.ModTime)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:      ObjectCreatedCopy,
		Bucket:    dstBucket,
		ObjInfo:   objInfo,
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api objectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
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
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL)
		return
	}

	/// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	rAuthType := getRequestAuthType(r)
	if rAuthType == authTypeStreamingSigned {
		sizeStr := r.Header.Get("x-amz-decoded-content-length")
		size, err = strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
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

	// Extract metadata to be saved from incoming HTTP header.
	metadata, err := extractMetadataFromHeader(r.Header)
	if err != nil {
		errorIf(err, "found invalid http request header")
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
	)
	reader = r.Body
	switch rAuthType {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, ErrAccessDenied, r.URL)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		sourceIP := getSourceIPAddress(r)
		if s3Err = enforceBucketPolicy(bucket, "s3:PutObject", r.URL.Path, r.Referer(), sourceIP, r.URL.Query()); s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL)
			return
		}
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Err = newSignV4ChunkedReader(r)
		if s3Err != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
			writeErrorResponse(w, s3Err, r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Err = isReqAuthenticatedV2(r)
		if s3Err != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
			writeErrorResponse(w, s3Err, r.URL)
			return
		}

	case authTypePresigned, authTypeSigned:
		if s3Err = reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Err != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
			writeErrorResponse(w, s3Err, r.URL)
			return
		}
		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r)
		}
	}

	hashReader, err := hash.NewReader(reader, size, md5hex, sha256hex)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	if objectAPI.IsEncryptionSupported() {
		if IsSSECustomerRequest(r.Header) && !hasSuffix(object, slashSeparator) { // handle SSE-C requests
			reader, err = EncryptRequest(hashReader, r, metadata)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			info := ObjectInfo{Size: size}
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "") // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}

	objInfo, err := objectAPI.PutObject(bucket, object, hashReader, metadata)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	w.Header().Set("ETag", "\""+objInfo.ETag+"\"")
	if objectAPI.IsEncryptionSupported() {
		if IsSSECustomerRequest(r.Header) {
			w.Header().Set(SSECustomerAlgorithm, r.Header.Get(SSECustomerAlgorithm))
			w.Header().Set(SSECustomerKeyMD5, r.Header.Get(SSECustomerKeyMD5))
		}
	}

	writeSuccessResponseHeadersOnly(w)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:      ObjectCreatedPut,
		Bucket:    bucket,
		ObjInfo:   objInfo,
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})
}

/// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload.
func (api objectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:PutObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Validate storage class metadata if present
	if _, ok := r.Header[amzStorageClassCanonical]; ok {
		if !isValidStorageClassMeta(r.Header.Get(amzStorageClassCanonical)) {
			writeErrorResponse(w, ErrInvalidStorageClass, r.URL)
			return
		}
	}

	if IsSSECustomerRequest(r.Header) { // handle SSE-C requests
		// SSE-C is not implemented for multipart operations yet
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	// Extract metadata that needs to be saved.
	metadata, err := extractMetadataFromHeader(r.Header)
	if err != nil {
		errorIf(err, "found invalid http request header")
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	uploadID, err := objectAPI.NewMultipartUpload(bucket, object, metadata)
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
	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, dstBucket, "s3:PutObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	if IsSSECustomerRequest(r.Header) { // handle SSE-C requests
		// SSE-C is not implemented for multipart operations yet
		writeErrorResponse(w, ErrNotImplemented, r.URL)
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

	srcInfo, err := objectAPI.GetObjectInfo(srcBucket, srcObject)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Get request range.
	var hrange *httpRange
	rangeHeader := r.Header.Get("x-amz-copy-source-range")
	if rangeHeader != "" {
		if hrange, err = parseCopyPartRange(rangeHeader, srcInfo.Size); err != nil {
			// Handle only errInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			errorIf(err, "Unable to extract range %s", rangeHeader)
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
	}

	/// maximum copy size for multipart objects in a single operation
	if isMaxAllowedPartSize(length) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	// Make sure to remove all metadata from source for for multipart operations.
	srcInfo.UserDefined = nil

	// Copy source object to destination, if source and destination
	// object is same then only metadata is updated.
	partInfo, err := objectAPI.CopyObjectPart(srcBucket, srcObject, dstBucket,
		dstObject, uploadID, partID, startOffset, length, srcInfo)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	response := generateCopyObjectPartResponse(partInfo.ETag, partInfo.LastModified)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// PutObjectPartHandler - uploads an incoming part for an ongoing multipart operation.
func (api objectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
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
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL)
		return
	}

	if IsSSECustomerRequest(r.Header) { // handle SSE-C requests
		// SSE-C is not implemented for multipart operations yet
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength

	rAuthType := getRequestAuthType(r)
	// For auth type streaming signature, we need to gather a different content length.
	if rAuthType == authTypeStreamingSigned {
		sizeStr := r.Header.Get("x-amz-decoded-content-length")
		size, err = strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
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
		reader    = r.Body
	)

	switch rAuthType {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, ErrAccessDenied, r.URL)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy(bucket, "s3:PutObject", r.URL.Path,
			r.Referer(), getSourceIPAddress(r), r.URL.Query()); s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		var s3Error APIErrorCode
		reader, s3Error = newSignV4ChunkedReader(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Error != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}

		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r)
		}
	}

	hashReader, err := hash.NewReader(reader, size, md5hex, sha256hex)
	if err != nil {
		// Verify if the underlying error is signature mismatch.
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	partInfo, err := objectAPI.PutObjectPart(bucket, object, uploadID, partID, hashReader)
	if err != nil {
		// Verify if the underlying error is signature mismatch.
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	if partInfo.ETag != "" {
		w.Header().Set("ETag", "\""+partInfo.ETag+"\"")
	}

	writeSuccessResponseHeadersOnly(w)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api objectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:AbortMultipartUpload", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	if err := objectAPI.AbortMultipartUpload(bucket, object, uploadID); err != nil {
		errorIf(err, "AbortMultipartUpload failed")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api objectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListMultipartUploadParts", globalServerConfig.GetRegion()); s3Error != ErrNone {
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
	listPartsInfo, err := objectAPI.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
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
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:PutObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
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
		part.ETag = canonicalizeETag(part.ETag)
		completeParts = append(completeParts, part)
	}

	objInfo, err := objectAPI.CompleteMultipartUpload(bucket, object, uploadID, completeParts)
	if err != nil {
		err = errors.Cause(err)
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
	location := getLocation(r)
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
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:      ObjectCreatedCompleteMultipartUpload,
		Bucket:    bucket,
		ObjInfo:   objInfo,
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api objectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:DeleteObject", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	// Ignore delete object errors while replying to client, since we are
	// suppposed to reply only 204. Additionally log the error for
	// investigation.
	if err := deleteObject(objectAPI, bucket, object, r); err != nil {
		errorIf(err, "Unable to delete an object %s", pathJoin(bucket, object))
	}
	writeSuccessNoContent(w)
}
