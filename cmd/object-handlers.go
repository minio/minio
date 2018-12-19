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
	"io"
	goioutil "io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	snappy "github.com/golang/snappy"
	"github.com/gorilla/mux"
	"github.com/klauspost/readahead"
	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/s3select"
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

// SelectObjectContentHandler - GET Object?select
// ----------
// This implementation of the GET operation retrieves object content based
// on an SQL expression. In the request, along with the sql expression, you must
// also specify a data serialization format (JSON, CSV) of the object.
func (api objectAPIHandlers) SelectObjectContentHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SelectObject")

	defer logger.AuditLog(w, r, "SelectObject", mustGetClaimsFromToken(r))

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) { // SSE-KMS is not supported
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrBadRequest, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in
			// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
			// If the object you request does not exist,
			// the error Amazon S3 returns depends on
			// whether you also have the s3:ListBucket
			// permission.
			// * If you have the s3:ListBucket permission
			//   on the bucket, Amazon S3 will return an
			//   HTTP status code 404 ("no such key")
			//   error.
			// * if you don’t have the s3:ListBucket
			//   permission, Amazon S3 will return an HTTP
			//   status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.Args{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, ""),
				IsOwner:         false,
			}) {
				getObjectInfo := objectAPI.GetObjectInfo
				if api.CacheAPI() != nil {
					getObjectInfo = api.CacheAPI().GetObjectInfo
				}

				_, err := getObjectInfo(ctx, bucket, object, ObjectOptions{})
				if toAPIErrorCode(ctx, err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Get request range.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		writeErrorResponse(w, ErrUnsupportedRangeHeader, r.URL, guessIsBrowserReq(r))
		return
	}

	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrEmptyRequestBody, r.URL, guessIsBrowserReq(r))
		return
	}
	var selectReq s3select.ObjectSelectRequest
	if err := xmlDecoder(r.Body, &selectReq, r.ContentLength); err != nil {
		writeErrorResponse(w, ErrMalformedXML, r.URL, guessIsBrowserReq(r))
		return
	}

	if !strings.EqualFold(string(selectReq.ExpressionType), "SQL") {
		writeErrorResponse(w, ErrInvalidExpressionType, r.URL, guessIsBrowserReq(r))
		return
	}
	if len(selectReq.Expression) >= s3select.MaxExpressionLength {
		writeErrorResponse(w, ErrExpressionTooLong, r.URL, guessIsBrowserReq(r))
		return
	}

	getObjectNInfo := objectAPI.GetObjectNInfo
	if api.CacheAPI() != nil {
		getObjectNInfo = api.CacheAPI().GetObjectNInfo
	}

	var opts ObjectOptions
	gr, err := getObjectNInfo(ctx, bucket, object, nil, r.Header, readLock, opts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	defer gr.Close()

	objInfo := gr.ObjInfo

	if selectReq.InputSerialization.CompressionType == s3select.SelectCompressionGZIP {
		if !strings.Contains(objInfo.ContentType, "gzip") {
			writeErrorResponse(w, ErrInvalidDataSource, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	if selectReq.InputSerialization.CompressionType == s3select.SelectCompressionBZIP {
		if !strings.Contains(objInfo.ContentType, "bzip") {
			writeErrorResponse(w, ErrInvalidDataSource, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	if selectReq.InputSerialization.CompressionType == "" {
		selectReq.InputSerialization.CompressionType = s3select.SelectCompressionNONE
		if !strings.Contains(objInfo.ContentType, "text/csv") && !strings.Contains(objInfo.ContentType, "application/json") {
			writeErrorResponse(w, ErrInvalidDataSource, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	if !strings.EqualFold(string(selectReq.ExpressionType), "SQL") {
		writeErrorResponse(w, ErrInvalidExpressionType, r.URL, guessIsBrowserReq(r))
		return
	}
	if len(selectReq.Expression) >= s3select.MaxExpressionLength {
		writeErrorResponse(w, ErrExpressionTooLong, r.URL, guessIsBrowserReq(r))
		return
	}
	if selectReq.InputSerialization.CSV == nil && selectReq.InputSerialization.JSON == nil {
		writeErrorResponse(w, ErrInvalidRequestParameter, r.URL, guessIsBrowserReq(r))
		return
	}
	if selectReq.OutputSerialization.CSV == nil && selectReq.OutputSerialization.JSON == nil {
		writeErrorResponse(w, ErrInvalidRequestParameter, r.URL, guessIsBrowserReq(r))
		return
	}

	if selectReq.InputSerialization.CSV != nil {
		if selectReq.InputSerialization.CSV.FileHeaderInfo != s3select.CSVFileHeaderInfoUse &&
			selectReq.InputSerialization.CSV.FileHeaderInfo != s3select.CSVFileHeaderInfoNone &&
			selectReq.InputSerialization.CSV.FileHeaderInfo != s3select.CSVFileHeaderInfoIgnore &&
			selectReq.InputSerialization.CSV.FileHeaderInfo != "" {
			writeErrorResponse(w, ErrInvalidFileHeaderInfo, r.URL, guessIsBrowserReq(r))
			return
		}
		if selectReq.OutputSerialization.CSV != nil {
			if selectReq.OutputSerialization.CSV.QuoteFields != s3select.CSVQuoteFieldsAlways &&
				selectReq.OutputSerialization.CSV.QuoteFields != s3select.CSVQuoteFieldsAsNeeded &&
				selectReq.OutputSerialization.CSV.QuoteFields != "" {
				writeErrorResponse(w, ErrInvalidQuoteFields, r.URL, guessIsBrowserReq(r))
				return
			}
		}
		if len(selectReq.InputSerialization.CSV.RecordDelimiter) > 2 {
			writeErrorResponse(w, ErrInvalidRequestParameter, r.URL, guessIsBrowserReq(r))
			return
		}

	}
	if selectReq.InputSerialization.JSON != nil {
		if selectReq.InputSerialization.JSON.Type != s3select.JSONLinesType {
			writeErrorResponse(w, ErrInvalidJSONType, r.URL, guessIsBrowserReq(r))
			return
		}

	}

	// Set encryption response headers
	if objectAPI.IsEncryptionSupported() {
		if crypto.IsEncrypted(objInfo.UserDefined) {
			switch {
			case crypto.S3.IsEncrypted(objInfo.UserDefined):
				w.Header().Set(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
			case crypto.SSEC.IsEncrypted(objInfo.UserDefined):
				w.Header().Set(crypto.SSECAlgorithm, r.Header.Get(crypto.SSECAlgorithm))
				w.Header().Set(crypto.SSECKeyMD5, r.Header.Get(crypto.SSECKeyMD5))
			}
		}
	}

	reader := readahead.NewReader(gr)
	defer reader.Close()

	size := objInfo.Size
	if objInfo.IsCompressed() {
		size = objInfo.GetActualSize()
		if size < 0 {
			writeErrorResponse(w, toAPIErrorCode(ctx, errInvalidDecompressedSize), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	s3s, err := s3select.New(reader, size, selectReq)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Parses the select query and checks for an error
	_, _, _, _, _, _, err = s3select.ParseSelect(s3s)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Executes the query on data-set
	s3select.Execute(w, s3s)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a GET request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedGet,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
	})
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api objectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObject")

	defer logger.AuditLog(w, r, "GetObject", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponse(w, ErrBadRequest, r.URL, guessIsBrowserReq(r))
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrBadRequest, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if vid := r.URL.Query().Get("versionId"); vid != "" && vid != "null" {
		writeErrorResponse(w, ErrNoSuchVersion, r.URL, guessIsBrowserReq(r))
		return
	}

	var (
		opts ObjectOptions
		err  error
	)
	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in
			// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
			// If the object you request does not exist,
			// the error Amazon S3 returns depends on
			// whether you also have the s3:ListBucket
			// permission.
			// * If you have the s3:ListBucket permission
			//   on the bucket, Amazon S3 will return an
			//   HTTP status code 404 ("no such key")
			//   error.
			// * if you don’t have the s3:ListBucket
			//   permission, Amazon S3 will return an HTTP
			//   status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.Args{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, ""),
				IsOwner:         false,
			}) {
				getObjectInfo := objectAPI.GetObjectInfo
				if api.CacheAPI() != nil {
					getObjectInfo = api.CacheAPI().GetObjectInfo
				}

				_, err = getObjectInfo(ctx, bucket, object, opts)
				if toAPIErrorCode(ctx, err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	getObjectNInfo := objectAPI.GetObjectNInfo
	if api.CacheAPI() != nil {
		getObjectNInfo = api.CacheAPI().GetObjectNInfo
	}

	// Get request range.
	var rs *HTTPRangeSpec
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if rs, err = parseRequestRangeSpec(rangeHeader); err != nil {
			// Handle only errInvalidRange. Ignore other
			// parse error and treat it as regular Get
			// request like Amazon S3.
			if err == errInvalidRange {
				writeErrorResponse(w, ErrInvalidRange, r.URL, guessIsBrowserReq(r))
				return
			}

			logger.LogIf(ctx, err)
		}
	}

	gr, err := getObjectNInfo(ctx, bucket, object, rs, r.Header, readLock, opts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	defer gr.Close()

	objInfo := gr.ObjInfo

	if objectAPI.IsEncryptionSupported() {
		if _, err = DecryptObjectInfo(&objInfo, r.Header); err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Set encryption response headers
	if objectAPI.IsEncryptionSupported() {
		if crypto.IsEncrypted(objInfo.UserDefined) {
			switch {
			case crypto.S3.IsEncrypted(objInfo.UserDefined):
				w.Header().Set(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
			case crypto.SSEC.IsEncrypted(objInfo.UserDefined):
				w.Header().Set(crypto.SSECAlgorithm, r.Header.Get(crypto.SSECAlgorithm))
				w.Header().Set(crypto.SSECKeyMD5, r.Header.Get(crypto.SSECKeyMD5))
			}
		}
	}

	if err = setObjectHeaders(w, objInfo, rs); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	setHeadGetRespHeaders(w, r.URL.Query())

	statusCodeWritten := false
	httpWriter := ioutil.WriteOnClose(w)
	if rs != nil {
		statusCodeWritten = true
		w.WriteHeader(http.StatusPartialContent)
	}

	// Write object content to response body
	if _, err = io.Copy(httpWriter, gr); err != nil {
		if !httpWriter.HasWritten() && !statusCodeWritten { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		}
		return
	}

	if err = httpWriter.Close(); err != nil {
		if !httpWriter.HasWritten() && !statusCodeWritten { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a GET request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedGet,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
	})
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api objectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadObject")

	defer logger.AuditLog(w, r, "HeadObject", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponseHeadersOnly(w, ErrBadRequest)
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrBadRequest, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if vid := r.URL.Query().Get("versionId"); vid != "" && vid != "null" {
		writeErrorResponseHeadersOnly(w, ErrNoSuchVersion)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfo = api.CacheAPI().GetObjectInfo
	}

	var (
		opts ObjectOptions
		err  error
	)

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in
			// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
			// If the object you request does not exist,
			// the error Amazon S3 returns depends on
			// whether you also have the s3:ListBucket
			// permission.
			// * If you have the s3:ListBucket permission
			//   on the bucket, Amazon S3 will return an
			//   HTTP status code 404 ("no such key")
			//   error.
			// * if you don’t have the s3:ListBucket
			//   permission, Amazon S3 will return an HTTP
			//   status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.Args{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, ""),
				IsOwner:         false,
			}) {
				_, err = getObjectInfo(ctx, bucket, object, opts)
				if toAPIErrorCode(ctx, err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponseHeadersOnly(w, s3Error)
		return
	}

	// Get request range.
	var rs *HTTPRangeSpec
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if rs, err = parseRequestRangeSpec(rangeHeader); err != nil {
			// Handle only errInvalidRange. Ignore other
			// parse error and treat it as regular Get
			// request like Amazon S3.
			if err == errInvalidRange {
				writeErrorResponseHeadersOnly(w, ErrInvalidRange)
				return
			}

			logger.LogIf(ctx, err)
		}
	}

	objInfo, err := getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(ctx, err))
		return
	}

	if objectAPI.IsEncryptionSupported() {
		if _, err = DecryptObjectInfo(&objInfo, r.Header); err != nil {
			writeErrorResponseHeadersOnly(w, toAPIErrorCode(ctx, err))
			return
		}
	}

	// Set encryption response headers
	if objectAPI.IsEncryptionSupported() {
		if crypto.IsEncrypted(objInfo.UserDefined) {
			switch {
			case crypto.S3.IsEncrypted(objInfo.UserDefined):
				w.Header().Set(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
			case crypto.SSEC.IsEncrypted(objInfo.UserDefined):
				// Validate the SSE-C Key set in the header.
				if _, err = crypto.SSEC.UnsealObjectKey(r.Header, objInfo.UserDefined, bucket, object); err != nil {
					writeErrorResponseHeadersOnly(w, toAPIErrorCode(ctx, err))
					return
				}
				w.Header().Set(crypto.SSECAlgorithm, r.Header.Get(crypto.SSECAlgorithm))
				w.Header().Set(crypto.SSECKeyMD5, r.Header.Get(crypto.SSECKeyMD5))
			}
		}
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Set standard object headers.
	if err = setObjectHeaders(w, objInfo, rs); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(ctx, err))
		return
	}

	// Set any additional requested response headers.
	setHeadGetRespHeaders(w, r.URL.Query())

	// Successful response.
	if rs != nil {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object accessed via a HEAD request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedHead,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
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

// Returns a minio-go Client configured to access remote host described by destDNSRecord
// Applicable only in a federated deployment
var getRemoteInstanceClient = func(r *http.Request, host string) (*miniogo.Core, error) {
	cred := getReqAccessCred(r, globalServerConfig.GetRegion())
	// In a federated deployment, all the instances share config files
	// and hence expected to have same credentials.
	core, err := miniogo.NewCore(host, cred.AccessKey, cred.SecretKey, globalIsSSL)
	if err != nil {
		return nil, err
	}
	core.SetCustomTransport(NewCustomHTTPTransport())
	return core, nil
}

// Check if the bucket is on a remote site, this code only gets executed when federation is enabled.
var isRemoteCallRequired = func(ctx context.Context, bucket string, objAPI ObjectLayer) bool {
	if globalDNSConfig == nil {
		return false
	}
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	return err == toObjectErr(errVolumeNotFound, bucket)
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CopyObject")

	defer logger.AuditLog(w, r, "CopyObject", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r)) // SSE-KMS is not supported
		return
	}
	if !objectAPI.IsEncryptionSupported() && (hasServerSideEncryptionHeader(r.Header) || crypto.SSECopy.IsRequested(r.Header)) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, dstBucket, dstObject); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// TODO: Reject requests where body/payload is present, for now we don't even read it.

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	// Check https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectVersioning.html
	// Regardless of whether you have enabled versioning, each object in your bucket
	// has a version ID. If you have not enabled versioning, Amazon S3 sets the value
	// of the version ID to null. If you have enabled versioning, Amazon S3 assigns a
	// unique version ID value for the object.
	if u, err := url.Parse(cpSrcPath); err == nil {
		// Check if versionId query param was added, if yes then check if
		// its non "null" value, we should error out since we do not support
		// any versions other than "null".
		if vid := u.Query().Get("versionId"); vid != "" && vid != "null" {
			writeErrorResponse(w, ErrNoSuchVersion, r.URL, guessIsBrowserReq(r))
			return
		}
		cpSrcPath = u.Path
	}

	srcBucket, srcObject := path2BucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, srcBucket, srcObject); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if metadata directive is valid.
	if !isMetadataDirectiveValid(r.Header) {
		writeErrorResponse(w, ErrInvalidMetadataDirective, r.URL, guessIsBrowserReq(r))
		return
	}

	var srcOpts, dstOpts ObjectOptions

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, dstBucket, dstObject, dstOpts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	getObjectNInfo := objectAPI.GetObjectNInfo
	if api.CacheAPI() != nil {
		getObjectNInfo = api.CacheAPI().GetObjectNInfo
	}

	var lock = noLock
	if !cpSrcDstSame {
		lock = readLock
	}

	var rs *HTTPRangeSpec
	gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, lock, srcOpts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	defer gr.Close()
	srcInfo := gr.ObjInfo

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if checkCopyObjectPreconditions(w, r, srcInfo) {
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(srcInfo.Size) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL, guessIsBrowserReq(r))
		return
	}

	// We have to copy metadata only if source and destination are same.
	// this changes for encryption which can be observed below.
	if cpSrcDstSame {
		srcInfo.metadataOnly = true
	}

	var reader io.Reader
	var length = srcInfo.Size

	// Set the actual size to the decrypted size if encrypted.
	actualSize := srcInfo.Size
	if crypto.IsEncrypted(srcInfo.UserDefined) {
		actualSize, err = srcInfo.DecryptedSize()
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
		length = actualSize
	}

	// Check if the destination bucket is on a remote site, this code only gets executed
	// when federation is enabled, ie when globalDNSConfig is non 'nil'.
	//
	// This function is similar to isRemoteCallRequired but specifically for COPY object API
	// if destination and source are same we do not need to check for destnation bucket
	// to exist locally.
	var isRemoteCopyRequired = func(ctx context.Context, srcBucket, dstBucket string, objAPI ObjectLayer) bool {
		if globalDNSConfig == nil {
			return false
		}
		if srcBucket == dstBucket {
			return false
		}
		_, err := objAPI.GetBucketInfo(ctx, dstBucket)
		return err == toObjectErr(errVolumeNotFound, dstBucket)
	}

	var compressMetadata map[string]string
	// No need to compress for remote etcd calls
	// Pass the decompressed stream to such calls.
	isCompressed := objectAPI.IsCompressionSupported() && isCompressible(r.Header, srcObject) && !isRemoteCopyRequired(ctx, srcBucket, dstBucket, objectAPI)
	if isCompressed {
		compressMetadata = make(map[string]string, 2)
		// Preserving the compression metadata.
		compressMetadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV1
		compressMetadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(actualSize, 10)
		// Remove all source encrypted related metadata to
		// avoid copying them in target object.
		crypto.RemoveInternalEntries(srcInfo.UserDefined)
		// Open a pipe for compression.
		// Where pipeWriter is piped to srcInfo.Reader.
		// gr writes to pipeWriter.
		pipeReader, pipeWriter := io.Pipe()
		reader = pipeReader
		length = -1

		snappyWriter := snappy.NewWriter(pipeWriter)

		go func() {
			// Compress the decompressed source object.
			_, cerr := io.Copy(snappyWriter, gr)
			snappyWriter.Close()
			pipeWriter.CloseWithError(cerr)
		}()
	} else {
		// Remove the metadata for remote calls.
		delete(srcInfo.UserDefined, ReservedMetadataPrefix+"compression")
		delete(srcInfo.UserDefined, ReservedMetadataPrefix+"actual-size")
		reader = gr
	}

	srcInfo.Reader, err = hash.NewReader(reader, length, "", "", actualSize)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rawReader := srcInfo.Reader
	pReader := NewPutObjReader(srcInfo.Reader, nil, nil)

	if globalAutoEncryption && !crypto.SSEC.IsRequested(r.Header) {
		r.Header.Add(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
	}
	var encMetadata = make(map[string]string)
	if objectAPI.IsEncryptionSupported() && !isCompressed {
		// Encryption parameters not applicable for this object.
		if !crypto.IsEncrypted(srcInfo.UserDefined) && crypto.SSECopy.IsRequested(r.Header) {
			writeErrorResponse(w, toAPIErrorCode(ctx, errInvalidEncryptionParameters), r.URL, guessIsBrowserReq(r))
			return
		}
		// Encryption parameters not present for this object.
		if crypto.SSEC.IsEncrypted(srcInfo.UserDefined) && !crypto.SSECopy.IsRequested(r.Header) {
			writeErrorResponse(w, ErrInvalidSSECustomerAlgorithm, r.URL, guessIsBrowserReq(r))
			return
		}

		var oldKey, newKey, objEncKey []byte
		sseCopyS3 := crypto.S3.IsEncrypted(srcInfo.UserDefined)
		sseCopyC := crypto.SSEC.IsEncrypted(srcInfo.UserDefined) && crypto.SSECopy.IsRequested(r.Header)
		sseC := crypto.SSEC.IsRequested(r.Header)
		sseS3 := crypto.S3.IsRequested(r.Header)

		isSourceEncrypted := sseCopyC || sseCopyS3
		isTargetEncrypted := sseC || sseS3

		if sseC {
			newKey, err = ParseSSECustomerRequest(r)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
		}

		// If src == dst and either
		// - the object is encrypted using SSE-C and two different SSE-C keys are present
		// - the object is encrypted using SSE-S3 and the SSE-S3 header is present
		// than execute a key rotation.
		if cpSrcDstSame && ((sseCopyC && sseC) || (sseS3 && sseCopyS3)) {
			if sseCopyC && sseC {
				oldKey, err = ParseSSECopyCustomerRequest(r.Header, srcInfo.UserDefined)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}

			for k, v := range srcInfo.UserDefined {
				if hasPrefix(k, ReservedMetadataPrefix) {
					encMetadata[k] = v
				}
			}

			// In case of SSE-S3 oldKey and newKey aren't used - the KMS manages the keys.
			if err = rotateKey(oldKey, newKey, srcBucket, srcObject, encMetadata); err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}

			// Since we are rotating the keys, make sure to update the metadata.
			srcInfo.metadataOnly = true
		} else {
			if isSourceEncrypted || isTargetEncrypted {
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
			}

			// Calculate the size of the target object
			var targetSize int64

			switch {
			case !isSourceEncrypted && !isTargetEncrypted:
				targetSize = srcInfo.Size
			case isSourceEncrypted && isTargetEncrypted:
				targetSize = srcInfo.Size
			case !isSourceEncrypted && isTargetEncrypted:
				targetSize = srcInfo.EncryptedSize()
			case isSourceEncrypted && !isTargetEncrypted:
				targetSize, _ = srcInfo.DecryptedSize()
			}

			if isTargetEncrypted {
				reader, objEncKey, err = newEncryptReader(srcInfo.Reader, newKey, dstBucket, dstObject, encMetadata, sseS3)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}

			if isSourceEncrypted {
				// Remove all source encrypted related metadata to
				// avoid copying them in target object.
				crypto.RemoveInternalEntries(srcInfo.UserDefined)
			}

			srcInfo.Reader, err = hash.NewReader(reader, targetSize, "", "", targetSize) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}

			pReader = NewPutObjReader(rawReader, srcInfo.Reader, objEncKey)
		}
	}

	srcInfo.PutObjReader = pReader

	srcInfo.UserDefined, err = getCpObjMetadataFromHeader(ctx, r, srcInfo.UserDefined)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
		return
	}

	// Store the preserved compression metadata.
	for k, v := range compressMetadata {
		srcInfo.UserDefined[k] = v
	}

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	for k, v := range encMetadata {
		srcInfo.UserDefined[k] = v
	}

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(srcInfo.UserDefined)

	// Check if x-amz-metadata-directive was not set to REPLACE and source,
	// desination are same objects. Apply this restriction also when
	// metadataOnly is true indicating that we are not overwriting the object.
	// if encryption is enabled we do not need explicit "REPLACE" metadata to
	// be enabled as well - this is to allow for key-rotation.
	if !isMetadataReplace(r.Header) && srcInfo.metadataOnly && !crypto.IsEncrypted(srcInfo.UserDefined) {
		// If x-amz-metadata-directive is not set to REPLACE then we need
		// to error out if source and destination are same.
		writeErrorResponse(w, ErrInvalidCopyDest, r.URL, guessIsBrowserReq(r))
		return
	}

	var objInfo ObjectInfo

	if isRemoteCopyRequired(ctx, srcBucket, dstBucket, objectAPI) {
		var dstRecords []dns.SrvRecord
		dstRecords, err = globalDNSConfig.Get(dstBucket)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		// Send PutObject request to appropriate instance (in federated deployment)
		client, rerr := getRemoteInstanceClient(r, getHostFromSrv(dstRecords))
		if rerr != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, rerr), r.URL, guessIsBrowserReq(r))
			return
		}
		remoteObjInfo, rerr := client.PutObject(dstBucket, dstObject, srcInfo.Reader,
			srcInfo.Size, "", "", srcInfo.UserDefined, dstOpts.ServerSideEncryption)
		if rerr != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, rerr), r.URL, guessIsBrowserReq(r))
			return
		}
		objInfo.ETag = remoteObjInfo.ETag
		objInfo.ModTime = remoteObjInfo.LastModified
	} else {
		// Copy source object to destination, if source and destination
		// object is same then only metadata is updated.
		objInfo, err = objectAPI.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	response := generateCopyObjectResponse(getDecryptedETag(r.Header, objInfo, false), objInfo.ModTime)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	if objInfo.IsCompressed() {
		objInfo.Size = actualSize
	}

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedCopy,
		BucketName:   dstBucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
	})
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObject")

	defer logger.AuditLog(w, r, "PutObject", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r)) // SSE-KMS is not supported
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate storage class metadata if present
	if _, ok := r.Header[amzStorageClassCanonical]; ok {
		if !isValidStorageClassMeta(r.Header.Get(amzStorageClassCanonical)) {
			writeErrorResponse(w, ErrInvalidStorageClass, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header)
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL, guessIsBrowserReq(r))
		return
	}
	/// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	rAuthType := getRequestAuthType(r)
	if rAuthType == authTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}
	if size == -1 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
		return
	}

	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL, guessIsBrowserReq(r))
		return
	}

	metadata, err := extractMetadata(ctx, r)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
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

	// Check if put is allowed
	if s3Err = isPutAllowed(rAuthType, bucket, object, r); s3Err != ErrNone {
		writeErrorResponse(w, s3Err, r.URL, guessIsBrowserReq(r))
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Err = newSignV4ChunkedReader(r)
		if s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL, guessIsBrowserReq(r))
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Err = isReqAuthenticatedV2(r)
		if s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL, guessIsBrowserReq(r))
			return
		}

	case authTypePresigned, authTypeSigned:
		if s3Err = reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Err != ErrNone {
			writeErrorResponse(w, s3Err, r.URL, guessIsBrowserReq(r))
			return
		}
		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r)
		}
	}

	actualSize := size

	if objectAPI.IsCompressionSupported() && isCompressible(r.Header, object) && size > 0 {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV1
		metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(size, 10)

		pipeReader, pipeWriter := io.Pipe()
		snappyWriter := snappy.NewWriter(pipeWriter)

		var actualReader *hash.Reader
		actualReader, err = hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		go func() {
			// Writing to the compressed writer.
			_, cerr := io.CopyN(snappyWriter, actualReader, actualSize)
			snappyWriter.Close()
			pipeWriter.CloseWithError(cerr)
		}()

		// Set compression metrics.
		size = -1   // Since compressed size is un-predictable.
		md5hex = "" // Do not try to verify the content.
		sha256hex = ""
		reader = pipeReader
	}

	hashReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rawReader := hashReader
	pReader := NewPutObjReader(rawReader, nil, nil)

	var opts ObjectOptions

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object, opts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	if globalAutoEncryption && !crypto.SSEC.IsRequested(r.Header) {
		r.Header.Add(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
	}
	var objectEncryptionKey []byte
	if objectAPI.IsEncryptionSupported() {
		if hasServerSideEncryptionHeader(r.Header) && !hasSuffix(object, slashSeparator) { // handle SSE requests
			reader, objectEncryptionKey, err = EncryptRequest(hashReader, r, bucket, object, metadata)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			info := ObjectInfo{Size: size}
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", size) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			pReader = NewPutObjReader(rawReader, hashReader, objectEncryptionKey)
		}
	}

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(metadata)

	if api.CacheAPI() != nil && !hasServerSideEncryptionHeader(r.Header) {
		putObject = api.CacheAPI().PutObject
	}

	// Create the object..
	objInfo, err := putObject(ctx, bucket, object, pReader, metadata, opts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	etag := objInfo.ETag
	if objInfo.IsCompressed() {
		// Ignore compressed ETag.
		etag = objInfo.ETag + "-1"
	} else if hasServerSideEncryptionHeader(r.Header) {
		etag = getDecryptedETag(r.Header, objInfo, false)
	}
	w.Header().Set("ETag", "\""+etag+"\"")

	if objectAPI.IsEncryptionSupported() {
		if crypto.IsEncrypted(objInfo.UserDefined) {
			switch {
			case crypto.S3.IsEncrypted(objInfo.UserDefined):
				w.Header().Set(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
			case crypto.SSEC.IsRequested(r.Header):
				w.Header().Set(crypto.SSECAlgorithm, r.Header.Get(crypto.SSECAlgorithm))
				w.Header().Set(crypto.SSECKeyMD5, r.Header.Get(crypto.SSECKeyMD5))
			}
		}
	}

	writeSuccessResponseHeadersOnly(w)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedPut,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
	})
}

/// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload.
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NewMultipartUpload")

	defer logger.AuditLog(w, r, "NewMultipartUpload", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r)) // SSE-KMS is not supported
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	var (
		opts ObjectOptions
		err  error
	)
	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object, opts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Validate storage class metadata if present
	if _, ok := r.Header[amzStorageClassCanonical]; ok {
		if !isValidStorageClassMeta(r.Header.Get(amzStorageClassCanonical)) {
			writeErrorResponse(w, ErrInvalidStorageClass, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	var encMetadata = map[string]string{}

	if globalAutoEncryption && !crypto.SSEC.IsRequested(r.Header) {
		r.Header.Add(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
	}
	if objectAPI.IsEncryptionSupported() {
		if hasServerSideEncryptionHeader(r.Header) {
			if err = setEncryptionMetadata(r, bucket, object, encMetadata); err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
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
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
		return
	}

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	for k, v := range encMetadata {
		metadata[k] = v
	}

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(metadata)

	if objectAPI.IsCompressionSupported() && isCompressible(r.Header, object) {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV1
	}

	newMultipartUpload := objectAPI.NewMultipartUpload
	if api.CacheAPI() != nil && !hasServerSideEncryptionHeader(r.Header) {
		newMultipartUpload = api.CacheAPI().NewMultipartUpload
	}
	uploadID, err := newMultipartUpload(ctx, bucket, object, metadata, opts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
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

	defer logger.AuditLog(w, r, "CopyObjectPart", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r)) // SSE-KMS is not supported
		return
	}
	if !objectAPI.IsEncryptionSupported() && (hasServerSideEncryptionHeader(r.Header) || crypto.SSECopy.IsRequested(r.Header)) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, dstBucket, dstObject); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	// Check https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectVersioning.html
	// Regardless of whether you have enabled versioning, each object in your bucket
	// has a version ID. If you have not enabled versioning, Amazon S3 sets the value
	// of the version ID to null. If you have enabled versioning, Amazon S3 assigns a
	// unique version ID value for the object.
	if u, err := url.Parse(cpSrcPath); err == nil {
		// Check if versionId query param was added, if yes then check if
		// its non "null" value, we should error out since we do not support
		// any versions other than "null".
		if vid := u.Query().Get("versionId"); vid != "" && vid != "null" {
			writeErrorResponse(w, ErrNoSuchVersion, r.URL, guessIsBrowserReq(r))
			return
		}
		cpSrcPath = u.Path
	}

	srcBucket, srcObject := path2BucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, srcBucket, srcObject); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, ErrInvalidPart, r.URL, guessIsBrowserReq(r))
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL, guessIsBrowserReq(r))
		return
	}

	var srcOpts, dstOpts ObjectOptions

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, dstBucket, dstObject, dstOpts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	getObjectNInfo := objectAPI.GetObjectNInfo
	if api.CacheAPI() != nil {
		getObjectNInfo = api.CacheAPI().GetObjectNInfo
	}

	// Get request range.
	var rs *HTTPRangeSpec
	rangeHeader := r.Header.Get("x-amz-copy-source-range")
	if rangeHeader != "" {
		var parseRangeErr error
		if rs, parseRangeErr = parseCopyPartRangeSpec(rangeHeader); parseRangeErr != nil {
			// Handle only errInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			logger.GetReqInfo(ctx).AppendTags("rangeHeader", rangeHeader)
			logger.LogIf(ctx, parseRangeErr)
			writeCopyPartErr(w, parseRangeErr, r.URL, guessIsBrowserReq(r))
			return

		}
	}

	gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, readLock, srcOpts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	defer gr.Close()
	srcInfo := gr.ObjInfo

	actualPartSize := srcInfo.Size
	if crypto.IsEncrypted(srcInfo.UserDefined) {
		actualPartSize, err = srcInfo.DecryptedSize()
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Special care for CopyObjectPart
	if partRangeErr := checkCopyPartRangeWithSize(rs, actualPartSize); partRangeErr != nil {
		writeCopyPartErr(w, partRangeErr, r.URL, guessIsBrowserReq(r))
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if checkCopyObjectPartPreconditions(w, r, srcInfo) {
		return
	}

	// Get the object offset & length
	startOffset, length, err := rs.GetOffsetLength(actualPartSize)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	/// maximum copy size for multipart objects in a single operation
	if isMaxAllowedPartSize(length) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL, guessIsBrowserReq(r))
		return
	}

	actualPartSize = length
	var reader io.Reader

	var li ListPartsInfo
	li, err = objectAPI.ListObjectParts(ctx, dstBucket, dstObject, uploadID, 0, 1)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Read compression metadata preserved in the init multipart for the decision.
	_, compressPart := li.UserDefined[ReservedMetadataPrefix+"compression"]
	isCompressed := compressPart
	// Compress only if the compression is enabled during initial multipart.
	if isCompressed {
		// Open a pipe for compression.
		// Where pipeWriter is piped to srcInfo.Reader.
		// gr writes to pipeWriter.
		pipeReader, pipeWriter := io.Pipe()
		reader = pipeReader
		length = -1

		snappyWriter := snappy.NewWriter(pipeWriter)

		go func() {
			// Compress the decompressed source object.
			_, cerr := io.Copy(snappyWriter, gr)
			snappyWriter.Close()
			pipeWriter.CloseWithError(cerr)
		}()
	} else {
		reader = gr
	}

	srcInfo.Reader, err = hash.NewReader(reader, length, "", "", actualPartSize)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rawReader := srcInfo.Reader
	pReader := NewPutObjReader(rawReader, nil, nil)

	isEncrypted := false
	var objectEncryptionKey []byte
	if objectAPI.IsEncryptionSupported() && !isCompressed {
		if crypto.IsEncrypted(li.UserDefined) {
			if !crypto.SSEC.IsRequested(r.Header) && crypto.SSEC.IsEncrypted(li.UserDefined) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL, guessIsBrowserReq(r))
				return
			}
			if crypto.S3.IsEncrypted(li.UserDefined) && crypto.SSEC.IsRequested(r.Header) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL, guessIsBrowserReq(r))
				return
			}
			isEncrypted = true // to detect SSE-S3 encryption
			var key []byte
			if crypto.SSEC.IsRequested(r.Header) {
				key, err = ParseSSECustomerRequest(r)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}
			objectEncryptionKey, err = decryptObjectInfo(key, dstBucket, dstObject, li.UserDefined)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}

			var partIDbin [4]byte
			binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

			mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
			mac.Write(partIDbin[:])
			partEncryptionKey := mac.Sum(nil)
			reader, err = sio.EncryptReader(reader, sio.Config{Key: partEncryptionKey})
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}

			info := ObjectInfo{Size: length}
			srcInfo.Reader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", length)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			pReader = NewPutObjReader(rawReader, srcInfo.Reader, objectEncryptionKey)
		}
	}

	srcInfo.PutObjReader = pReader
	// Copy source object to destination, if source and destination
	// object is same then only metadata is updated.
	partInfo, err := objectAPI.CopyObjectPart(ctx, srcBucket, srcObject, dstBucket, dstObject, uploadID, partID,
		startOffset, length, srcInfo, srcOpts, dstOpts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if isEncrypted {
		partInfo.ETag = tryDecryptETag(objectEncryptionKey, partInfo.ETag, crypto.SSEC.IsRequested(r.Header))
	}

	response := generateCopyObjectPartResponse(partInfo.ETag, partInfo.LastModified)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// PutObjectPartHandler - uploads an incoming part for an ongoing multipart operation.
func (api objectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectPart")

	defer logger.AuditLog(w, r, "PutObjectPart", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r)) // SSE-KMS is not supported
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL, guessIsBrowserReq(r))
		return
	}

	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header)
	if err != nil {
		writeErrorResponse(w, ErrInvalidDigest, r.URL, guessIsBrowserReq(r))
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength

	rAuthType := getRequestAuthType(r)
	// For auth type streaming signature, we need to gather a different content length.
	if rAuthType == authTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}
	if size == -1 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxAllowedPartSize(size) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL, guessIsBrowserReq(r))
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, ErrInvalidPart, r.URL, guessIsBrowserReq(r))
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL, guessIsBrowserReq(r))
		return
	}

	var (
		md5hex    = hex.EncodeToString(md5Bytes)
		sha256hex = ""
		reader    io.Reader
		s3Error   APIErrorCode
	)
	reader = r.Body
	if s3Error = isPutAllowed(rAuthType, bucket, object, r); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Error = newSignV4ChunkedReader(r)
		if s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		if s3Error = isReqAuthenticatedV2(r); s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error = reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
			return
		}

		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r)
		}
	}

	actualSize := size
	var pipeReader *io.PipeReader
	var pipeWriter *io.PipeWriter

	var li ListPartsInfo
	li, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, 0, 1)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	// Read compression metadata preserved in the init multipart for the decision.
	_, compressPart := li.UserDefined[ReservedMetadataPrefix+"compression"]

	isCompressed := false
	if objectAPI.IsCompressionSupported() && compressPart {
		pipeReader, pipeWriter = io.Pipe()
		snappyWriter := snappy.NewWriter(pipeWriter)

		var actualReader *hash.Reader
		actualReader, err = hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		go func() {
			// Writing to the compressed writer.
			_, cerr := io.CopyN(snappyWriter, actualReader, actualSize)
			snappyWriter.Close()
			pipeWriter.CloseWithError(cerr)
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
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rawReader := hashReader
	pReader := NewPutObjReader(rawReader, nil, nil)
	var opts ObjectOptions

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object, opts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	isEncrypted := false
	var objectEncryptionKey []byte
	if objectAPI.IsEncryptionSupported() && !isCompressed {
		var li ListPartsInfo
		li, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, 0, 1)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
		if crypto.IsEncrypted(li.UserDefined) {
			if !crypto.SSEC.IsRequested(r.Header) && crypto.SSEC.IsEncrypted(li.UserDefined) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL, guessIsBrowserReq(r))
				return
			}
			isEncrypted = true // to detect SSE-S3 encryption
			var key []byte
			if crypto.SSEC.IsRequested(r.Header) {
				key, err = ParseSSECustomerRequest(r)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}

			// Calculating object encryption key
			objectEncryptionKey, err = decryptObjectInfo(key, bucket, object, li.UserDefined)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}

			var partIDbin [4]byte
			binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

			mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
			mac.Write(partIDbin[:])
			partEncryptionKey := mac.Sum(nil)

			reader, err = sio.EncryptReader(hashReader, sio.Config{Key: partEncryptionKey})
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			info := ObjectInfo{Size: size}
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", size) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			pReader = NewPutObjReader(rawReader, hashReader, objectEncryptionKey)
		}
	}

	putObjectPart := objectAPI.PutObjectPart
	if api.CacheAPI() != nil && !isEncrypted {
		putObjectPart = api.CacheAPI().PutObjectPart
	}
	partInfo, err := putObjectPart(ctx, bucket, object, uploadID, partID, pReader, opts)
	if err != nil {
		// Verify if the underlying error is signature mismatch.
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	etag := partInfo.ETag
	if isCompressed {
		pipeWriter.Close()
		// Suppress compressed ETag.
		etag = partInfo.ETag + "-1"
	} else if isEncrypted {
		etag = tryDecryptETag(objectEncryptionKey, partInfo.ETag, crypto.SSEC.IsRequested(r.Header))
	}
	w.Header().Set("ETag", "\""+etag+"\"")

	writeSuccessResponseHeadersOnly(w)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api objectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AbortMultipartUpload")

	defer logger.AuditLog(w, r, "AbortMultipartUpload", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	abortMultipartUpload := objectAPI.AbortMultipartUpload
	if api.CacheAPI() != nil {
		abortMultipartUpload = api.CacheAPI().AbortMultipartUpload
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.AbortMultipartUploadAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object, ObjectOptions{}); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	uploadID, _, _, _, s3Error := getObjectResources(r.URL.Query())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}
	if err := abortMultipartUpload(ctx, bucket, object, uploadID); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api objectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectParts")

	defer logger.AuditLog(w, r, "ListObjectParts", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListMultipartUploadPartsAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	uploadID, partNumberMarker, maxParts, _, s3Error := getObjectResources(r.URL.Query())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}
	if partNumberMarker < 0 {
		writeErrorResponse(w, ErrInvalidPartNumberMarker, r.URL, guessIsBrowserReq(r))
		return
	}
	if maxParts < 0 {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL, guessIsBrowserReq(r))
		return
	}
	listPartsInfo, err := objectAPI.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	var ssec bool
	if objectAPI.IsEncryptionSupported() {
		var li ListPartsInfo
		li, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, 0, 1)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
		if crypto.IsEncrypted(li.UserDefined) {
			var key []byte
			if crypto.SSEC.IsEncrypted(li.UserDefined) {
				ssec = true
			}
			var objectEncryptionKey []byte
			if crypto.S3.IsEncrypted(li.UserDefined) {
				// Calculating object encryption key
				objectEncryptionKey, err = decryptObjectInfo(key, bucket, object, li.UserDefined)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}
			parts := make([]PartInfo, len(listPartsInfo.Parts))
			for i, p := range listPartsInfo.Parts {
				part := p
				part.ETag = tryDecryptETag(objectEncryptionKey, p.ETag, ssec)
				parts[i] = part
			}
			listPartsInfo.Parts = parts
		}
	}

	response := generateListPartsResponse(listPartsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload.
func (api objectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CompleteMultipartUpload")

	defer logger.AuditLog(w, r, "CompleteMultipartUpload", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object, ObjectOptions{}); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Get upload id.
	uploadID, _, _, _, s3Error := getObjectResources(r.URL.Query())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	completeMultipartBytes, err := goioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
		return
	}
	complMultipartUpload := &CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		writeErrorResponse(w, ErrMalformedXML, r.URL, guessIsBrowserReq(r))
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		writeErrorResponse(w, ErrMalformedXML, r.URL, guessIsBrowserReq(r))
		return
	}
	if !sort.IsSorted(CompletedParts(complMultipartUpload.Parts)) {
		writeErrorResponse(w, ErrInvalidPartOrder, r.URL, guessIsBrowserReq(r))
		return
	}
	var objectEncryptionKey []byte
	var opts ObjectOptions
	var isEncrypted, ssec bool

	if objectAPI.IsEncryptionSupported() {
		var li ListPartsInfo
		li, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, 0, 1)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
		if crypto.IsEncrypted(li.UserDefined) {
			isEncrypted = true
			ssec = crypto.SSEC.IsEncrypted(li.UserDefined)
			var key []byte
			if crypto.S3.IsEncrypted(li.UserDefined) {
				// Calculating object encryption key
				objectEncryptionKey, err = decryptObjectInfo(key, bucket, object, li.UserDefined)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}
		}
	}

	partsMap := make(map[string]PartInfo)
	var listPartsInfo ListPartsInfo

	if isEncrypted {
		var partNumberMarker int
		maxParts := 1000
		for {
			listPartsInfo, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			for _, part := range listPartsInfo.Parts {
				partsMap[strconv.Itoa(part.PartNumber)] = part
			}
			partNumberMarker = listPartsInfo.NextPartNumberMarker
			if !listPartsInfo.IsTruncated {
				break
			}
		}
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
		if isEncrypted {
			// ETag is stored in the backend in encrypted form. Validate client sent ETag with
			// decrypted ETag.
			if bkPartInfo, ok := partsMap[strconv.Itoa(part.PartNumber)]; ok {
				bkETag := tryDecryptETag(objectEncryptionKey, bkPartInfo.ETag, ssec)
				if bkETag != part.ETag {
					writeErrorResponse(w, ErrInvalidPart, r.URL, guessIsBrowserReq(r))
					return
				}
				part.ETag = bkPartInfo.ETag
			}
		}
		completeParts = append(completeParts, part)
	}

	completeMultiPartUpload := objectAPI.CompleteMultipartUpload
	if api.CacheAPI() != nil {
		completeMultiPartUpload = api.CacheAPI().CompleteMultipartUpload
	}
	objInfo, err := completeMultiPartUpload(ctx, bucket, object, uploadID, completeParts, opts)
	if err != nil {
		switch oErr := err.(type) {
		case PartTooSmall:
			// Write part too small error.
			writePartSmallErrorResponse(w, r, oErr)
		default:
			// Handle all other generic issues.
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		}
		return
	}

	// Get object location.
	location := getObjectLocation(r, globalDomainName, bucket, object)
	// Generate complete multipart response.
	response := generateCompleteMultpartUploadResponse(bucket, object, location, objInfo.ETag)
	encodedSuccessResponse := encodeResponse(response)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
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
		EventName:    event.ObjectCreatedCompleteMultipartUpload,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
	})
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api objectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteObject")

	defer logger.AuditLog(w, r, "DeleteObject", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	if vid := r.URL.Query().Get("versionId"); vid != "" && vid != "null" {
		writeErrorResponse(w, ErrNoSuchVersion, r.URL, guessIsBrowserReq(r))
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		// Not required to check whether given object exists or not, because
		// DeleteObject is always successful irrespective of object existence.
		writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
		return
	}

	if globalDNSConfig != nil {
		_, err := globalDNSConfig.Get(bucket)
		if err != nil {
			if err == dns.ErrNoEntriesFound {
				writeErrorResponse(w, ErrNoSuchBucket, r.URL, guessIsBrowserReq(r))
			} else {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			}
			return
		}
	}

	// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	if err := deleteObject(ctx, objectAPI, api.CacheAPI(), bucket, object, r); err != nil {
		switch toAPIErrorCode(ctx, err) {
		case ErrNoSuchBucket:
			// When bucket doesn't exist specially handle it.
			writeErrorResponse(w, ErrNoSuchBucket, r.URL, guessIsBrowserReq(r))
			return
		}
		// Ignore delete object errors while replying to client, since we are suppposed to reply only 204.
	}
	writeSuccessNoContent(w)
}
