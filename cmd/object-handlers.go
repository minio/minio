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

	defer logger.AuditLog(ctx, r)

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) { // SSE-KMS is not supported
		writeErrorResponse(w, ErrNotImplemented, r.URL)
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
				if toAPIErrorCode(err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Get request range.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		writeErrorResponse(w, ErrUnsupportedRangeHeader, r.URL)
		return
	}

	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrEmptyRequestBody, r.URL)
		return
	}

	var selectReq ObjectSelectRequest
	if err := xmlDecoder(r.Body, &selectReq, r.ContentLength); err != nil {
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}

	if !strings.EqualFold(string(selectReq.ExpressionType), "SQL") {
		writeErrorResponse(w, ErrInvalidExpressionType, r.URL)
		return
	}
	if len(selectReq.Expression) >= s3select.MaxExpressionLength {
		writeErrorResponse(w, ErrExpressionTooLong, r.URL)
		return
	}

	getObjectNInfo := objectAPI.GetObjectNInfo
	if api.CacheAPI() != nil {
		getObjectNInfo = api.CacheAPI().GetObjectNInfo
	}

	gr, err := getObjectNInfo(ctx, bucket, object, nil, r.Header, readLock, ObjectOptions{})
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	defer gr.Close()

	objInfo := gr.ObjInfo

	if selectReq.InputSerialization.CompressionType == SelectCompressionGZIP {
		if !strings.Contains(objInfo.ContentType, "gzip") {
			writeErrorResponse(w, ErrInvalidDataSource, r.URL)
			return
		}
	}
	if selectReq.InputSerialization.CompressionType == SelectCompressionBZIP {
		if !strings.Contains(objInfo.ContentType, "bzip") {
			writeErrorResponse(w, ErrInvalidDataSource, r.URL)
			return
		}
	}
	if selectReq.InputSerialization.CompressionType == SelectCompressionNONE ||
		selectReq.InputSerialization.CompressionType == "" {
		selectReq.InputSerialization.CompressionType = SelectCompressionNONE
		if !strings.Contains(objInfo.ContentType, "text/csv") {
			writeErrorResponse(w, ErrInvalidDataSource, r.URL)
			return
		}
	}
	if !strings.EqualFold(string(selectReq.ExpressionType), "SQL") {
		writeErrorResponse(w, ErrInvalidExpressionType, r.URL)
		return
	}
	if len(selectReq.Expression) >= s3select.MaxExpressionLength {
		writeErrorResponse(w, ErrExpressionTooLong, r.URL)
		return
	}
	if selectReq.InputSerialization.CSV == nil || selectReq.OutputSerialization.CSV == nil {
		writeErrorResponse(w, ErrInvalidRequestParameter, r.URL)
		return
	}
	if selectReq.InputSerialization.CSV.FileHeaderInfo != CSVFileHeaderInfoUse &&
		selectReq.InputSerialization.CSV.FileHeaderInfo != CSVFileHeaderInfoNone &&
		selectReq.InputSerialization.CSV.FileHeaderInfo != CSVFileHeaderInfoIgnore &&
		selectReq.InputSerialization.CSV.FileHeaderInfo != "" {
		writeErrorResponse(w, ErrInvalidFileHeaderInfo, r.URL)
		return
	}
	if selectReq.OutputSerialization.CSV.QuoteFields != CSVQuoteFieldsAlways &&
		selectReq.OutputSerialization.CSV.QuoteFields != CSVQuoteFieldsAsNeeded &&
		selectReq.OutputSerialization.CSV.QuoteFields != "" {
		writeErrorResponse(w, ErrInvalidQuoteFields, r.URL)
		return
	}
	if len(selectReq.InputSerialization.CSV.RecordDelimiter) > 2 {
		writeErrorResponse(w, ErrInvalidRequestParameter, r.URL)
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

	//s3select //Options
	if selectReq.OutputSerialization.CSV.FieldDelimiter == "" {
		selectReq.OutputSerialization.CSV.FieldDelimiter = ","
	}
	if selectReq.InputSerialization.CSV.FileHeaderInfo == "" {
		selectReq.InputSerialization.CSV.FileHeaderInfo = CSVFileHeaderInfoNone
	}
	if selectReq.InputSerialization.CSV.RecordDelimiter == "" {
		selectReq.InputSerialization.CSV.RecordDelimiter = "\n"
	}
	if selectReq.InputSerialization.CSV != nil {
		options := &s3select.Options{
			HasHeader:            selectReq.InputSerialization.CSV.FileHeaderInfo != CSVFileHeaderInfoNone,
			RecordDelimiter:      selectReq.InputSerialization.CSV.RecordDelimiter,
			FieldDelimiter:       selectReq.InputSerialization.CSV.FieldDelimiter,
			Comments:             selectReq.InputSerialization.CSV.Comments,
			Name:                 "S3Object", // Default table name for all objects
			ReadFrom:             gr,
			Compressed:           string(selectReq.InputSerialization.CompressionType),
			Expression:           selectReq.Expression,
			OutputFieldDelimiter: selectReq.OutputSerialization.CSV.FieldDelimiter,
			StreamSize:           objInfo.Size,
			HeaderOpt:            selectReq.InputSerialization.CSV.FileHeaderInfo == CSVFileHeaderInfoUse,
			Progress:             selectReq.RequestProgress.Enabled,
		}
		s3s, err := s3select.NewInput(options)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		_, _, _, _, _, _, err = s3s.ParseSelect(selectReq.Expression)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		if err = s3s.Execute(w); err != nil {
			logger.LogIf(ctx, err)
		}
	}

	for k, v := range objInfo.UserDefined {
		logger.GetReqInfo(ctx).SetTags(k, v)
	}

	logger.GetReqInfo(ctx).SetTags("etag", objInfo.ETag)
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api objectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObject")

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponse(w, ErrBadRequest, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	var opts ObjectOptions

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

				_, err := getObjectInfo(ctx, bucket, object, opts)
				if toAPIErrorCode(err) == ErrNoSuchKey {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(w, s3Error, r.URL)
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
		var err error
		if rs, err = parseRequestRangeSpec(rangeHeader); err != nil {
			// Handle only errInvalidRange. Ignore other
			// parse error and treat it as regular Get
			// request like Amazon S3.
			if err == errInvalidRange {
				writeErrorResponse(w, ErrInvalidRange, r.URL)
				return
			}

			logger.LogIf(ctx, err)
		}
	}

	gr, err := getObjectNInfo(ctx, bucket, object, rs, r.Header, readLock, opts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	defer gr.Close()
	objInfo := gr.ObjInfo

	if objectAPI.IsEncryptionSupported() {
		if _, err = DecryptObjectInfo(objInfo, r.Header); err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
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

	if hErr := setObjectHeaders(w, objInfo, rs); hErr != nil {
		writeErrorResponse(w, toAPIErrorCode(hErr), r.URL)
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

	for k, v := range objInfo.UserDefined {
		logger.GetReqInfo(ctx).SetTags(k, v)
	}

	logger.GetReqInfo(ctx).SetTags("etag", objInfo.ETag)
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api objectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadObject")

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponse(w, ErrBadRequest, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	getObjectInfo := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfo = api.CacheAPI().GetObjectInfo
	}

	var opts ObjectOptions
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
				_, err := getObjectInfo(ctx, bucket, object, opts)
				if toAPIErrorCode(err) == ErrNoSuchKey {
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
		var err error
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
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(err))
		return
	}

	if objectAPI.IsEncryptionSupported() {
		if _, err = DecryptObjectInfo(objInfo, r.Header); err != nil {
			writeErrorResponseHeadersOnly(w, toAPIErrorCode(err))
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

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Set standard object headers.
	if hErr := setObjectHeaders(w, objInfo, rs); hErr != nil {
		writeErrorResponse(w, toAPIErrorCode(hErr), r.URL)
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

	for k, v := range objInfo.UserDefined {
		logger.GetReqInfo(ctx).SetTags(k, v)
	}

	logger.GetReqInfo(ctx).SetTags("etag", objInfo.ETag)
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
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CopyObject")

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL) // SSE-KMS is not supported
		return
	}

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

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

	var srcOpts, dstOpts ObjectOptions

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, dstBucket, dstObject, dstOpts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	getObjectNInfo := objectAPI.GetObjectNInfo
	if api.CacheAPI() != nil {
		getObjectNInfo = api.CacheAPI().GetObjectNInfo
	}

	// Get request range.
	var rs *HTTPRangeSpec
	rangeHeader := r.Header.Get("x-amz-copy-source-range")
	if rangeHeader != "" {
		var parseRangeErr error
		if rs, parseRangeErr = parseRequestRangeSpec(rangeHeader); parseRangeErr != nil {
			// Handle only errInvalidRange. Ignore other
			// parse error and treat it as regular Get
			// request like Amazon S3.
			if parseRangeErr == errInvalidRange {
				writeErrorResponse(w, ErrInvalidRange, r.URL)
				return
			}

			// log the error.
			logger.LogIf(ctx, parseRangeErr)
		}
	}

	var lock = noLock
	if !cpSrcDstSame {
		lock = readLock
	}

	gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, lock, srcOpts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
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
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	// We have to copy metadata only if source and destination are same.
	// this changes for encryption which can be observed below.
	if cpSrcDstSame {
		srcInfo.metadataOnly = true
	}

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

	var reader io.Reader
	var length = srcInfo.Size
	// No need to compress for remote etcd calls
	// Pass the decompressed stream to such calls.
	if srcInfo.IsCompressed() && !isRemoteCallRequired(ctx, srcBucket, dstBucket, objectAPI) {
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

	srcInfo.Reader, err = hash.NewReader(reader, length, "", "", srcInfo.Size)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Save the original size for later use when we want to copy
	// encrypted file into an unencrypted one.
	size := srcInfo.Size

	var encMetadata = make(map[string]string)
	if objectAPI.IsEncryptionSupported() && !srcInfo.IsCompressed() {
		var oldKey, newKey []byte
		sseCopyS3 := crypto.S3.IsEncrypted(srcInfo.UserDefined)
		sseCopyC := crypto.SSECopy.IsRequested(r.Header)
		sseC := crypto.SSEC.IsRequested(r.Header)
		sseS3 := crypto.S3.IsRequested(r.Header)
		if sseC || sseS3 {
			if sseC {
				newKey, err = ParseSSECustomerRequest(r)
			}
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
		// AWS S3 implementation requires us to only rotate keys
		// when/ both keys are provided and destination is same
		// otherwise we proceed to encrypt/decrypt.
		if sseCopyC && sseC && cpSrcDstSame {
			// Get the old key which needs to be rotated.
			oldKey, err = ParseSSECopyCustomerRequest(r.Header, srcInfo.UserDefined)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			for k, v := range srcInfo.UserDefined {
				encMetadata[k] = v
			}
			if err = rotateKey(oldKey, newKey, srcBucket, srcObject, encMetadata); err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}

			// Since we are rotating the keys, make sure to update the metadata.
			srcInfo.metadataOnly = true
		} else {
			if sseCopyC || sseCopyS3 {
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
				if sseC || sseS3 {
					size = srcInfo.Size
				}
			}
			if sseC || sseS3 {
				reader, err = newEncryptReader(reader, newKey, dstBucket, dstObject, encMetadata, sseS3)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
				// We are not only copying just metadata instead
				// we are creating a new object at this point, even
				// if source and destination are same objects.
				srcInfo.metadataOnly = false
				if !sseCopyC && !sseCopyS3 {
					size = srcInfo.EncryptedSize()
				}
			} else {
				if sseCopyC || sseCopyS3 {
					size, _ = srcInfo.DecryptedSize()
					delete(srcInfo.UserDefined, crypto.SSEIV)
					delete(srcInfo.UserDefined, crypto.SSESealAlgorithm)
					delete(srcInfo.UserDefined, crypto.SSECSealedKey)
					delete(srcInfo.UserDefined, crypto.SSEMultipart)
					delete(srcInfo.UserDefined, crypto.S3SealedKey)
					delete(srcInfo.UserDefined, crypto.S3KMSSealedKey)
					delete(srcInfo.UserDefined, crypto.S3KMSKeyID)
				}
			}

			srcInfo.Reader, err = hash.NewReader(reader, size, "", "", size) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}

	srcInfo.UserDefined, err = getCpObjMetadataFromHeader(ctx, r, srcInfo.UserDefined)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
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
	if !isMetadataReplace(r.Header) && srcInfo.metadataOnly && !crypto.SSEC.IsEncrypted(srcInfo.UserDefined) {
		// If x-amz-metadata-directive is not set to REPLACE then we need
		// to error out if source and destination are same.
		writeErrorResponse(w, ErrInvalidCopyDest, r.URL)
		return
	}

	var objInfo ObjectInfo

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
			// Send PutObject request to appropriate instance (in federated deployment)
			host, port := getRandomHostPort(dstRecords)
			client, rerr := getRemoteInstanceClient(host, port)
			if rerr != nil {
				writeErrorResponse(w, ErrInternalError, r.URL)
				return
			}
			remoteObjInfo, rerr := client.PutObject(dstBucket, dstObject, srcInfo.Reader, srcInfo.Size, "", "", srcInfo.UserDefined, dstOpts.ServerSideEncryption)
			if rerr != nil {
				writeErrorResponse(w, ErrInternalError, r.URL)
				return
			}
			objInfo.ETag = remoteObjInfo.ETag
			objInfo.ModTime = remoteObjInfo.LastModified
		}
	} else {
		// Copy source object to destination, if source and destination
		// object is same then only metadata is updated.
		objInfo, err = objectAPI.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}

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
		objInfo.Size = srcInfo.GetActualSize()
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

	for k, v := range objInfo.UserDefined {
		logger.GetReqInfo(ctx).SetTags(k, v)
	}

	logger.GetReqInfo(ctx).SetTags("etag", objInfo.ETag)
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

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL) // SSE-KMS is not supported
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, ErrInvalidCopySource, r.URL)
		return
	}

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

	// Check if put is allowed
	if s3Err = isPutAllowed(rAuthType, bucket, object, r); s3Err != ErrNone {
		writeErrorResponse(w, s3Err, r.URL)
		return
	}

	switch rAuthType {
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
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
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
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	opts := ObjectOptions{}
	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object, opts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
			return
		}
	}

	if objectAPI.IsEncryptionSupported() {
		if hasServerSideEncryptionHeader(r.Header) && !hasSuffix(object, slashSeparator) { // handle SSE requests
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

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(metadata)

	if api.CacheAPI() != nil && !hasServerSideEncryptionHeader(r.Header) {
		putObject = api.CacheAPI().PutObject
	}

	// Create the object..
	objInfo, err := putObject(ctx, bucket, object, hashReader, metadata, opts)
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
		EventName:  event.ObjectCreatedPut,
		BucketName: bucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       host,
		Port:       port,
	})

	for k, v := range objInfo.UserDefined {
		logger.GetReqInfo(ctx).SetTags(k, v)
	}

	logger.GetReqInfo(ctx).SetTags("etag", objInfo.ETag)
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

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL) // SSE-KMS is not supported
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	opts := ObjectOptions{}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object, opts); err == nil {
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
		if hasServerSideEncryptionHeader(r.Header) {
			if err := setEncryptionMetadata(r, bucket, object, encMetadata); err != nil {
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

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL) // SSE-KMS is not supported
		return
	}

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject := vars["object"]

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
	var srcOpts, dstOpts ObjectOptions

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, dstBucket, dstObject, dstOpts); err == nil {
			writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
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
			writeCopyPartErr(w, parseRangeErr, r.URL)
			return

		}
	}

	gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, readLock, srcOpts)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	defer gr.Close()
	srcInfo := gr.ObjInfo

	var actualPartSize int64
	actualPartSize = srcInfo.Size

	// Special care for CopyObjectPart
	if partRangeErr := checkCopyPartRangeWithSize(rs, srcInfo.Size); partRangeErr != nil {
		writeCopyPartErr(w, partRangeErr, r.URL)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if checkCopyObjectPartPreconditions(w, r, srcInfo) {
		return
	}

	// Get the object offset & length
	startOffset, length, _ := rs.GetOffsetLength(srcInfo.Size)

	if rangeHeader != "" {
		actualPartSize = length
	}

	if objectAPI.IsEncryptionSupported() {
		if crypto.IsEncrypted(srcInfo.UserDefined) {
			decryptedSize, decryptErr := srcInfo.DecryptedSize()
			if decryptErr != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
			startOffset, length, _ = rs.GetOffsetLength(decryptedSize)
		}
	}

	/// maximum copy size for multipart objects in a single operation
	if isMaxAllowedPartSize(length) {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	var reader io.Reader
	var getLength = length

	// Need to decompress only for range-enabled copy parts.
	if srcInfo.IsCompressed() && rangeHeader != "" {
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
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	if objectAPI.IsEncryptionSupported() && !srcInfo.IsCompressed() {
		var li ListPartsInfo
		li, err = objectAPI.ListObjectParts(ctx, dstBucket, dstObject, uploadID, 0, 1)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
		if crypto.IsEncrypted(li.UserDefined) {
			if !hasServerSideEncryptionHeader(r.Header) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL)
				return
			}
			var key []byte
			if crypto.SSEC.IsRequested(r.Header) {
				key, err = ParseSSECustomerRequest(r)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
			}
			var objectEncryptionKey []byte
			objectEncryptionKey, err = decryptObjectInfo(key, dstBucket, dstObject, li.UserDefined)
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

			info := ObjectInfo{Size: length}
			size := info.EncryptedSize()
			srcInfo.Reader, err = hash.NewReader(reader, size, "", "", actualPartSize)
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}
	// Copy source object to destination, if source and destination
	// object is same then only metadata is updated.
	partInfo, err := objectAPI.CopyObjectPart(ctx, srcBucket, srcObject, dstBucket,
		dstObject, uploadID, partID, startOffset, getLength, srcInfo, srcOpts, dstOpts)
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
	ctx := newContext(r, w, "PutObjectPart")

	defer logger.AuditLog(ctx, r)

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL) // SSE-KMS is not supported
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

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
		s3Error   APIErrorCode
	)
	reader = r.Body

	if s3Error = isPutAllowed(rAuthType, bucket, object, r); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Error = newSignV4ChunkedReader(r)
		if s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		if s3Error = isReqAuthenticatedV2(r); s3Error != ErrNone {
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error = reqSignatureV4Verify(r, globalServerConfig.GetRegion()); s3Error != ErrNone {
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

	var li ListPartsInfo
	li, err = objectAPI.ListObjectParts(ctx, bucket, object, uploadID, 0, 1)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
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
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
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
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	opts := ObjectOptions{}
	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = objectAPI.GetObjectInfo(ctx, bucket, object, opts); err == nil {
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
		if crypto.IsEncrypted(li.UserDefined) {
			if !hasServerSideEncryptionHeader(r.Header) {
				writeErrorResponse(w, ErrSSEMultipartEncrypted, r.URL)
				return
			}
			var key []byte
			if crypto.SSEC.IsRequested(r.Header) {
				key, err = ParseSSECustomerRequest(r)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(err), r.URL)
					return
				}
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
	if api.CacheAPI() != nil && !hasServerSideEncryptionHeader(r.Header) {
		putObjectPart = api.CacheAPI().PutObjectPart
	}
	partInfo, err := putObjectPart(ctx, bucket, object, uploadID, partID, hashReader, opts)
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

	defer logger.AuditLog(ctx, r)

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
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object, ObjectOptions{}); err == nil {
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

	defer logger.AuditLog(ctx, r)

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

	defer logger.AuditLog(ctx, r)

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
		if _, err := objectAPI.GetObjectInfo(ctx, bucket, object, ObjectOptions{}); err == nil {
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

	for k, v := range objInfo.UserDefined {
		logger.GetReqInfo(ctx).SetTags(k, v)
	}

	logger.GetReqInfo(ctx).SetTags("etag", objInfo.ETag)
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api objectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteObject")

	defer logger.AuditLog(ctx, r)

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
