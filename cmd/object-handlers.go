/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	mux "github.com/gorilla/mux"
)

// supportedGetReqParams - supported request parameters for GET presigned request.
var supportedGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
	"response-content-encoding":    "Content-Encoding",
	"response-content-language":    "Content-Language",
	"response-content-disposition": "Content-Disposition",
}

// setGetRespHeaders - set any requested parameters as response headers.
func setGetRespHeaders(w http.ResponseWriter, reqParams url.Values) {
	for k, v := range reqParams {
		if header, ok := supportedGetReqParams[k]; ok {
			w.Header()[header] = v
		}
	}
}

// errAllowableNotFound - For an anon user, return 404 if have ListBucket, 403 otherwise
// this is in keeping with the permissions sections of the docs of both:
//   HEAD Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//   GET Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func errAllowableObjectNotFound(bucket string, r *http.Request) APIErrorCode {
	if getRequestAuthType(r) == authTypeAnonymous {
		//we care about the bucket as a whole, not a particular resource
		url := *r.URL
		url.Path = "/" + bucket

		if s3Error := enforceBucketPolicy(bucket, "s3:ListBucket", &url); s3Error != ErrNone {
			return ErrAccessDenied
		}
	}
	return ErrNoSuchKey
}

// Simple way to convert a func to io.Writer type.
type funcToWriter func([]byte) (int, error)

func (f funcToWriter) Write(p []byte) (int, error) {
	return f(p)
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
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:GetObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	objInfo, err := objectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info.")
		apiErr := toAPIErrorCode(err)
		if apiErr == ErrNoSuchKey {
			apiErr = errAllowableObjectNotFound(bucket, r)
		}
		writeErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}

	// Get request range.
	var hrange *httpRange
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if hrange, err = parseRequestRange(rangeHeader, objInfo.Size); err != nil {
			// Handle only errInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == errInvalidRange {
				writeErrorResponse(w, r, ErrInvalidRange, r.URL.Path)
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
	startOffset := int64(0)
	length := objInfo.Size
	if hrange != nil {
		startOffset = hrange.offsetBegin
		length = hrange.getLength()
	}
	// Indicates if any data was written to the http.ResponseWriter
	dataWritten := false
	// io.Writer type which keeps track if any data was written.
	writer := funcToWriter(func(p []byte) (int, error) {
		if !dataWritten {
			// Set headers on the first write.
			// Set standard object headers.
			setObjectHeaders(w, objInfo, hrange)

			// Set any additional requested response headers.
			setGetRespHeaders(w, r.URL.Query())

			dataWritten = true
		}
		return w.Write(p)
	})

	// Reads the object at startOffset and writes to mw.
	if err := objectAPI.GetObject(bucket, object, startOffset, length, writer); err != nil {
		errorIf(err, "Unable to write to client.")
		if !dataWritten {
			// Error response only if no data has been written to client yet. i.e if
			// partial data has already been written before an error
			// occurred then no point in setting StatusCode and
			// sending error XML.
			apiErr := toAPIErrorCode(err)
			writeErrorResponse(w, r, apiErr, r.URL.Path)
		}
		return
	}
	if !dataWritten {
		// If ObjectAPI.GetObject did not return error and no data has
		// been written it would mean that it is a 0-byte object.
		// call wrter.Write(nil) to set appropriate headers.
		writer.Write(nil)
	}
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
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:GetObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	objInfo, err := objectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info.")
		apiErr := toAPIErrorCode(err)
		if apiErr == ErrNoSuchKey {
			apiErr = errAllowableObjectNotFound(bucket, r)
		}
		writeErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Set standard object headers.
	setObjectHeaders(w, objInfo, nil)

	// Successful response.
	w.WriteHeader(http.StatusOK)
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
func (api objectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:PutObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// TODO: Reject requests where body/payload is present, for now we don't even read it.

	// objectSource
	objectSource, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		objectSource = r.Header.Get("X-Amz-Copy-Source")
	}

	// Skip the first element if it is '/', split the rest.
	objectSource = strings.TrimPrefix(objectSource, "/")
	splits := strings.SplitN(objectSource, "/", 2)

	// Save sourceBucket and sourceObject extracted from url Path.
	var sourceBucket, sourceObject string
	if len(splits) == 2 {
		sourceBucket = splits[0]
		sourceObject = splits[1]
	}
	// If source object is empty, reply back error.
	if sourceObject == "" {
		writeErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}

	// Source and destination objects cannot be same, reply back error.
	if sourceObject == object && sourceBucket == bucket {
		writeErrorResponse(w, r, ErrInvalidCopyDest, r.URL.Path)
		return
	}

	objInfo, err := objectAPI.GetObjectInfo(sourceBucket, sourceObject)
	if err != nil {
		errorIf(err, "Unable to fetch object info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), objectSource)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if checkCopyObjectPreconditions(w, r, objInfo) {
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(objInfo.Size) {
		writeErrorResponse(w, r, ErrEntityTooLarge, objectSource)
		return
	}

	// Size of object.
	size := objInfo.Size

	pipeReader, pipeWriter := io.Pipe()
	go func() {
		startOffset := int64(0) // Read the whole file.
		// Get the object.
		gErr := objectAPI.GetObject(sourceBucket, sourceObject, startOffset, size, pipeWriter)
		if gErr != nil {
			errorIf(gErr, "Unable to read an object.")
			pipeWriter.CloseWithError(gErr)
			return
		}
		pipeWriter.Close() // Close.
	}()

	// Save other metadata if available.
	metadata := objInfo.UserDefined

	// Remove the etag from source metadata because if it was uploaded as a multipart object
	// then its ETag will not be MD5sum of the object.
	delete(metadata, "md5Sum")

	sha256sum := ""
	// Create the object.
	objInfo, err = objectAPI.PutObject(bucket, object, size, pipeReader, metadata, sha256sum)
	if err != nil {
		// Close the this end of the pipe upon error in PutObject.
		pipeReader.CloseWithError(err)
		errorIf(err, "Unable to create an object.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	// Explicitly close the reader, before fetching object info.
	pipeReader.Close()

	md5Sum := objInfo.MD5Sum
	response := generateCopyObjectResponse(md5Sum, objInfo.ModTime)
	encodedSuccessResponse := encodeResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)

	// Notify object created event.
	eventNotify(eventData{
		Type:    ObjectCreatedCopy,
		Bucket:  bucket,
		ObjInfo: objInfo,
		ReqParams: map[string]string{
			"sourceIPAddress": r.RemoteAddr,
		},
	})
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api objectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		errorIf(err, "Unable to validate content-md5 format.")
		writeErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
		return
	}

	/// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	rAuthType := getRequestAuthType(r)
	if rAuthType == authTypeStreamingSigned {
		sizeStr := r.Header.Get("x-amz-decoded-content-length")
		size, err = strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			errorIf(err, "Unable to parse `x-amz-decoded-content-length` into its integer value", sizeStr)
			writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
			return
		}
	}
	if size == -1 && !contains(r.TransferEncoding, "chunked") {
		writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}

	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
		return
	}

	// Extract metadata to be saved from incoming HTTP header.
	metadata := extractMetadataFromHeader(r.Header)
	// Make sure we hex encode md5sum here.
	metadata["md5Sum"] = hex.EncodeToString(md5Bytes)

	sha256sum := ""

	var objInfo ObjectInfo
	switch rAuthType {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy(bucket, "s3:PutObject", r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// Create anonymous object.
		objInfo, err = objectAPI.PutObject(bucket, object, size, r.Body, metadata, sha256sum)
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Error := newSignV4ChunkedReader(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		objInfo, err = objectAPI.PutObject(bucket, object, size, reader, metadata, sha256sum)
	case authTypeSignedV2, authTypePresignedV2:
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		objInfo, err = objectAPI.PutObject(bucket, object, size, r.Body, metadata, sha256sum)
	case authTypePresigned, authTypeSigned:
		if s3Error := reqSignatureV4Verify(r); s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		if !skipContentSha256Cksum(r) {
			sha256sum = r.Header.Get("X-Amz-Content-Sha256")
		}
		// Create object.
		objInfo, err = objectAPI.PutObject(bucket, object, size, r.Body, metadata, sha256sum)
	}
	if err != nil {
		errorIf(err, "Unable to create an object.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	w.Header().Set("ETag", "\""+objInfo.MD5Sum+"\"")
	writeSuccessResponse(w, nil)

	// Notify object created event.
	eventNotify(eventData{
		Type:    ObjectCreatedPut,
		Bucket:  bucket,
		ObjInfo: objInfo,
		ReqParams: map[string]string{
			"sourceIPAddress": r.RemoteAddr,
		},
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
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:PutObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Extract metadata that needs to be saved.
	metadata := extractMetadataFromHeader(r.Header)

	uploadID, err := objectAPI.NewMultipartUpload(bucket, object, metadata)
	if err != nil {
		errorIf(err, "Unable to initiate new multipart upload id.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	response := generateInitiateMultipartUploadResponse(bucket, object, uploadID)
	encodedSuccessResponse := encodeResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// PutObjectPartHandler - Upload part
func (api objectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		writeErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
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
			errorIf(err, "Unable to parse `x-amz-decoded-content-length` into its integer value", sizeStr)
			writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
			return
		}
	}
	if size == -1 {
		writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, r, ErrInvalidPart, r.URL.Path)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(w, r, ErrInvalidMaxParts, r.URL.Path)
		return
	}

	var partMD5 string
	incomingMD5 := hex.EncodeToString(md5Bytes)
	sha256sum := ""
	switch rAuthType {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy(bucket, "s3:PutObject", r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// No need to verify signature, anonymous request access is already allowed.
		partMD5, err = objectAPI.PutObjectPart(bucket, object, uploadID, partID, size, r.Body, incomingMD5, sha256sum)
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Error := newSignV4ChunkedReader(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		partMD5, err = objectAPI.PutObjectPart(bucket, object, uploadID, partID, size, reader, incomingMD5, sha256sum)
	case authTypeSignedV2, authTypePresignedV2:
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		partMD5, err = objectAPI.PutObjectPart(bucket, object, uploadID, partID, size, r.Body, incomingMD5, sha256sum)
	case authTypePresigned, authTypeSigned:
		if s3Error := reqSignatureV4Verify(r); s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}

		if !skipContentSha256Cksum(r) {
			sha256sum = r.Header.Get("X-Amz-Content-Sha256")
		}
		partMD5, err = objectAPI.PutObjectPart(bucket, object, uploadID, partID, size, r.Body, incomingMD5, sha256sum)
	}
	if err != nil {
		errorIf(err, "Unable to create object part.")
		// Verify if the underlying error is signature mismatch.
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	if partMD5 != "" {
		w.Header().Set("ETag", "\""+partMD5+"\"")
	}
	writeSuccessResponse(w, nil)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api objectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:AbortMultipartUpload", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	if err := objectAPI.AbortMultipartUpload(bucket, object, uploadID); err != nil {
		errorIf(err, "Unable to abort multipart upload.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
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
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListMultipartUploadParts", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	uploadID, partNumberMarker, maxParts, _ := getObjectResources(r.URL.Query())
	if partNumberMarker < 0 {
		writeErrorResponse(w, r, ErrInvalidPartNumberMarker, r.URL.Path)
		return
	}
	if maxParts < 0 {
		writeErrorResponse(w, r, ErrInvalidMaxParts, r.URL.Path)
		return
	}
	listPartsInfo, err := objectAPI.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		errorIf(err, "Unable to list uploaded parts.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	response := generateListPartsResponse(listPartsInfo)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers.
	setCommonHeaders(w)
	// Write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload.
func (api objectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:PutObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	var md5Sum string
	completeMultipartBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorIf(err, "Unable to complete multipart upload.")
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	complMultipartUpload := &completeMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		errorIf(err, "Unable to parse complete multipart upload XML.")
		writeErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		writeErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}
	if !sort.IsSorted(completedParts(complMultipartUpload.Parts)) {
		writeErrorResponse(w, r, ErrInvalidPartOrder, r.URL.Path)
		return
	}
	// Complete parts.
	var completeParts []completePart
	for _, part := range complMultipartUpload.Parts {
		part.ETag = strings.TrimPrefix(part.ETag, "\"")
		part.ETag = strings.TrimSuffix(part.ETag, "\"")
		completeParts = append(completeParts, part)
	}

	md5Sum, err = objectAPI.CompleteMultipartUpload(bucket, object, uploadID, completeParts)
	if err != nil {
		err = errorCause(err)
		errorIf(err, "Unable to complete multipart upload.")
		switch oErr := err.(type) {
		case PartTooSmall:
			// Write part too small error.
			writePartSmallErrorResponse(w, r, oErr)
		default:
			// Handle all other generic issues.
			writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		}
		return
	}

	// Get object location.
	location := getLocation(r)
	// Generate complete multipart response.
	response := generateCompleteMultpartUploadResponse(bucket, object, location, md5Sum)
	encodedSuccessResponse := encodeResponse(response)
	if err != nil {
		errorIf(err, "Unable to parse CompleteMultipartUpload response")
		writeErrorResponseNoHeader(w, r, ErrInternalError, r.URL.Path)
		return
	}

	// Set etag.
	w.Header().Set("ETag", "\""+md5Sum+"\"")

	// Write success response.
	w.Write(encodedSuccessResponse)
	w.(http.Flusher).Flush()

	// Fetch object info for notifications.
	objInfo, err := objectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info for \"%s\"", path.Join(bucket, object))
		return
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:    ObjectCreatedCompleteMultipartUpload,
		Bucket:  bucket,
		ObjInfo: objInfo,
		ReqParams: map[string]string{
			"sourceIPAddress": r.RemoteAddr,
		},
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
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:DeleteObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	/// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	/// Ignore delete object errors, since we are suppposed to reply
	/// only 204.
	if err := objectAPI.DeleteObject(bucket, object); err != nil {
		writeSuccessNoContent(w)
		return
	}
	writeSuccessNoContent(w)

	// Notify object deleted event.
	eventNotify(eventData{
		Type:   ObjectRemovedDelete,
		Bucket: bucket,
		ObjInfo: ObjectInfo{
			Name: object,
		},
		ReqParams: map[string]string{
			"sourceIPAddress": r.RemoteAddr,
		},
	})
}
