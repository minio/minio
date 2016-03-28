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

package main

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	fastSha256 "github.com/minio/minio/pkg/crypto/sha256"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/probe"
)

// supportedGetReqParams - supported request parameters for GET presigned request.
var supportedGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
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

		if s3Error := enforceBucketPolicy("s3:ListBucket", bucket, &url); s3Error != ErrNone {
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

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:GetObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}
	// Fetch object stat info.
	objInfo, err := api.ObjectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, errAllowableObjectNotFound(bucket, r), r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		default:
			errorIf(err.Trace(), "GetObjectInfo failed.", nil)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Verify 'If-Modified-Since' and 'If-Unmodified-Since'.
	lastModified := objInfo.ModTime
	if checkLastModified(w, r, lastModified) {
		return
	}
	// Verify 'If-Match' and 'If-None-Match'.
	if checkETag(w, r) {
		return
	}

	var hrange *httpRange
	hrange, err = getRequestedRange(r.Header.Get("Range"), objInfo.Size)
	if err != nil {
		writeErrorResponse(w, r, ErrInvalidRange, r.URL.Path)
		return
	}

	// Get the object.
	startOffset := hrange.start
	readCloser, err := api.ObjectAPI.GetObject(bucket, object, startOffset)
	if err != nil {
		switch err.ToGoError().(type) {
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, errAllowableObjectNotFound(bucket, r), r.URL.Path)
		default:
			errorIf(err.Trace(), "GetObject failed.", nil)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	defer readCloser.Close() // Close after this handler returns.

	// Set standard object headers.
	setObjectHeaders(w, objInfo, hrange)

	// Set any additional requested response headers.
	setGetRespHeaders(w, r.URL.Query())

	if hrange.length > 0 {
		if _, e := io.CopyN(w, readCloser, hrange.length); e != nil {
			errorIf(probe.NewError(e), "Writing to client failed", nil)
			// Do not send error response here, since client could have died.
			return
		}
	} else {
		if _, e := io.Copy(w, readCloser); e != nil {
			errorIf(probe.NewError(e), "Writing to client failed", nil)
			// Do not send error response here, since client could have died.
			return
		}
	}
}

var unixEpochTime = time.Unix(0, 0)

// checkLastModified implements If-Modified-Since and
// If-Unmodified-Since checks.
//
// modtime is the modification time of the resource to be served, or
// IsZero(). return value is whether this request is now complete.
func checkLastModified(w http.ResponseWriter, r *http.Request, modtime time.Time) bool {
	if modtime.IsZero() || modtime.Equal(unixEpochTime) {
		// If the object doesn't have a modtime (IsZero), or the modtime
		// is obviously garbage (Unix time == 0), then ignore modtimes
		// and don't process the If-Modified-Since header.
		return false
	}

	// The Date-Modified header truncates sub-second precision, so
	// use mtime < t+1s instead of mtime <= t to check for unmodified.
	if _, ok := r.Header["If-Modified-Since"]; ok {
		// Return the object only if it has been modified since the
		// specified time, otherwise return a 304 (not modified).
		t, err := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since"))
		if err == nil && modtime.Before(t.Add(1*time.Second)) {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if _, ok := r.Header["If-Unmodified-Since"]; ok {
		// Return the object only if it has not been modified since
		// the specified time, otherwise return a 412 (precondition failed).
		t, err := time.Parse(http.TimeFormat, r.Header.Get("If-Unmodified-Since"))
		if err == nil && modtime.After(t.Add(1*time.Second)) {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}
	w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	return false
}

// checkETag implements If-None-Match and If-Match checks.
//
// The ETag must have been previously set in the ResponseWriter's
// headers. The return value is whether this request is now considered
// done.
func checkETag(w http.ResponseWriter, r *http.Request) bool {
	etag := w.Header().Get("ETag")
	// Must know ETag.
	if etag == "" {
		return false
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		// Return the object only if its entity tag (ETag) is
		// different from the one specified; otherwise, return a 304
		// (not modified).
		if r.Method != "GET" && r.Method != "HEAD" {
			return false
		}
		if inm == etag || inm == "*" {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if im := r.Header.Get("If-Match"); im != "" {
		// Return the object only if its entity tag (ETag) is the same
		// as the one specified; otherwise, return a 412 (precondition failed).
		if r.Method != "GET" && r.Method != "HEAD" {
			return false
		}
		if im != etag {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}
	return false
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api objectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:GetObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	objInfo, err := api.ObjectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err.Trace(bucket, object), "GetObjectInfo failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, errAllowableObjectNotFound(bucket, r), r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Verify 'If-Modified-Since' and 'If-Unmodified-Since'.
	lastModified := objInfo.ModTime
	if checkLastModified(w, r, lastModified) {
		return
	}

	// Verify 'If-Match' and 'If-None-Match'.
	if checkETag(w, r) {
		return
	}

	// Set standard object headers.
	setObjectHeaders(w, objInfo, nil)

	// Successfull response.
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

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// TODO: Reject requests where body/payload is present, for now we
	// don't even read it.

	// objectSource
	objectSource := r.Header.Get("X-Amz-Copy-Source")

	// Skip the first element if it is '/', split the rest.
	if strings.HasPrefix(objectSource, "/") {
		objectSource = objectSource[1:]
	}
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

	objInfo, err := api.ObjectAPI.GetObjectInfo(sourceBucket, sourceObject)
	if err != nil {
		errorIf(err.Trace(), "GetObjectInfo failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, objectSource)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, objectSource)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, objectSource)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, objectSource)
		default:
			writeErrorResponse(w, r, ErrInternalError, objectSource)
		}
		return
	}
	// Verify before writing.

	// Verify x-amz-copy-source-if-modified-since and
	// x-amz-copy-source-if-unmodified-since.
	lastModified := objInfo.ModTime
	if checkCopySourceLastModified(w, r, lastModified) {
		return
	}

	// Verify x-amz-copy-source-if-match and
	// x-amz-copy-source-if-none-match.
	if checkCopySourceETag(w, r) {
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(objInfo.Size) {
		writeErrorResponse(w, r, ErrEntityTooLarge, objectSource)
		return
	}

	var md5Bytes []byte
	if objInfo.MD5Sum != "" {
		var e error
		md5Bytes, e = hex.DecodeString(objInfo.MD5Sum)
		if e != nil {
			errorIf(probe.NewError(e), "Decoding md5 failed.", nil)
			writeErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
			return
		}
	}

	startOffset := int64(0) // Read the whole file.
	// Get the object.
	readCloser, getErr := api.ObjectAPI.GetObject(sourceBucket, sourceObject, startOffset)
	if getErr != nil {
		errorIf(getErr.Trace(sourceBucket, sourceObject), "Reading "+objectSource+" failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, objectSource)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, objectSource)
		default:
			writeErrorResponse(w, r, ErrInternalError, objectSource)
		}
		return
	}
	// Size of object.
	size := objInfo.Size

	// Save metadata.
	metadata := make(map[string]string)
	metadata["md5Sum"] = hex.EncodeToString(md5Bytes)

	// Create the object.
	md5Sum, err := api.ObjectAPI.PutObject(bucket, object, size, readCloser, metadata)
	if err != nil {
		switch err.ToGoError().(type) {
		case RootPathFull:
			writeErrorResponse(w, r, ErrRootPathFull, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BadDigest:
			writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
		case IncompleteBody:
			writeErrorResponse(w, r, ErrIncompleteBody, r.URL.Path)
		case ObjectExistsAsPrefix:
			writeErrorResponse(w, r, ErrObjectExistsAsPrefix, r.URL.Path)
		default:
			errorIf(err.Trace(), "PutObject failed.", nil)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	objInfo, err = api.ObjectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "GetObjectInfo failed.", nil)
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}

	response := generateCopyObjectResponse(md5Sum, objInfo.ModTime)
	encodedSuccessResponse := encodeResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
	// Explicitly close the reader, to avoid fd leaks.
	readCloser.Close()
}

// checkCopySource implements x-amz-copy-source-if-modified-since and
// x-amz-copy-source-if-unmodified-since checks.
//
// modtime is the modification time of the resource to be served, or
// IsZero(). return value is whether this request is now complete.
func checkCopySourceLastModified(w http.ResponseWriter, r *http.Request, modtime time.Time) bool {
	if modtime.IsZero() || modtime.Equal(unixEpochTime) {
		// If the object doesn't have a modtime (IsZero), or the modtime
		// is obviously garbage (Unix time == 0), then ignore modtimes
		// and don't process the If-Modified-Since header.
		return false
	}
	// The Date-Modified header truncates sub-second precision, so
	// use mtime < t+1s instead of mtime <= t to check for unmodified.
	if _, ok := r.Header["x-amz-copy-source-if-modified-since"]; ok {
		// Return the object only if it has been modified since the
		// specified time, otherwise return a 304 error (not modified).
		t, err := time.Parse(http.TimeFormat, r.Header.Get("x-amz-copy-source-if-modified-since"))
		if err == nil && modtime.Before(t.Add(1*time.Second)) {
			h := w.Header()
			// Remove Content headers if set
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if _, ok := r.Header["x-amz-copy-source-if-unmodified-since"]; ok {
		// Return the object only if it has not been modified since the
		// specified time, otherwise return a 412 error (precondition failed).
		t, err := time.Parse(http.TimeFormat, r.Header.Get("x-amz-copy-source-if-unmodified-since"))
		if err == nil && modtime.After(t.Add(1*time.Second)) {
			h := w.Header()
			// Remove Content headers if set
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}
	w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	return false
}

// checkCopySourceETag implements x-amz-copy-source-if-match and
// x-amz-copy-source-if-none-match checks.
//
// The ETag must have been previously set in the ResponseWriter's
// headers. The return value is whether this request is now considered
// complete.
func checkCopySourceETag(w http.ResponseWriter, r *http.Request) bool {
	etag := w.Header().Get("ETag")
	// Tag must be provided...
	if etag == "" {
		return false
	}
	if inm := r.Header.Get("x-amz-copy-source-if-none-match"); inm != "" {
		// Return the object only if its entity tag (ETag) is different
		// from the one specified; otherwise, return a 304 (not modified).
		if r.Method != "PUT" {
			return false
		}
		if inm == etag || inm == "*" {
			h := w.Header()
			// Remove Content headers if set
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if inm := r.Header.Get("x-amz-copy-source-if-match"); inm != "" {
		// Return the object only if its entity tag (ETag) is the same
		// as the one specified; otherwise, return a 412 (precondition failed).
		if r.Method != "PUT" {
			return false
		}
		if inm != etag {
			h := w.Header()
			// Remove Content headers if set
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}
	return false

}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api objectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	// If the matching failed, it means that the X-Amz-Copy-Source was
	// wrong, fail right here.
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
		errorIf(err.Trace(r.Header.Get("Content-Md5")), "Decoding md5 failed.", nil)
		writeErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
		return
	}
	/// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if size == -1 && !contains(r.TransferEncoding, "chunked") {
		writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}
	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
		return
	}

	var md5Sum string
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// Create anonymous object.
		md5Sum, err = api.ObjectAPI.PutObject(bucket, object, size, r.Body, nil)
	case authTypePresigned, authTypeSigned:
		// Initialize a pipe for data pipe line.
		reader, writer := io.Pipe()

		// Start writing in a routine.
		go func() {
			shaWriter := fastSha256.New()
			multiWriter := io.MultiWriter(shaWriter, writer)
			if _, e := io.CopyN(multiWriter, r.Body, size); e != nil {
				errorIf(probe.NewError(e), "Unable to read HTTP body.", nil)
				writer.CloseWithError(e)
				return
			}
			shaPayload := shaWriter.Sum(nil)
			validateRegion := true // Validate region.
			var s3Error APIErrorCode
			if isRequestSignatureV4(r) {
				s3Error = doesSignatureMatch(hex.EncodeToString(shaPayload), r, validateRegion)
			} else if isRequestPresignedSignatureV4(r) {
				s3Error = doesPresignedSignatureMatch(hex.EncodeToString(shaPayload), r, validateRegion)
			}
			if s3Error != ErrNone {
				if s3Error == ErrSignatureDoesNotMatch {
					writer.CloseWithError(errSignatureMismatch)
					return
				}
				writer.CloseWithError(fmt.Errorf("%v", getAPIError(s3Error)))
				return
			}
			// Close the writer.
			writer.Close()
		}()

		// Save metadata.
		metadata := make(map[string]string)
		// Make sure we hex encode here.
		metadata["md5"] = hex.EncodeToString(md5Bytes)
		// Create object.
		md5Sum, err = api.ObjectAPI.PutObject(bucket, object, size, reader, metadata)
	}
	if err != nil {
		errorIf(err.Trace(), "PutObject failed.", nil)
		e := err.ToGoError()
		// Verify if the underlying error is signature mismatch.
		if e == errSignatureMismatch {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
		switch e.(type) {
		case RootPathFull:
			writeErrorResponse(w, r, ErrRootPathFull, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BadDigest:
			writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
		case IncompleteBody:
			writeErrorResponse(w, r, ErrIncompleteBody, r.URL.Path)
		case ObjectExistsAsPrefix:
			writeErrorResponse(w, r, ErrObjectExistsAsPrefix, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	if md5Sum != "" {
		w.Header().Set("ETag", "\""+md5Sum+"\"")
	}
	writeSuccessResponse(w, nil)
}

/// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload
func (api objectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	uploadID, err := api.ObjectAPI.NewMultipartUpload(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "NewMultipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case RootPathFull:
			writeErrorResponse(w, r, ErrRootPathFull, r.URL.Path)
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
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

	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		errorIf(err.Trace(r.Header.Get("Content-Md5")), "Decoding md5 failed.", nil)
		writeErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength
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

	partID, e := strconv.Atoi(partIDString)
	if e != nil {
		writeErrorResponse(w, r, ErrInvalidPart, r.URL.Path)
		return
	}

	var partMD5 string
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// No need to verify signature, anonymous request access is
		// already allowed.
		partMD5, err = api.ObjectAPI.PutObjectPart(bucket, object, uploadID, partID, size, r.Body, hex.EncodeToString(md5Bytes))
	case authTypePresigned, authTypeSigned:
		validateRegion := true // Validate region.
		// Initialize a pipe for data pipe line.
		reader, writer := io.Pipe()

		// Start writing in a routine.
		go func() {
			shaWriter := fastSha256.New()
			multiWriter := io.MultiWriter(shaWriter, writer)
			if _, e := io.CopyN(multiWriter, r.Body, size); e != nil {
				errorIf(probe.NewError(e), "Unable to read HTTP body.", nil)
				writer.CloseWithError(e)
				return
			}
			shaPayload := shaWriter.Sum(nil)
			var s3Error APIErrorCode
			if isRequestSignatureV4(r) {
				s3Error = doesSignatureMatch(hex.EncodeToString(shaPayload), r, validateRegion)
			} else if isRequestPresignedSignatureV4(r) {
				s3Error = doesPresignedSignatureMatch(hex.EncodeToString(shaPayload), r, validateRegion)
			}
			if s3Error != ErrNone {
				if s3Error == ErrSignatureDoesNotMatch {
					writer.CloseWithError(errSignatureMismatch)
					return
				}
				writer.CloseWithError(fmt.Errorf("%v", getAPIError(s3Error)))
				return
			}
			// Close the writer.
			writer.Close()
		}()
		partMD5, err = api.ObjectAPI.PutObjectPart(bucket, object, uploadID, partID, size, reader, hex.EncodeToString(md5Bytes))
	}
	if err != nil {
		errorIf(err.Trace(), "PutObjectPart failed.", nil)
		e := err.ToGoError()
		// Verify if the underlying error is signature mismatch.
		if e == errSignatureMismatch {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
		switch e.(type) {
		case RootPathFull:
			writeErrorResponse(w, r, ErrRootPathFull, r.URL.Path)
		case InvalidUploadID:
			writeErrorResponse(w, r, ErrNoSuchUpload, r.URL.Path)
		case BadDigest:
			writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
		case IncompleteBody:
			writeErrorResponse(w, r, ErrIncompleteBody, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
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

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:AbortMultipartUpload", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	err := api.ObjectAPI.AbortMultipartUpload(bucket, object, uploadID)
	if err != nil {
		errorIf(err.Trace(), "AbortMutlipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case InvalidUploadID:
			writeErrorResponse(w, r, ErrNoSuchUpload, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api objectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:ListMultipartUploadParts", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
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
	if maxParts == 0 {
		maxParts = maxPartsList
	}

	listPartsInfo, err := api.ObjectAPI.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		errorIf(err.Trace(), "ListObjectParts failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case InvalidUploadID:
			writeErrorResponse(w, r, ErrNoSuchUpload, r.URL.Path)
		case InvalidPart:
			writeErrorResponse(w, r, ErrInvalidPart, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	response := generateListPartsResponse(listPartsInfo)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers.
	setCommonHeaders(w)
	// Write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api objectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	var md5Sum string
	var err *probe.Error
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}
	completeMultipartBytes, e := ioutil.ReadAll(r.Body)
	if e != nil {
		errorIf(probe.NewError(e), "CompleteMultipartUpload failed.", nil)
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	complMultipartUpload := &completeMultipartUpload{}
	if e = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); e != nil {
		errorIf(probe.NewError(e), "XML Unmarshal failed", nil)
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
	// Complete multipart upload.
	md5Sum, err = api.ObjectAPI.CompleteMultipartUpload(bucket, object, uploadID, completeParts)
	if err != nil {
		errorIf(err.Trace(), "CompleteMultipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case InvalidUploadID:
			writeErrorResponse(w, r, ErrNoSuchUpload, r.URL.Path)
		case InvalidPart:
			writeErrorResponse(w, r, ErrInvalidPart, r.URL.Path)
		case IncompleteBody:
			writeErrorResponse(w, r, ErrIncompleteBody, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	// Get object location.
	location := getLocation(r)
	// Generate complete multipart response.
	response := generateCompleteMultpartUploadResponse(bucket, object, location, md5Sum)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers.
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api objectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:DeleteObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}
	err := api.ObjectAPI.DeleteObject(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "DeleteObject failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case ObjectNotFound:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		case ObjectNameInvalid:
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}
