/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"net"
	"net/http"
	"strings"
	"time"
)

// Validates the preconditions for CopyObjectPart, returns true if CopyObjectPart
// operation should not proceed. Preconditions supported are:
//  x-amz-copy-source-if-modified-since
//  x-amz-copy-source-if-unmodified-since
//  x-amz-copy-source-if-match
//  x-amz-copy-source-if-none-match
func checkCopyObjectPartPreconditions(w http.ResponseWriter, r *http.Request, objInfo ObjectInfo) bool {
	return checkCopyObjectPreconditions(w, r, objInfo)
}

// Validates the preconditions for CopyObject, returns true if CopyObject operation should not proceed.
// Preconditions supported are:
//  x-amz-copy-source-if-modified-since
//  x-amz-copy-source-if-unmodified-since
//  x-amz-copy-source-if-match
//  x-amz-copy-source-if-none-match
func checkCopyObjectPreconditions(w http.ResponseWriter, r *http.Request, objInfo ObjectInfo) bool {
	// Return false for methods other than GET and HEAD.
	if r.Method != "PUT" {
		return false
	}
	// If the object doesn't have a modtime (IsZero), or the modtime
	// is obviously garbage (Unix time == 0), then ignore modtimes
	// and don't process the If-Modified-Since header.
	if objInfo.ModTime.IsZero() || objInfo.ModTime.Equal(time.Unix(0, 0)) {
		return false
	}

	// Headers to be set of object content is not going to be written to the client.
	writeHeaders := func() {
		// set common headers
		setCommonHeaders(w)

		// set object-related metadata headers
		w.Header().Set("Last-Modified", objInfo.ModTime.UTC().Format(http.TimeFormat))

		if objInfo.ETag != "" {
			w.Header().Set("ETag", "\""+objInfo.ETag+"\"")
		}
	}
	// x-amz-copy-source-if-modified-since: Return the object only if it has been modified
	// since the specified time otherwise return 412 (precondition failed).
	ifModifiedSinceHeader := r.Header.Get("x-amz-copy-source-if-modified-since")
	if ifModifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifModifiedSinceHeader); err == nil {
			if !ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is not modified since the specified time.
				writeHeaders()
				writeErrorResponse(w, ErrPreconditionFailed, r.URL)
				return true
			}
		}
	}

	// x-amz-copy-source-if-unmodified-since : Return the object only if it has not been
	// modified since the specified time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := r.Header.Get("x-amz-copy-source-if-unmodified-since")
	if ifUnmodifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader); err == nil {
			if ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is modified since the specified time.
				writeHeaders()
				writeErrorResponse(w, ErrPreconditionFailed, r.URL)
				return true
			}
		}
	}

	// x-amz-copy-source-if-match : Return the object only if its entity tag (ETag) is the
	// same as the one specified; otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get("x-amz-copy-source-if-match")
	if ifMatchETagHeader != "" {
		if objInfo.ETag != "" && !isETagEqual(objInfo.ETag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(w, ErrPreconditionFailed, r.URL)
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get("x-amz-copy-source-if-none-match")
	if ifNoneMatchETagHeader != "" {
		if objInfo.ETag != "" && isETagEqual(objInfo.ETag, ifNoneMatchETagHeader) {
			// If the object ETag matches with the specified ETag.
			writeHeaders()
			writeErrorResponse(w, ErrPreconditionFailed, r.URL)
			return true
		}
	}
	// Object content should be written to http.ResponseWriter
	return false
}

// Validates the preconditions. Returns true if GET/HEAD operation should not proceed.
// Preconditions supported are:
//  If-Modified-Since
//  If-Unmodified-Since
//  If-Match
//  If-None-Match
func checkPreconditions(w http.ResponseWriter, r *http.Request, objInfo ObjectInfo) bool {
	// Return false for methods other than GET and HEAD.
	if r.Method != "GET" && r.Method != "HEAD" {
		return false
	}
	// If the object doesn't have a modtime (IsZero), or the modtime
	// is obviously garbage (Unix time == 0), then ignore modtimes
	// and don't process the If-Modified-Since header.
	if objInfo.ModTime.IsZero() || objInfo.ModTime.Equal(time.Unix(0, 0)) {
		return false
	}

	// Headers to be set of object content is not going to be written to the client.
	writeHeaders := func() {
		// set common headers
		setCommonHeaders(w)

		// set object-related metadata headers
		w.Header().Set("Last-Modified", objInfo.ModTime.UTC().Format(http.TimeFormat))

		if objInfo.ETag != "" {
			w.Header().Set("ETag", "\""+objInfo.ETag+"\"")
		}
	}
	// If-Modified-Since : Return the object only if it has been modified since the specified time,
	// otherwise return a 304 (not modified).
	ifModifiedSinceHeader := r.Header.Get("If-Modified-Since")
	if ifModifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifModifiedSinceHeader); err == nil {
			if !ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is not modified since the specified time.
				writeHeaders()
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}

	// If-Unmodified-Since : Return the object only if it has not been modified since the specified
	// time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := r.Header.Get("If-Unmodified-Since")
	if ifUnmodifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader); err == nil {
			if ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is modified since the specified time.
				writeHeaders()
				writeErrorResponse(w, ErrPreconditionFailed, r.URL)
				return true
			}
		}
	}

	// If-Match : Return the object only if its entity tag (ETag) is the same as the one specified;
	// otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get("If-Match")
	if ifMatchETagHeader != "" {
		if !isETagEqual(objInfo.ETag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(w, ErrPreconditionFailed, r.URL)
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get("If-None-Match")
	if ifNoneMatchETagHeader != "" {
		if isETagEqual(objInfo.ETag, ifNoneMatchETagHeader) {
			// If the object ETag matches with the specified ETag.
			writeHeaders()
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}
	// Object content should be written to http.ResponseWriter
	return false
}

// returns true if object was modified after givenTime.
func ifModifiedSince(objTime time.Time, givenTime time.Time) bool {
	// The Date-Modified header truncates sub-second precision, so
	// use mtime < t+1s instead of mtime <= t to check for unmodified.
	if objTime.After(givenTime.Add(1 * time.Second)) {
		return true
	}
	return false
}

// canonicalizeETag returns ETag with leading and trailing double-quotes removed,
// if any present
func canonicalizeETag(etag string) string {
	canonicalETag := strings.TrimPrefix(etag, "\"")
	return strings.TrimSuffix(canonicalETag, "\"")
}

// isETagEqual return true if the canonical representations of two ETag strings
// are equal, false otherwise
func isETagEqual(left, right string) bool {
	return canonicalizeETag(left) == canonicalizeETag(right)
}

// deleteObject is a convenient wrapper to delete an object, this
// is a common function to be called from object handlers and
// web handlers.
func deleteObject(obj ObjectLayer, bucket, object string, r *http.Request) (err error) {

	// Proceed to delete the object.
	if err = obj.DeleteObject(bucket, object); err != nil {
		return err
	}

	// Get host and port from Request.RemoteAddr.
	host, port, _ := net.SplitHostPort(r.RemoteAddr)

	// Notify object deleted event.
	eventNotify(eventData{
		Type:   ObjectRemovedDelete,
		Bucket: bucket,
		ObjInfo: ObjectInfo{
			Name: object,
		},
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})

	return nil
}
