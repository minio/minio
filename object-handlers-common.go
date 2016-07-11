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

package main

import (
	"net/http"
	"strings"
	"time"
)

// Validates the preconditions. Returns true if object content need not be written to http.ResponseWriter.
func checkPreconditions(r *http.Request, w http.ResponseWriter, objInfo ObjectInfo) bool {
	// Return false for methods other than GET and HEAD.
	if r.Method != "GET" && r.Method != "HEAD" {
		return false
	}

	// Headers to be set of object content is not going to be written to the client.
	writeHeaders := func() {
		// set common headers
		setCommonHeaders(w)

		// set object-related metadata headers
		w.Header().Set("Last-Modified", objInfo.ModTime.UTC().Format(http.TimeFormat))

		if objInfo.MD5Sum != "" {
			w.Header().Set("ETag", "\""+objInfo.MD5Sum+"\"")
		}
	}
	// If-Modified-Since : Return the object only if it has been modified since the specified time,
	// otherwise return a 304 (not modified).
	ifModifiedSinceHeader := r.Header.Get("If-Modified-Since")
	if ifModifiedSinceHeader != "" {
		if !ifModifiedSince(objInfo.ModTime, ifModifiedSinceHeader) {
			// If the object is not modified since the specified time.
			writeHeaders()
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	// If-Unmodified-Since : Return the object only if it has not been modified since the specified
	// time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := r.Header.Get("If-Unmodified-Since")
	if ifUnmodifiedSinceHeader != "" {
		if ifModifiedSince(objInfo.ModTime, ifUnmodifiedSinceHeader) {
			// If the object is modified since the specified time.
			writeHeaders()
			writeErrorResponse(w, r, ErrPreconditionFailed, r.URL.Path)
			return true
		}
	}

	// If-Match : Return the object only if its entity tag (ETag) is the same as the one specified;
	// otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get("If-Match")
	if ifMatchETagHeader != "" {
		if !isETagEqual(objInfo.MD5Sum, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(w, r, ErrPreconditionFailed, r.URL.Path)
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get("If-None-Match")
	if ifNoneMatchETagHeader != "" {
		if isETagEqual(objInfo.MD5Sum, ifNoneMatchETagHeader) {
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
func ifModifiedSince(objTime time.Time, givenTimeStr string) bool {
	if objTime.IsZero() || objTime.Equal(time.Unix(0, 0)) {
		// If the object doesn't have a modtime (IsZero), or the modtime
		// is obviously garbage (Unix time == 0), then ignore modtimes
		// and don't process the If-Modified-Since header.
		return false
	}
	givenTime, err := time.Parse(http.TimeFormat, givenTimeStr)
	if err != nil {
		return true
	}
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

// checkCopySource implements x-amz-copy-source-if-modified-since and
// x-amz-copy-source-if-unmodified-since checks.
//
// modtime is the modification time of the resource to be served, or
// IsZero(). return value is whether this request is now complete.
func checkCopySourceLastModified(w http.ResponseWriter, r *http.Request, modtime time.Time) bool {
	// writer always has quoted string
	// transform reader's etag to
	if r.Method != "PUT" {
		return false
	}
	if modtime.IsZero() || modtime.Equal(time.Unix(0, 0)) {
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
			writeErrorResponse(w, r, ErrPreconditionFailed, r.URL.Path)
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
	// writer always has quoted string
	// transform reader's etag to
	if r.Method != "PUT" {
		return false
	}
	etag := w.Header().Get("ETag")
	// Tag must be provided...
	if etag == "" {
		return false
	}
	if inm := r.Header.Get("x-amz-copy-source-if-none-match"); inm != "" {
		// Return the object only if its entity tag (ETag) is
		// different from the one specified; otherwise, the
		// request returns a 412 HTTP status code error (failed precondition).
		if isETagEqual(inm, etag) || isETagEqual(inm, "*") {
			h := w.Header()
			// Remove Content headers if set
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			writeErrorResponse(w, r, ErrPreconditionFailed, r.URL.Path)
			return true
		}
	} else if inm := r.Header.Get("x-amz-copy-source-if-match"); !isETagEqual(inm, "") {
		// Return the object only if its entity tag (ETag) is the same
		// as the one specified; otherwise, return a 412 (precondition failed).
		if !isETagEqual(inm, etag) {
			h := w.Header()
			// Remove Content headers if set
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			writeErrorResponse(w, r, ErrPreconditionFailed, r.URL.Path)
			return true
		}
	}
	return false

}
