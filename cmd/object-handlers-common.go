/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/handlers"
)

var (
	etagRegex = regexp.MustCompile("\"*?([^\"]*?)\"*?$")
)

// Validates the preconditions for CopyObjectPart, returns true if CopyObjectPart
// operation should not proceed. Preconditions supported are:
//  x-amz-copy-source-if-modified-since
//  x-amz-copy-source-if-unmodified-since
//  x-amz-copy-source-if-match
//  x-amz-copy-source-if-none-match
func checkCopyObjectPartPreconditions(ctx context.Context, w http.ResponseWriter, r *http.Request, objInfo ObjectInfo, encETag string) bool {
	return checkCopyObjectPreconditions(ctx, w, r, objInfo, encETag)
}

// Validates the preconditions for CopyObject, returns true if CopyObject operation should not proceed.
// Preconditions supported are:
//  x-amz-copy-source-if-modified-since
//  x-amz-copy-source-if-unmodified-since
//  x-amz-copy-source-if-match
//  x-amz-copy-source-if-none-match
func checkCopyObjectPreconditions(ctx context.Context, w http.ResponseWriter, r *http.Request, objInfo ObjectInfo, encETag string) bool {
	// Return false for methods other than GET and HEAD.
	if r.Method != http.MethodPut {
		return false
	}
	if encETag == "" {
		encETag = objInfo.ETag
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
		w.Header().Set(xhttp.LastModified, objInfo.ModTime.UTC().Format(http.TimeFormat))

		if objInfo.ETag != "" {
			w.Header()[xhttp.ETag] = []string{"\"" + objInfo.ETag + "\""}
		}
	}
	// x-amz-copy-source-if-modified-since: Return the object only if it has been modified
	// since the specified time otherwise return 412 (precondition failed).
	ifModifiedSinceHeader := r.Header.Get(xhttp.AmzCopySourceIfModifiedSince)
	if ifModifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifModifiedSinceHeader); err == nil {
			if !ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is not modified since the specified time.
				writeHeaders()
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL, guessIsBrowserReq(r))
				return true
			}
		}
	}

	// x-amz-copy-source-if-unmodified-since : Return the object only if it has not been
	// modified since the specified time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := r.Header.Get(xhttp.AmzCopySourceIfUnmodifiedSince)
	if ifUnmodifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader); err == nil {
			if ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is modified since the specified time.
				writeHeaders()
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL, guessIsBrowserReq(r))
				return true
			}
		}
	}

	shouldDecryptEtag := crypto.SSECopy.IsRequested(r.Header) && !crypto.IsMultiPart(objInfo.UserDefined)

	// x-amz-copy-source-if-match : Return the object only if its entity tag (ETag) is the
	// same as the one specified; otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get(xhttp.AmzCopySourceIfMatch)
	if ifMatchETagHeader != "" {
		etag := objInfo.ETag
		if shouldDecryptEtag {
			etag = encETag[len(encETag)-32:]
		}
		if objInfo.ETag != "" && !isETagEqual(etag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL, guessIsBrowserReq(r))
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get(xhttp.AmzCopySourceIfNoneMatch)
	if ifNoneMatchETagHeader != "" {
		etag := objInfo.ETag
		if shouldDecryptEtag {
			etag = encETag[len(encETag)-32:]
		}
		if objInfo.ETag != "" && isETagEqual(etag, ifNoneMatchETagHeader) {
			// If the object ETag matches with the specified ETag.
			writeHeaders()
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL, guessIsBrowserReq(r))
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
func checkPreconditions(ctx context.Context, w http.ResponseWriter, r *http.Request, objInfo ObjectInfo, opts ObjectOptions) bool {
	// Return false for methods other than GET and HEAD.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
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
		w.Header().Set(xhttp.LastModified, objInfo.ModTime.UTC().Format(http.TimeFormat))

		if objInfo.ETag != "" {
			w.Header()[xhttp.ETag] = []string{"\"" + objInfo.ETag + "\""}
		}
	}

	// Check if the part number is correct.
	if opts.PartNumber > 1 && opts.PartNumber > len(objInfo.Parts) {
		writeHeaders()
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return true
	}

	// If-Modified-Since : Return the object only if it has been modified since the specified time,
	// otherwise return a 304 (not modified).
	ifModifiedSinceHeader := r.Header.Get(xhttp.IfModifiedSince)
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
	ifUnmodifiedSinceHeader := r.Header.Get(xhttp.IfUnmodifiedSince)
	if ifUnmodifiedSinceHeader != "" {
		if givenTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader); err == nil {
			if ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is modified since the specified time.
				writeHeaders()
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL, guessIsBrowserReq(r))
				return true
			}
		}
	}

	// If-Match : Return the object only if its entity tag (ETag) is the same as the one specified;
	// otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get(xhttp.IfMatch)
	if ifMatchETagHeader != "" {
		if !isETagEqual(objInfo.ETag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL, guessIsBrowserReq(r))
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get(xhttp.IfNoneMatch)
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
	return objTime.After(givenTime.Add(1 * time.Second))
}

// canonicalizeETag returns ETag with leading and trailing double-quotes removed,
// if any present
func canonicalizeETag(etag string) string {
	return etagRegex.ReplaceAllString(etag, "$1")
}

// isETagEqual return true if the canonical representations of two ETag strings
// are equal, false otherwise
func isETagEqual(left, right string) bool {
	return canonicalizeETag(left) == canonicalizeETag(right)
}

// setPutObjHeaders sets all the necessary headers returned back
// upon a success Put/Copy/CompleteMultipart/Delete requests
// to activate delete only headers set delete as true
func setPutObjHeaders(w http.ResponseWriter, objInfo ObjectInfo, delete bool) {
	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as map entry.
	if objInfo.ETag != "" && !delete {
		w.Header()[xhttp.ETag] = []string{`"` + objInfo.ETag + `"`}
	}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
		// If version is a deleted marker, set this header as well
		if objInfo.DeleteMarker && delete { // only returned during delete object
			w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(objInfo.DeleteMarker)}
		}
	}

	if lc, err := globalLifecycleSys.Get(objInfo.Bucket); err == nil && !delete {
		ruleID, expiryTime := lc.PredictExpiryTime(lifecycle.ObjectOpts{
			Name:         objInfo.Name,
			UserTags:     objInfo.UserTags,
			VersionID:    objInfo.VersionID,
			ModTime:      objInfo.ModTime,
			IsLatest:     objInfo.IsLatest,
			DeleteMarker: objInfo.DeleteMarker,
		})
		if !expiryTime.IsZero() {
			w.Header()[xhttp.AmzExpiration] = []string{
				fmt.Sprintf(`expiry-date="%s", rule-id="%s"`, expiryTime.Format(http.TimeFormat), ruleID),
			}
		}
	}
}

// deleteObject is a convenient wrapper to delete an object, this
// is a common function to be called from object handlers and
// web handlers.
func deleteObject(ctx context.Context, obj ObjectLayer, cache CacheObjectLayer, bucket, object string, r *http.Request, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	deleteObject := obj.DeleteObject
	if cache != nil {
		deleteObject = cache.DeleteObject
	}
	// Proceed to delete the object.
	if objInfo, err = deleteObject(ctx, bucket, object, opts); err != nil {
		return objInfo, err
	}

	// Requesting only a delete marker which was successfully attempted.
	if objInfo.DeleteMarker {
		// Notify object deleted marker event.
		sendEvent(eventArgs{
			EventName:  event.ObjectRemovedDeleteMarkerCreated,
			BucketName: bucket,
			Object:     objInfo,
			ReqParams:  extractReqParams(r),
			UserAgent:  r.UserAgent(),
			Host:       handlers.GetSourceIP(r),
		})
	} else {
		// Notify object deleted event.
		sendEvent(eventArgs{
			EventName:  event.ObjectRemovedDelete,
			BucketName: bucket,
			Object:     objInfo,
			ReqParams:  extractReqParams(r),
			UserAgent:  r.UserAgent(),
			Host:       handlers.GetSourceIP(r),
		})
	}
	return objInfo, nil
}
