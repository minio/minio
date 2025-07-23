// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
)

var etagRegex = regexp.MustCompile("\"*?([^\"]*?)\"*?$")

// Validates the preconditions for CopyObjectPart, returns true if CopyObjectPart
// operation should not proceed. Preconditions supported are:
//
//	x-amz-copy-source-if-modified-since
//	x-amz-copy-source-if-unmodified-since
//	x-amz-copy-source-if-match
//	x-amz-copy-source-if-none-match
func checkCopyObjectPartPreconditions(ctx context.Context, w http.ResponseWriter, r *http.Request, objInfo ObjectInfo) bool {
	return checkCopyObjectPreconditions(ctx, w, r, objInfo)
}

// Validates the preconditions for CopyObject, returns true if CopyObject operation should not proceed.
// Preconditions supported are:
//
//	x-amz-copy-source-if-modified-since
//	x-amz-copy-source-if-unmodified-since
//	x-amz-copy-source-if-match
//	x-amz-copy-source-if-none-match
func checkCopyObjectPreconditions(ctx context.Context, w http.ResponseWriter, r *http.Request, objInfo ObjectInfo) bool {
	// Return false for methods other than GET and HEAD.
	if r.Method != http.MethodPut {
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
	// x-amz-copy-source-if-modified-since: Return the object only if it has been modified
	// since the specified time otherwise return 412 (precondition failed).
	ifModifiedSinceHeader := r.Header.Get(xhttp.AmzCopySourceIfModifiedSince)
	if ifModifiedSinceHeader != "" {
		if givenTime, err := amztime.ParseHeader(ifModifiedSinceHeader); err == nil {
			if !ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is not modified since the specified time.
				writeHeaders()
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
				return true
			}
		}
	}

	// x-amz-copy-source-if-unmodified-since : Return the object only if it has not been
	// modified since the specified time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := r.Header.Get(xhttp.AmzCopySourceIfUnmodifiedSince)
	if ifUnmodifiedSinceHeader != "" {
		if givenTime, err := amztime.ParseHeader(ifUnmodifiedSinceHeader); err == nil {
			if ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is modified since the specified time.
				writeHeaders()
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
				return true
			}
		}
	}

	// x-amz-copy-source-if-match : Return the object only if its entity tag (ETag) is the
	// same as the one specified; otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get(xhttp.AmzCopySourceIfMatch)
	if ifMatchETagHeader != "" {
		if !isETagEqual(objInfo.ETag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get(xhttp.AmzCopySourceIfNoneMatch)
	if ifNoneMatchETagHeader != "" {
		if isETagEqual(objInfo.ETag, ifNoneMatchETagHeader) {
			// If the object ETag matches with the specified ETag.
			writeHeaders()
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
			return true
		}
	}
	// Object content should be written to http.ResponseWriter
	return false
}

// Validates the preconditions. Returns true if PUT operation should not proceed.
// Preconditions supported are:
//
//	x-minio-source-mtime
//	x-minio-source-etag
func checkPreconditionsPUT(ctx context.Context, w http.ResponseWriter, r *http.Request, objInfo ObjectInfo, opts ObjectOptions) bool {
	// Return false for methods other than PUT.
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		return false
	}
	// If the object doesn't have a modtime (IsZero), or the modtime
	// is obviously garbage (Unix time == 0), then ignore modtimes
	// and don't process the If-Modified-Since header.
	if objInfo.ModTime.IsZero() || objInfo.ModTime.Equal(time.Unix(0, 0)) {
		return false
	}

	// If top level is a delete marker proceed to upload.
	if objInfo.DeleteMarker {
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

	// If-Match : Return the object only if its entity tag (ETag) is the same as the one specified;
	// otherwise return a 412 (precondition failed).
	ifMatchETagHeader := r.Header.Get(xhttp.IfMatch)
	if ifMatchETagHeader != "" {
		if !isETagEqual(objInfo.ETag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			writeHeaders()
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
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
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
			return true
		}
	}

	etagMatch := opts.PreserveETag != "" && isETagEqual(objInfo.ETag, opts.PreserveETag)
	vidMatch := opts.VersionID != "" && opts.VersionID == objInfo.VersionID
	if etagMatch && vidMatch {
		writeHeaders()
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
		return true
	}

	// Object content should be persisted.
	return false
}

// Headers to be set of object content is not going to be written to the client.
func writeHeadersPrecondition(w http.ResponseWriter, objInfo ObjectInfo) {
	// set common headers
	setCommonHeaders(w)

	// set object-related metadata headers
	w.Header().Set(xhttp.LastModified, objInfo.ModTime.UTC().Format(http.TimeFormat))

	if objInfo.ETag != "" {
		w.Header()[xhttp.ETag] = []string{"\"" + objInfo.ETag + "\""}
	}

	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	if !objInfo.Expires.IsZero() {
		w.Header().Set(xhttp.Expires, objInfo.Expires.UTC().Format(http.TimeFormat))
	}

	if objInfo.CacheControl != "" {
		w.Header().Set(xhttp.CacheControl, objInfo.CacheControl)
	}
}

// Validates the preconditions. Returns true if GET/HEAD operation should not proceed.
// Preconditions supported are:
//
//	If-Modified-Since
//	If-Unmodified-Since
//	If-Match
//	If-None-Match
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

	// Check if the part number is correct.
	if opts.PartNumber > 1 {
		partFound := false
		for _, pi := range objInfo.Parts {
			if pi.Number == opts.PartNumber {
				partFound = true
				break
			}
		}
		if !partFound {
			// According to S3 we don't need to set any object information here.
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPartNumber), r.URL)
			return true
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := r.Header.Get(xhttp.IfNoneMatch)
	if ifNoneMatchETagHeader != "" {
		if isETagEqual(objInfo.ETag, ifNoneMatchETagHeader) {
			// Do not care If-Modified-Since, Because:
			// 1. If If-Modified-Since condition evaluates to true.
			//  If both of the If-None-Match and If-Modified-Since headers are present in the request as follows:
			// 	If-None-Match condition evaluates to false , and;
			//  If-Modified-Since condition evaluates to true ;
			// 	Then Amazon S3 returns the 304 Not Modified response code.
			// 2. If If-Modified-Since condition evaluates to false, The following `ifModifiedSinceHeader` judgment will also return 304

			// If the object ETag matches with the specified ETag.
			writeHeadersPrecondition(w, objInfo)
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	// If-Modified-Since : Return the object only if it has been modified since the specified time,
	// otherwise return a 304 (not modified).
	ifModifiedSinceHeader := r.Header.Get(xhttp.IfModifiedSince)
	if ifModifiedSinceHeader != "" {
		if givenTime, err := amztime.ParseHeader(ifModifiedSinceHeader); err == nil {
			if !ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is not modified since the specified time.
				writeHeadersPrecondition(w, objInfo)
				w.WriteHeader(http.StatusNotModified)
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
			writeHeadersPrecondition(w, objInfo)
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
			return true
		}
	}

	// If-Unmodified-Since : Return the object only if it has not been modified since the specified
	// time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := r.Header.Get(xhttp.IfUnmodifiedSince)
	if ifUnmodifiedSinceHeader != "" && ifMatchETagHeader == "" {
		if givenTime, err := amztime.ParseHeader(ifUnmodifiedSinceHeader); err == nil {
			if ifModifiedSince(objInfo.ModTime, givenTime) {
				// If the object is modified since the specified time.
				writeHeadersPrecondition(w, objInfo)
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPreconditionFailed), r.URL)
				return true
			}
		}
	}

	// Object content should be written to http.ResponseWriter
	return false
}

// returns true if object was modified after givenTime.
func ifModifiedSince(objTime time.Time, givenTime time.Time) bool {
	// The Date-Modified header truncates sub-second precision, so
	// use mtime < t+1s instead of mtime <= t to check for unmodified.
	return !objTime.Before(givenTime.Add(1 * time.Second))
}

// canonicalizeETag returns ETag with leading and trailing double-quotes removed,
// if any present
func canonicalizeETag(etag string) string {
	return etagRegex.ReplaceAllString(etag, "$1")
}

// isETagEqual return true if the canonical representations of two ETag strings
// are equal, false otherwise
func isETagEqual(left, right string) bool {
	if strings.TrimSpace(right) == "*" {
		return true
	}
	return canonicalizeETag(left) == canonicalizeETag(right)
}

// setPutObjHeaders sets all the necessary headers returned back
// upon a success Put/Copy/CompleteMultipart/Delete requests
// to activate delete only headers set delete as true
func setPutObjHeaders(w http.ResponseWriter, objInfo ObjectInfo, del bool, h http.Header) {
	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as map entry.
	if objInfo.ETag != "" && !del {
		w.Header()[xhttp.ETag] = []string{`"` + objInfo.ETag + `"`}
	}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" && objInfo.VersionID != nullVersionID {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
		// If version is a deleted marker, set this header as well
		if objInfo.DeleteMarker && del { // only returned during delete object
			w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(objInfo.DeleteMarker)}
		}
	}

	if objInfo.Bucket != "" && objInfo.Name != "" {
		if lc, err := globalLifecycleSys.Get(objInfo.Bucket); err == nil && !del {
			lc.SetPredictionHeaders(w, objInfo.ToLifecycleOpts())
		}
	}
	cs, _ := objInfo.decryptChecksums(0, h)
	hash.AddChecksumHeader(w, cs)
}

func deleteObjectVersions(ctx context.Context, o ObjectLayer, bucket string, toDel []ObjectToDelete, lcEvent []lifecycle.Event) {
	for remaining := toDel; len(remaining) > 0; toDel = remaining {
		if len(toDel) > maxDeleteList {
			remaining = toDel[maxDeleteList:]
			toDel = toDel[:maxDeleteList]
		} else {
			remaining = nil
		}
		vc, _ := globalBucketVersioningSys.Get(bucket)
		deletedObjs, errs := o.DeleteObjects(ctx, bucket, toDel, ObjectOptions{
			PrefixEnabledFn:  vc.PrefixEnabled,
			VersionSuspended: vc.Suspended(),
		})

		for i, dobj := range deletedObjs {
			oi := ObjectInfo{
				Bucket:    bucket,
				Name:      dobj.ObjectName,
				VersionID: dobj.VersionID,
			}
			traceFn := globalLifecycleSys.trace(oi)
			tags := newLifecycleAuditEvent(lcEventSrc_Scanner, lcEvent[i]).Tags()

			// Send audit for the lifecycle delete operation
			auditLogLifecycle(
				ctx,
				oi,
				ILMExpiry, tags, traceFn)

			evArgs := eventArgs{
				EventName:  event.ObjectRemovedDelete,
				BucketName: bucket,
				Object: ObjectInfo{
					Name:      dobj.ObjectName,
					VersionID: dobj.VersionID,
				},
				UserAgent: "Internal: [ILM-Expiry]",
				Host:      globalLocalNodeName,
			}
			if errs[i] != nil {
				evArgs.RespElements = map[string]string{
					"error": fmt.Sprintf("failed to delete %s(%s), with error %v", dobj.ObjectName, dobj.VersionID, errs[i]),
				}
			}
			sendEvent(evArgs)
		}
	}
}
