// Copyright (c) 2015-2021 MinIO, Inc.
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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
)

// Returns a hexadecimal representation of time at the
// time response is sent to the client.
func mustGetRequestID(t time.Time) string {
	return fmt.Sprintf("%X", t.UnixNano())
}

// setEventStreamHeaders to allow proxies to avoid buffering proxy responses
func setEventStreamHeaders(w http.ResponseWriter) {
	w.Header().Set(xhttp.ContentType, "text/event-stream")
	w.Header().Set(xhttp.CacheControl, "no-cache") // nginx to turn off buffering
	w.Header().Set("X-Accel-Buffering", "no")      // nginx to turn off buffering
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	// Set the "Server" http header.
	w.Header().Set(xhttp.ServerInfo, "MinIO")

	// Set `x-amz-bucket-region` only if region is set on the server
	// by default minio uses an empty region.
	if region := globalServerRegion; region != "" {
		w.Header().Set(xhttp.AmzBucketRegion, region)
	}
	w.Header().Set(xhttp.AcceptRanges, "bytes")

	// Remove sensitive information
	crypto.RemoveSensitiveHeaders(w.Header())
}

// Encodes the response headers into XML format.
func encodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Encodes the response headers into JSON format.
func encodeResponseJSON(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	e := json.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Write parts count
func setPartsCountHeaders(w http.ResponseWriter, objInfo ObjectInfo) {
	if strings.Contains(objInfo.ETag, "-") && len(objInfo.Parts) > 0 {
		w.Header()[xhttp.AmzMpPartsCount] = []string{strconv.Itoa(len(objInfo.Parts))}
	}
}

// Write object header
func setObjectHeaders(w http.ResponseWriter, objInfo ObjectInfo, rs *HTTPRangeSpec, opts ObjectOptions) (err error) {
	// set common headers
	setCommonHeaders(w)

	// Set last modified time.
	lastModified := objInfo.ModTime.UTC().Format(http.TimeFormat)
	w.Header().Set(xhttp.LastModified, lastModified)

	// Set Etag if available.
	if objInfo.ETag != "" {
		w.Header()[xhttp.ETag] = []string{"\"" + objInfo.ETag + "\""}
	}

	if objInfo.ContentType != "" {
		w.Header().Set(xhttp.ContentType, objInfo.ContentType)
	}

	if objInfo.ContentEncoding != "" {
		w.Header().Set(xhttp.ContentEncoding, objInfo.ContentEncoding)
	}

	if !objInfo.Expires.IsZero() {
		w.Header().Set(xhttp.Expires, objInfo.Expires.UTC().Format(http.TimeFormat))
	}

	if globalCacheConfig.Enabled {
		w.Header().Set(xhttp.XCache, objInfo.CacheStatus.String())
		w.Header().Set(xhttp.XCacheLookup, objInfo.CacheLookupStatus.String())
	}

	// Set tag count if object has tags
	if len(objInfo.UserTags) > 0 {
		tags, _ := url.ParseQuery(objInfo.UserTags)
		if len(tags) > 0 {
			w.Header()[xhttp.AmzTagCount] = []string{strconv.Itoa(len(tags))}
		}
	}

	// Set all other user defined metadata.
	for k, v := range objInfo.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			// Do not need to send any internal metadata
			// values to client.
			continue
		}

		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}

		var isSet bool
		for _, userMetadataPrefix := range userMetadataKeyPrefixes {
			if !strings.HasPrefix(strings.ToLower(k), strings.ToLower(userMetadataPrefix)) {
				continue
			}
			w.Header()[strings.ToLower(k)] = []string{v}
			isSet = true
			break
		}

		if !isSet {
			w.Header().Set(k, v)
		}
	}

	var start, rangeLen int64
	totalObjectSize, err := objInfo.GetActualSize()
	if err != nil {
		return err
	}

	if rs == nil && opts.PartNumber > 0 {
		rs = partNumberToRangeSpec(objInfo, opts.PartNumber)
	}

	// For providing ranged content
	start, rangeLen, err = rs.GetOffsetLength(totalObjectSize)
	if err != nil {
		return err
	}

	// Set content length.
	w.Header().Set(xhttp.ContentLength, strconv.FormatInt(rangeLen, 10))
	if rs != nil {
		contentRange := fmt.Sprintf("bytes %d-%d/%d", start, start+rangeLen-1, totalObjectSize)
		w.Header().Set(xhttp.ContentRange, contentRange)
	}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	if objInfo.ReplicationStatus.String() != "" {
		w.Header()[xhttp.AmzBucketReplicationStatus] = []string{objInfo.ReplicationStatus.String()}
	}

	if lc, err := globalLifecycleSys.Get(objInfo.Bucket); err == nil {
		if opts.VersionID == "" {
			if ruleID, expiryTime := lc.PredictExpiryTime(lifecycle.ObjectOpts{
				Name:             objInfo.Name,
				UserTags:         objInfo.UserTags,
				VersionID:        objInfo.VersionID,
				ModTime:          objInfo.ModTime,
				IsLatest:         objInfo.IsLatest,
				DeleteMarker:     objInfo.DeleteMarker,
				SuccessorModTime: objInfo.SuccessorModTime,
			}); !expiryTime.IsZero() {
				w.Header()[xhttp.AmzExpiration] = []string{
					fmt.Sprintf(`expiry-date="%s", rule-id="%s"`, expiryTime.Format(http.TimeFormat), ruleID),
				}
			}
		}
		if objInfo.IsRemote() {
			// Check if object is being restored. For more information on x-amz-restore header see
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
			w.Header()[xhttp.AmzStorageClass] = []string{objInfo.TransitionTier}
		}
		ruleID, transitionTime := lc.PredictTransitionTime(lifecycle.ObjectOpts{
			Name:             objInfo.Name,
			UserTags:         objInfo.UserTags,
			VersionID:        objInfo.VersionID,
			ModTime:          objInfo.ModTime,
			IsLatest:         objInfo.IsLatest,
			DeleteMarker:     objInfo.DeleteMarker,
			TransitionStatus: objInfo.TransitionStatus,
		})
		if !transitionTime.IsZero() {
			// This header is a MinIO centric extension to show expected transition date in a similar spirit as x-amz-expiration
			w.Header()[xhttp.MinIOTransition] = []string{
				fmt.Sprintf(`transition-date="%s", rule-id="%s"`, transitionTime.Format(http.TimeFormat), ruleID),
			}
		}

	}

	return nil
}
