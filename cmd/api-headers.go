/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/bucket/lifecycle"
)

// Returns a hexadecimal representation of time at the
// time response is sent to the client.
func mustGetRequestID(t time.Time) string {
	return fmt.Sprintf("%X", t.UnixNano())
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set(xhttp.ServerInfo, "MinIO/"+ReleaseTag)
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
func setObjectHeaders(w http.ResponseWriter, objInfo ObjectInfo, rs *HTTPRangeSpec) (err error) {
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
	tags, _ := url.ParseQuery(objInfo.UserTags)
	tagCount := len(tags)
	if tagCount > 0 {
		w.Header()[xhttp.AmzTagCount] = []string{strconv.Itoa(tagCount)}
	}

	// Set all other user defined metadata.
	for k, v := range objInfo.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			// Do not need to send any internal metadata
			// values to client.
			continue
		}
		var isSet bool
		for _, userMetadataPrefix := range userMetadataKeyPrefixes {
			if !strings.HasPrefix(k, userMetadataPrefix) {
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

	totalObjectSize, err := objInfo.GetActualSize()
	if err != nil {
		return err
	}

	// for providing ranged content
	start, rangeLen, err := rs.GetOffsetLength(totalObjectSize)
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

	if lc, err := globalLifecycleSys.Get(objInfo.Bucket); err == nil {
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

	return nil
}
