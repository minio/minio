/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"strconv"
	"time"
)

// Returns a hexadecimal representation of time at the
// time response is sent to the client.
func mustGetRequestID(t time.Time) string {
	return fmt.Sprintf("%X", t.UnixNano())
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	// Set unique request ID for each reply.
	w.Header().Set(responseRequestIDKey, mustGetRequestID(UTCNow()))
	w.Header().Set("Server", globalServerUserAgent)
	// Set `x-amz-bucket-region` only if region is set on the server
	// by default minio uses an empty region.
	if region := globalServerConfig.GetRegion(); region != "" {
		w.Header().Set("X-Amz-Bucket-Region", region)
	}
	w.Header().Set("Accept-Ranges", "bytes")
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

// Write object header
func setObjectHeaders(w http.ResponseWriter, objInfo ObjectInfo, contentRange *httpRange) {
	// set common headers
	setCommonHeaders(w)

	// Set content length.
	w.Header().Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))

	// Set last modified time.
	lastModified := objInfo.ModTime.UTC().Format(http.TimeFormat)
	w.Header().Set("Last-Modified", lastModified)

	// Set Etag if available.
	if objInfo.ETag != "" {
		w.Header().Set("ETag", "\""+objInfo.ETag+"\"")
	}

	if objInfo.ContentType != "" {
		w.Header().Set("Content-Type", objInfo.ContentType)
	}

	if objInfo.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", objInfo.ContentEncoding)
	}

	// Set all other user defined metadata.
	for k, v := range objInfo.UserDefined {
		if hasPrefix(k, ReservedMetadataPrefix) {
			// Do not need to send any internal metadata
			// values to client.
			continue
		}
		w.Header().Set(k, v)
	}

	// for providing ranged content
	if contentRange != nil && contentRange.offsetBegin > -1 {
		// Override content-length
		w.Header().Set("Content-Length", strconv.FormatInt(contentRange.getLength(), 10))
		w.Header().Set("Content-Range", contentRange.String())
		w.WriteHeader(http.StatusPartialContent)
	}
}
