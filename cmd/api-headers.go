/*
 * Minio Cloud Storage, (C) 2015,2016 Minio, Inc.
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
	"crypto/rand"
	"encoding/xml"
	"net/http"
	"runtime"
	"strconv"
)

// Static alphanumeric table used for generating unique request ids
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

// newRequestID generates and returns request ID string.
func newRequestID() string {
	alpha := make([]byte, 16)
	rand.Read(alpha)
	for i := 0; i < 16; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return string(alpha)
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	// Set unique request ID for each reply.
	w.Header().Set("X-Amz-Request-Id", newRequestID())
	w.Header().Set("Server", ("Minio/" + ReleaseTag + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"))
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
	if objInfo.MD5Sum != "" {
		w.Header().Set("ETag", "\""+objInfo.MD5Sum+"\"")
	}

	// Set all other user defined metadata.
	for k, v := range objInfo.UserDefined {
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
