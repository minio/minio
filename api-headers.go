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
	"bytes"
	"crypto/rand"
	"encoding/xml"
	"net/http"
	"runtime"
	"strconv"

	"github.com/minio/minio/pkg/fs"
)

//// helpers

// Static alphaNumeric table used for generating unique request ids
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

// generateRequestID - Generate request id
func generateRequestID() []byte {
	alpha := make([]byte, 16)
	rand.Read(alpha)
	for i := 0; i < 16; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	// Set unique request ID for each reply.
	w.Header().Set("X-Amz-Request-Id", string(generateRequestID()))
	w.Header().Set("Server", ("Minio/" + minioReleaseTag + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"))
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
func setObjectHeaders(w http.ResponseWriter, metadata fs.ObjectMetadata, contentRange *httpRange) {
	// set common headers
	setCommonHeaders(w)

	// set object-related metadata headers
	lastModified := metadata.LastModified.UTC().Format(http.TimeFormat)
	w.Header().Set("Last-Modified", lastModified)

	w.Header().Set("Content-Type", metadata.ContentType)
	if metadata.MD5 != "" {
		w.Header().Set("ETag", "\""+metadata.MD5+"\"")
	}

	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))

	// for providing ranged content
	if contentRange != nil {
		if contentRange.start > 0 || contentRange.length > 0 {
			// override content-length
			w.Header().Set("Content-Length", strconv.FormatInt(contentRange.length, 10))
			w.Header().Set("Content-Range", contentRange.String())
			w.WriteHeader(http.StatusPartialContent)
		}
	}
}
