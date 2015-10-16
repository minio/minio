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

// generateRequestID generate request id
func generateRequestID() []byte {
	alpha := make([]byte, 16)
	rand.Read(alpha)
	for i := 0; i < 16; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter, contentLength int) {
	// set unique request ID for each reply
	w.Header().Set("X-Amz-Request-Id", string(generateRequestID()))
	w.Header().Set("Server", ("Minio/" + minioReleaseTag + " (" + runtime.GOOS + ";" + runtime.GOARCH + ")"))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Connection", "close")
	// should be set to '0' by default
	w.Header().Set("Content-Length", strconv.Itoa(contentLength))
}

// Write error response headers
func encodeErrorResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	// write common headers
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Write object header
func setObjectHeaders(w http.ResponseWriter, metadata fs.ObjectMetadata, contentRange *httpRange) {
	// set common headers
	if contentRange != nil {
		if contentRange.length > 0 {
			setCommonHeaders(w, int(contentRange.length))
		} else {
			setCommonHeaders(w, int(metadata.Size))
		}
	} else {
		setCommonHeaders(w, int(metadata.Size))
	}
	// set object headers
	lastModified := metadata.Created.Format(http.TimeFormat)
	// object related headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", "\""+metadata.Md5+"\"")
	w.Header().Set("Last-Modified", lastModified)

	// set content range
	if contentRange != nil {
		if contentRange.start > 0 || contentRange.length > 0 {
			w.Header().Set("Content-Range", contentRange.String())
			w.WriteHeader(http.StatusPartialContent)
		}
	}
}

func encodeSuccessResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}
