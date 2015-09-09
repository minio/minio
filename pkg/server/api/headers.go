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

package api

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/minio/minio/pkg/donut"
)

// No encoder interface exists, so we create one.
type encoder interface {
	Encode(v interface{}) error
}

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
func setCommonHeaders(w http.ResponseWriter, acceptsType string, contentLength int) {
	// set unique request ID for each reply
	w.Header().Set("X-Amz-Request-Id", string(generateRequestID()))
	w.Header().Set("Server", "Minio")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", acceptsType)
	w.Header().Set("Connection", "close")
	// should be set to '0' by default
	w.Header().Set("Content-Length", strconv.Itoa(contentLength))
}

// Write error response headers
func encodeErrorResponse(response interface{}, acceptsType contentType) []byte {
	var bytesBuffer bytes.Buffer
	var e encoder
	// write common headers
	switch acceptsType {
	case xmlContentType:
		e = xml.NewEncoder(&bytesBuffer)
	case jsonContentType:
		e = json.NewEncoder(&bytesBuffer)
	// by default even if unknown Accept header received handle it by sending XML contenttype response
	default:
		e = xml.NewEncoder(&bytesBuffer)
	}
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Write object header
func setObjectHeaders(w http.ResponseWriter, metadata donut.ObjectMetadata, contentRange *httpRange) {
	// set common headers
	if contentRange != nil {
		if contentRange.length > 0 {
			setCommonHeaders(w, metadata.Metadata["contentType"], int(contentRange.length))
		} else {
			setCommonHeaders(w, metadata.Metadata["contentType"], int(metadata.Size))
		}
	} else {
		setCommonHeaders(w, metadata.Metadata["contentType"], int(metadata.Size))
	}
	// set object headers
	lastModified := metadata.Created.Format(http.TimeFormat)
	// object related headers
	w.Header().Set("ETag", "\""+metadata.MD5Sum+"\"")
	w.Header().Set("Last-Modified", lastModified)

	// set content range
	if contentRange != nil {
		if contentRange.start > 0 || contentRange.length > 0 {
			w.Header().Set("Content-Range", contentRange.String())
			w.WriteHeader(http.StatusPartialContent)
		}
	}
}

func encodeSuccessResponse(response interface{}, acceptsType contentType) []byte {
	var e encoder
	var bytesBuffer bytes.Buffer
	switch acceptsType {
	case xmlContentType:
		e = xml.NewEncoder(&bytesBuffer)
	case jsonContentType:
		e = json.NewEncoder(&bytesBuffer)
	}
	e.Encode(response)
	return bytesBuffer.Bytes()
}
