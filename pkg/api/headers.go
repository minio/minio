/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/minio/minio/pkg/storage/donut"
)

// No encoder interface exists, so we create one.
type encoder interface {
	Encode(v interface{}) error
}

//// helpers

// Write http common headers
func setCommonHeaders(w http.ResponseWriter, acceptsType string, contentLength int) {
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
	var encoder encoder
	// write common headers
	switch acceptsType {
	case xmlContentType:
		encoder = xml.NewEncoder(&bytesBuffer)
	case jsonContentType:
		encoder = json.NewEncoder(&bytesBuffer)
	// by default even if unknown Accept header received handle it by sending XML contenttype response
	default:
		encoder = xml.NewEncoder(&bytesBuffer)
	}
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}

// Write object header
func setObjectHeaders(w http.ResponseWriter, metadata donut.ObjectMetadata) {
	lastModified := metadata.Created.Format(http.TimeFormat)
	// common headers
	setCommonHeaders(w, metadata.ContentType, int(metadata.Size))
	// object related headers
	w.Header().Set("ETag", "\""+metadata.Md5+"\"")
	w.Header().Set("Last-Modified", lastModified)
}

// Write range object header
func setRangeObjectHeaders(w http.ResponseWriter, metadata donut.ObjectMetadata, contentRange *httpRange) {
	// set common headers
	setCommonHeaders(w, metadata.ContentType, int(metadata.Size))
	// set object headers
	setObjectHeaders(w, metadata)
	// set content range
	w.Header().Set("Content-Range", contentRange.getContentRange())
}

func encodeSuccessResponse(response interface{}, acceptsType contentType) []byte {
	var encoder encoder
	var bytesBuffer bytes.Buffer
	switch acceptsType {
	case xmlContentType:
		encoder = xml.NewEncoder(&bytesBuffer)
	case jsonContentType:
		encoder = json.NewEncoder(&bytesBuffer)
	}
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}
