/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package minioapi

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strconv"
	"time"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

// No encoder interface exists, so we create one.
type encoder interface {
	Encode(v interface{}) error
}

//// helpers

// Write http common headers
func writeCommonHeaders(w http.ResponseWriter, acceptsType string) {
	w.Header().Set("Server", "Minio")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", acceptsType)
}

// Write error response headers
func writeErrorResponse(w http.ResponseWriter, response interface{}, acceptsType contentType) []byte {
	var bytesBuffer bytes.Buffer
	var encoder encoder
	// write common headers
	writeCommonHeaders(w, getContentString(acceptsType))
	switch acceptsType {
	case xmlType:
		encoder = xml.NewEncoder(&bytesBuffer)
	case jsonType:
		encoder = json.NewEncoder(&bytesBuffer)
	}
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}

// Write object header
func writeObjectHeaders(w http.ResponseWriter, metadata mstorage.ObjectMetadata) {
	lastModified := metadata.Created.Format(time.RFC1123)
	// common headers
	writeCommonHeaders(w, metadata.ContentType)
	w.Header().Set("ETag", metadata.ETag)
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.Header().Set("Connection", "close")
}

// Write range object header
func writeRangeObjectHeaders(w http.ResponseWriter, metadata mstorage.ObjectMetadata, ra string) {
	lastModified := metadata.Created.Format(time.RFC1123)
	// common headers
	writeCommonHeaders(w, metadata.ContentType)
	w.Header().Set("ETag", metadata.ETag)
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Content-Range", ra)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
}

// Write object header and response
func writeObjectHeadersAndResponse(w http.ResponseWriter, response interface{}, acceptsType contentType) []byte {
	var bytesBuffer bytes.Buffer
	var encoder encoder
	// common headers
	writeCommonHeaders(w, getContentString(acceptsType))
	switch acceptsType {
	case xmlType:
		encoder = xml.NewEncoder(&bytesBuffer)
	case jsonType:
		encoder = json.NewEncoder(&bytesBuffer)
	}

	w.Header().Set("Connection", "close")
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}
