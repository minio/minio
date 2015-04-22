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

import "net/http"

type contentType int

const (
	unknownContentType contentType = iota
	xmlContentType
	jsonContentType
)

// Get content type requested from 'Accept' header
func getContentType(req *http.Request) contentType {
	acceptHeader := req.Header.Get("Accept")
	if acceptHeader != "" {
		switch {
		case acceptHeader == "application/json":
			return jsonContentType
		case acceptHeader == "application/xml":
			return xmlContentType
		default:
			return unknownContentType
		}
	}
	return xmlContentType
}

// Content type to human readable string
func getContentTypeString(content contentType) string {
	switch content {
	case jsonContentType:
		{
			return "application/json"
		}
	case xmlContentType:
		{
			return "application/xml"
		}
	default:
		{
			return "application/xml"
		}
	}
}
