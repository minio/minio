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
	"net/http"
	"strings"
)

type contentType int

const (
	xmlContentType contentType = iota
	jsonContentType
)

// Get content type requested from 'Accept' header
func getContentType(req *http.Request) contentType {
	acceptHeader := req.Header.Get("Accept")
	switch {
	case strings.HasPrefix(acceptHeader, "application/json"):
		return jsonContentType
	default:
		return xmlContentType
	}
}

// Content type to human readable string
func getContentTypeString(content contentType) string {
	switch content {
	case jsonContentType:
		{

			return "application/json"
		}
	default:
		{
			return "application/xml"
		}
	}
}
