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

package quota

import (
	"bytes"
	"encoding/xml"
	"net/http"
)

// copied from api, no cyclic deps allowed

// Error structure
type Error struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// ErrorResponse - error response format
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error" json:"-"`
	Code      string
	Message   string
	Resource  string
	RequestID string
	HostID    string
}

// Quota standard errors non exhaustive list
const (
	RequestTimeTooSkewed = iota
	BandWidthQuotaExceeded
	BandWidthInsufficientToProceed
	SlowDown
	ConnectionLimitExceeded
)

// Golang http doesn't implement these
const (
	StatusTooManyRequests = 429
)

func writeErrorResponse(w http.ResponseWriter, req *http.Request, errorType int, resource string) {
	error := getErrorCode(errorType)
	errorResponse := getErrorResponse(error, resource)
	// set headers
	writeErrorHeaders(w)
	w.WriteHeader(error.HTTPStatusCode)
	// write body
	encodedErrorResponse := encodeErrorResponse(errorResponse)
	w.Write(encodedErrorResponse)
}

func writeErrorHeaders(w http.ResponseWriter) {
	w.Header().Set("Server", "Minio")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Connection", "close")
}

// Error code to Error structure map
var errorCodeResponse = map[int]Error{
	BandWidthQuotaExceeded: {
		Code:           "BandwidthQuotaExceeded",
		Description:    "Bandwidth Quota Exceeded",
		HTTPStatusCode: StatusTooManyRequests,
	},
	BandWidthInsufficientToProceed: {
		Code:           "BandwidthQuotaWillBeExceeded",
		Description:    "Bandwidth quota will be exceeded with this request",
		HTTPStatusCode: StatusTooManyRequests,
	},
	SlowDown: {
		Code:           "SlowDown",
		Description:    "Reduce your request rate.",
		HTTPStatusCode: StatusTooManyRequests,
	},
	ConnectionLimitExceeded: {
		Code:           "ConnectionLimit",
		Description:    "Connection Limit Met",
		HTTPStatusCode: StatusTooManyRequests,
	},
}

// Write error response headers
func encodeErrorResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	encoder := xml.NewEncoder(&bytesBuffer)
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}

// errorCodeError provides errorCode to Error. It returns empty if the code provided is unknown
func getErrorCode(code int) Error {
	return errorCodeResponse[code]
}

// getErrorResponse gets in standard error and resource value and
// provides a encodable populated response values
func getErrorResponse(err Error, resource string) ErrorResponse {
	var data = ErrorResponse{}
	data.Code = err.Code
	data.Message = err.Description
	if resource != "" {
		data.Resource = resource
	}
	// TODO implement this in future
	data.RequestID = "3L137"
	data.HostID = "3L137"

	return data
}
