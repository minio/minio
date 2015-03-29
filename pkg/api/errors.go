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
	"encoding/xml"
	"net/http"
)

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

// Error codes, non exhaustive list
const (
	AccessDenied = iota
	BadDigest
	BucketAlreadyExists
	EntityTooSmall
	EntityTooLarge
	IncompleteBody
	InternalError
	InvalidAccessKeyID
	InvalidBucketName
	InvalidDigest
	InvalidRange
	MalformedXML
	MissingContentLength
	MissingRequestBodyError
	NoSuchBucket
	NoSuchKey
	NoSuchUpload
	NotImplemented
	RequestTimeTooSkewed
	SignatureDoesNotMatch
	TooManyBuckets
	InvalidPolicyDocument
	NoSuchBucketPolicy
)

// Error code to Error structure map
var errorCodeResponse = map[int]Error{
	AccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied",
		HTTPStatusCode: http.StatusForbidden,
	},
	BadDigest: {
		Code:           "BadDigest",
		Description:    "The Content-MD5 you specified did not match what we received.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	BucketAlreadyExists: {
		Code:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available.",
		HTTPStatusCode: http.StatusConflict,
	},
	EntityTooSmall: {
		Code:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	EntityTooLarge: {
		Code:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	IncompleteBody: {
		Code:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	},
	InvalidAccessKeyID: {
		Code:           "InvalidAccessKeyID",
		Description:    "The access key ID you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
	},
	InvalidBucketName: {
		Code:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-MD5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidRange: {
		Code:           "InvalidRange",
		Description:    "The requested range cannot be satisfied.",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
	},
	MalformedXML: {
		Code:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	MissingContentLength: {
		Code:           "MissingContentLength",
		Description:    "You must provide the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusLengthRequired,
	},
	MissingRequestBodyError: {
		Code:           "MissingRequestBodyError",
		Description:    "Request body is empty.",
		HTTPStatusCode: http.StatusLengthRequired,
	},
	NoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	NoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	NoSuchUpload: {
		Code:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	NotImplemented: {
		Code:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented.",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	RequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HTTPStatusCode: http.StatusForbidden,
	},
	SignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided.",
		HTTPStatusCode: http.StatusForbidden,
	},
	TooManyBuckets: {
		Code:           "TooManyBuckets",
		Description:    "You have attempted to create more buckets than allowed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidPolicyDocument: {
		Code:           "InvalidPolicyDocument",
		Description:    "The content of the form does not meet the conditions specified in the policy document.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	NoSuchBucketPolicy: {
		Code:           "NoSuchBucketPolicy",
		Description:    "The specified bucket does not have a bucket policy.",
		HTTPStatusCode: http.StatusNotFound,
	},
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
