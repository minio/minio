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
	"encoding/xml"
	"net/http"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// APIErrorResponse - error response format
type APIErrorResponse struct {
	XMLName   xml.Name `xml:"Error" json:"-"`
	Code      string
	Message   string
	Resource  string
	RequestID string `xml:"RequestId"`
	HostID    string `xml:"HostId"`
}

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
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
	InvalidRequest
	InvalidMaxKeys
	InvalidMaxUploads
	InvalidMaxParts
	InvalidPartNumberMarker
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
	MethodNotAllowed
	InvalidPart
	InvalidPartOrder
	AuthorizationHeaderMalformed
	MalformedPOSTRequest
)

// Error codes, non exhaustive list - standard HTTP errors
const (
	NotAcceptable = iota + 31
)

// APIError code to Error structure map
var errorCodeResponse = map[int]APIError{
	InvalidMaxUploads: {
		Code:           "InvalidArgument",
		Description:    "Argument maxUploads must be an integer between 0 and 2147483647.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidMaxKeys: {
		Code:           "InvalidArgument",
		Description:    "Argument maxKeys must be an integer between 0 and 2147483647.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidMaxParts: {
		Code:           "InvalidArgument",
		Description:    "Argument maxParts must be an integer between 1 and 10000.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidPartNumberMarker: {
		Code:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	AccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied.",
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
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
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
	MethodNotAllowed: {
		Code:           "MethodNotAllowed",
		Description:    "The specified method is not allowed against this resource.",
		HTTPStatusCode: http.StatusMethodNotAllowed,
	},
	NotAcceptable: {
		Code:           "NotAcceptable",
		Description:    "The requested resource is only capable of generating content not acceptable according to the Accept headers sent in the request.",
		HTTPStatusCode: http.StatusNotAcceptable,
	},
	InvalidPart: {
		Code:           "InvalidPart",
		Description:    "One or more of the specified parts could not be found.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	InvalidPartOrder: {
		Code:           "InvalidPartOrder",
		Description:    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	AuthorizationHeaderMalformed: {
		Code:           "AuthorizationHeaderMalformed",
		Description:    "The authorization header is malformed; the region is wrong; expecting 'milkyway'.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	MalformedPOSTRequest: {
		Code:           "MalformedPOSTRequest",
		Description:    "The body of your POST request is not well-formed multipart/form-data.",
		HTTPStatusCode: http.StatusBadRequest,
	},
}

// errorCodeError provides errorCode to Error. It returns empty if the code provided is unknown
func getErrorCode(code int) APIError {
	return errorCodeResponse[code]
}

// getErrorResponse gets in standard error and resource value and
// provides a encodable populated response values
func getErrorResponse(err APIError, resource string) APIErrorResponse {
	var data = APIErrorResponse{}
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
