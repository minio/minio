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

package cmd

import (
	"encoding/xml"
	"net/http"
)

// APIErrorResponse structure.
type APIErrorResponse struct {
	XMLName xml.Name `xml:"Error" json:"-"`

	// An error string that uniquely identifies an error condition.
	ErrCode string `xml:"Code"`

	// An error string that contains a generic description of the error condition.
	Message string

	// A bucket or object involved in the error.
	Resource string

	// ID of the request associated with the error.
	ReqID string `xml:"RequestId"`
}

// APIError interface implementing.
type APIError interface {
	// ID of the request associated with the error.
	RequestID() string
	// An error string that uniquely identifies an error condition.
	Code() string
	// An error string that contains a generic description of the error condition.
	Error() string
}

// RequestID - unique ID of the request for
// which the error occurred.
func (e APIErrorResponse) RequestID() string {
	return e.ReqID
}

// Code - error code for the s3 error.
func (e APIErrorResponse) Code() string {
	return e.ErrCode
}

// Error - error message for the s3 error.
func (e APIErrorResponse) Error() string {
	return e.Message
}

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrNone                         = ""
	ErrInvalidArgument              = "InvalidArgument"
	ErrInvalidRequest               = "InvalidRequest"
	ErrAccessDenied                 = "AccessDenied"
	ErrAuthQueryParams              = "AuthorizationQueryParametersError"
	ErrBadDigest                    = "BadDigest"
	ErrEntityTooSmall               = "EntityTooSmall"
	ErrEntityTooLarge               = "EntityTooLarge"
	ErrIncompleteBody               = "IncompleteBody"
	ErrInternalError                = "InternalError"
	ErrInvalidAccessKeyID           = "InvalidAccessKeyID"
	ErrInvalidBucketName            = "InvalidBucketName"
	ErrInvalidDigest                = "InvalidDigest"
	ErrInvalidRange                 = "InvalidRange"
	ErrInvalidMaxKeys               = ErrInvalidArgument
	ErrInvalidMaxUploads            = ErrInvalidArgument
	ErrInvalidMaxParts              = ErrInvalidArgument
	ErrInvalidPartNumberMarker      = ErrInvalidArgument
	ErrInvalidRequestBody           = ErrInvalidArgument
	ErrInvalidCopySource            = ErrInvalidArgument
	ErrInvalidCopyDest              = ErrInvalidRequest
	ErrInvalidPolicyDocument        = "InvalidPolicyDocument"
	ErrInvalidObjectState           = "InvalidObjectState"
	ErrMalformedXML                 = "MalformedXML"
	ErrMissingContentLength         = "MissingContentLength"
	ErrMissingContentMD5            = "MissingContentMD5"
	ErrMissingRequestBodyError      = "MissingRequestBodyError"
	ErrNoSuchBucket                 = "NoSuchBucket"
	ErrNoSuchBucketPolicy           = "NoSuchPolicy"
	ErrNoSuchKey                    = "NoSuchKey"
	ErrNoSuchUpload                 = "NoSuchUpload"
	ErrNotImplemented               = "NotImplemented"
	ErrPreconditionFailed           = "PreconditionFailed"
	ErrRequestTimeTooSkewed         = "RequestTimeTooSkewed"
	ErrSignatureDoesNotMatch        = "SignatureDoesNotMatch"
	ErrMethodNotAllowed             = "MethodNotAllowed"
	ErrInvalidPart                  = "InvalidPart"
	ErrInvalidPartOrder             = "InvalidPartOrder"
	ErrAuthorizationHeaderMalformed = "AuthorizationheaderMalformed"
	ErrMalformedPOSTRequest         = "MalformedPOSTRequest"
	ErrSignatureVersionNotSupported = ErrInvalidRequest
	ErrBucketNotEmpty               = "BucketNotEmpty"
	ErrAllAccessDisabled            = "AllAccessDisabled"
	ErrMalformedPolicy              = "MalformedPolicy"
	ErrMissingFields                = "MissingFields"
	ErrMissingCredTag               = ErrInvalidRequest
	ErrCredMalformed                = ErrAuthQueryParams
	ErrInvalidRegion                = "InvalidRegion"
	ErrInvalidService               = ErrAuthQueryParams
	ErrInvalidRequestVersion        = ErrAuthQueryParams
	ErrMissingSignTag               = ErrAccessDenied
	ErrMissingSignHeadersTag        = ErrInvalidArgument
	ErrPolicyAlreadyExpired         = ErrAccessDenied
	ErrMalformedDate                = "MalformedDate"
	ErrMalformedPresignedDate       = ErrAuthQueryParams
	ErrMalformedCredentialDate      = ErrAuthQueryParams
	ErrMalformedCredentialRegion    = ErrAuthQueryParams
	ErrMalformedExpires             = ErrAuthQueryParams
	ErrNegativeExpires              = ErrAuthQueryParams
	ErrAuthHeaderEmpty              = ErrInvalidArgument
	ErrExpiredPresignRequest        = ErrAccessDenied
	ErrRequestNotReadyYet           = ErrAccessDenied
	ErrUnsignedHeaders              = ErrAccessDenied
	ErrMissingDateHeader            = ErrAccessDenied
	ErrInvalidQuerySignatureAlgo    = ErrAuthQueryParams
	ErrInvalidQueryParams           = ErrAuthQueryParams
	ErrInvalidQuerySignAlgo         = ErrAuthQueryParams
	ErrBucketAlreadyOwnedByYou      = "BucketAlreadyOwnedByYou"
	// Add new error codes here.

	// Bucket notification related errors.
	ErrEventNotification             = ErrInvalidArgument
	ErrARNNotification               = ErrInvalidArgument
	ErrRegionNotification            = ErrInvalidArgument
	ErrOverlappingFilterNotification = ErrInvalidArgument
	ErrFilterNameInvalid             = ErrInvalidArgument
	ErrFilterNamePrefix              = ErrInvalidArgument
	ErrFilterNameSuffix              = ErrInvalidArgument
	ErrFilterValueInvalid            = ErrInvalidArgument
	ErrOverlappingConfigs            = ErrInvalidArgument

	// S3 extended errors.
	ErrContentSHA256Mismatch = "XAmzContentSHA256Mismatch"

	// Add new extended error codes here.

	// Minio extended errors.
	ErrReadQuorum                    = "XMinioReadQuorum"
	ErrWriteQuorum                   = "XMinioWriteQuorum"
	ErrStorageFull                   = "XMinioStorageFull"
	ErrFaultyDisk                    = "XMinioFaultyDisk"
	ErrObjectExistsAsDirectory       = "XMinioObjectExistsAsDirectory"
	ErrPolicyNesting                 = "XMinioPolicyNesting"
	ErrInvalidObjectName             = "XMinioInvalidObjectName"
	ErrServerNotInitialized          = "XMinioServerNotInitialized"
	ErrUnsupportedDelimiter          = ErrInvalidArgument
	ErrInvalidUploadIDKeyCombination = ErrInvalidArgument
	ErrInvalidMarkerKeyCombination   = ErrInvalidArgument

	// Add new extended error codes here.
	// Please open a https://github.com/minio/minio/issues before adding
	// new error codes here.
)

// error code to APIError structure, these fields carry respective http Status
// values for all the error responses.
var errCodeResponse = map[string]int{
	ErrInvalidPolicyDocument:        http.StatusBadRequest,
	ErrAccessDenied:                 http.StatusForbidden,
	ErrBadDigest:                    http.StatusBadRequest,
	ErrEntityTooSmall:               http.StatusBadRequest,
	ErrEntityTooLarge:               http.StatusBadRequest,
	ErrIncompleteBody:               http.StatusBadRequest,
	ErrInternalError:                http.StatusInternalServerError,
	ErrInvalidAccessKeyID:           http.StatusForbidden,
	ErrInvalidBucketName:            http.StatusBadRequest,
	ErrInvalidDigest:                http.StatusBadRequest,
	ErrInvalidRange:                 http.StatusRequestedRangeNotSatisfiable,
	ErrMalformedXML:                 http.StatusBadRequest,
	ErrMissingContentLength:         http.StatusLengthRequired,
	ErrMissingContentMD5:            http.StatusBadRequest,
	ErrNoSuchBucket:                 http.StatusNotFound,
	ErrNoSuchBucketPolicy:           http.StatusNotFound,
	ErrNoSuchKey:                    http.StatusNotFound,
	ErrNoSuchUpload:                 http.StatusNotFound,
	ErrNotImplemented:               http.StatusNotImplemented,
	ErrPreconditionFailed:           http.StatusPreconditionFailed,
	ErrRequestTimeTooSkewed:         http.StatusForbidden,
	ErrSignatureDoesNotMatch:        http.StatusForbidden,
	ErrMethodNotAllowed:             http.StatusMethodNotAllowed,
	ErrInvalidPart:                  http.StatusBadRequest,
	ErrInvalidPartOrder:             http.StatusBadRequest,
	ErrInvalidObjectState:           http.StatusForbidden,
	ErrAuthorizationHeaderMalformed: http.StatusBadRequest,
	ErrMalformedPOSTRequest:         http.StatusBadRequest,
	ErrBucketNotEmpty:               http.StatusConflict,
	ErrAllAccessDisabled:            http.StatusForbidden,
	ErrMalformedPolicy:              http.StatusBadRequest,
	ErrMissingFields:                http.StatusBadRequest,
	ErrInvalidRequest:               http.StatusBadRequest,
	ErrMalformedDate:                http.StatusBadRequest,
	ErrInvalidRegion:                http.StatusBadRequest,
	ErrInvalidArgument:              http.StatusBadRequest,
	ErrAuthQueryParams:              http.StatusBadRequest,
	ErrBucketAlreadyOwnedByYou:      http.StatusConflict,

	/// S3 extensions.
	ErrContentSHA256Mismatch: http.StatusBadRequest,

	/// Minio extensions.
	ErrStorageFull:             http.StatusInternalServerError,
	ErrFaultyDisk:              http.StatusInternalServerError,
	ErrObjectExistsAsDirectory: http.StatusConflict,
	ErrReadQuorum:              http.StatusServiceUnavailable,
	ErrWriteQuorum:             http.StatusServiceUnavailable,
	ErrPolicyNesting:           http.StatusConflict,
	ErrInvalidObjectName:       http.StatusBadRequest,
	ErrServerNotInitialized:    http.StatusServiceUnavailable,

	// Add new errors here.
}
