package minioapi

import (
	"encoding/xml"
	"net/http"
)

type Error struct {
	Code           string
	Description    string
	HttpStatusCode int
}

type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error" json:"-"`
	Code      string
	Message   string
	Resource  string
	RequestId string
	HostId    string
}

/// Error codes, non exhaustive list
const (
	AccessDenied = iota
	BadDigest
	BucketAlreadyExists
	EntityTooSmall
	EntityTooLarge
	IncompleteBody
	InternalError
	InvalidAccessKeyId
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
)

var errorCodeResponse = map[int]Error{
	AccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied",
		HttpStatusCode: http.StatusForbidden,
	},
	BadDigest: {
		Code:           "BadDigest",
		Description:    "The Content-MD5 you specified did not match what we received.",
		HttpStatusCode: http.StatusBadRequest,
	},
	BucketAlreadyExists: {
		Code:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available.",
		HttpStatusCode: http.StatusConflict,
	},
	EntityTooSmall: {
		Code:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HttpStatusCode: http.StatusBadRequest,
	},
	EntityTooLarge: {
		Code:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HttpStatusCode: http.StatusBadRequest,
	},
	IncompleteBody: {
		Code:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header",
		HttpStatusCode: http.StatusBadRequest,
	},
	InternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HttpStatusCode: http.StatusInternalServerError,
	},
	InvalidAccessKeyId: {
		Code:           "InvalidAccessKeyId",
		Description:    "The access key Id you provided does not exist in our records.",
		HttpStatusCode: http.StatusForbidden,
	},
	InvalidBucketName: {
		Code:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HttpStatusCode: http.StatusBadRequest,
	},
	InvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-MD5 you specified is not valid.",
		HttpStatusCode: http.StatusBadRequest,
	},
	InvalidRange: {
		Code:           "InvalidRange",
		Description:    "The requested range cannot be satisfied.",
		HttpStatusCode: http.StatusRequestedRangeNotSatisfiable,
	},
	MalformedXML: {
		Code:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HttpStatusCode: http.StatusBadRequest,
	},
	MissingContentLength: {
		Code:           "MissingContentLength",
		Description:    "You must provide the Content-Length HTTP header.",
		HttpStatusCode: http.StatusLengthRequired,
	},
	MissingRequestBodyError: {
		Code:           "MissingRequestBodyError",
		Description:    "Request body is empty.",
		HttpStatusCode: http.StatusLengthRequired,
	},
	NoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist.",
		HttpStatusCode: http.StatusNotFound,
	},
	NoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HttpStatusCode: http.StatusNotFound,
	},
	NoSuchUpload: {
		Code:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist.",
		HttpStatusCode: http.StatusNotFound,
	},
	NotImplemented: {
		Code:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented.",
		HttpStatusCode: http.StatusNotImplemented,
	},
	RequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HttpStatusCode: http.StatusForbidden,
	},
	SignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided.",
		HttpStatusCode: http.StatusForbidden,
	},
	TooManyBuckets: {
		Code:           "TooManyBuckets",
		Description:    "You have attempted to create more buckets than allowed.",
		HttpStatusCode: http.StatusBadRequest,
	},
}

// errorCodeError provides errorCode to Error. It returns empty if
// the code provided is unknown
func errorCodeError(code int) Error {
	return errorCodeResponse[code]
}

func getErrorResponse(err Error, resource string) ErrorResponse {
	var data = ErrorResponse{}
	data.Code = err.Code
	data.Message = err.Description
	if resource != "" {
		data.Resource = resource
	}
	// TODO implement this in future
	data.RequestId = "3L137"
	data.HostId = "3L137"

	return data
}
