/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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
	"context"
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/s3select"
	"github.com/minio/minio/pkg/s3select/format"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// APIErrorResponse - error response format
type APIErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string
	Message    string
	Key        string `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName string `xml:"BucketName,omitempty" json:"BucketName,omitempty"`
	Resource   string
	RequestID  string `xml:"RequestId" json:"RequestId"`
	HostID     string `xml:"HostId" json:"HostId"`
}

// APIErrorCode type of error status.
type APIErrorCode int

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrNone APIErrorCode = iota
	ErrAccessDenied
	ErrBadDigest
	ErrEntityTooSmall
	ErrEntityTooLarge
	ErrIncompleteBody
	ErrInternalError
	ErrInvalidAccessKeyID
	ErrInvalidBucketName
	ErrInvalidDigest
	ErrInvalidRange
	ErrInvalidCopyPartRange
	ErrInvalidCopyPartRangeSource
	ErrInvalidMaxKeys
	ErrInvalidMaxUploads
	ErrInvalidMaxParts
	ErrInvalidPartNumberMarker
	ErrInvalidRequestBody
	ErrInvalidCopySource
	ErrInvalidMetadataDirective
	ErrInvalidCopyDest
	ErrInvalidPolicyDocument
	ErrInvalidObjectState
	ErrMalformedXML
	ErrMissingContentLength
	ErrMissingContentMD5
	ErrMissingRequestBodyError
	ErrNoSuchBucket
	ErrNoSuchBucketPolicy
	ErrNoSuchKey
	ErrNoSuchUpload
	ErrNoSuchVersion
	ErrNotImplemented
	ErrPreconditionFailed
	ErrRequestTimeTooSkewed
	ErrSignatureDoesNotMatch
	ErrMethodNotAllowed
	ErrInvalidPart
	ErrInvalidPartOrder
	ErrAuthorizationHeaderMalformed
	ErrMalformedPOSTRequest
	ErrPOSTFileRequired
	ErrSignatureVersionNotSupported
	ErrBucketNotEmpty
	ErrAllAccessDisabled
	ErrMalformedPolicy
	ErrMissingFields
	ErrMissingCredTag
	ErrCredMalformed
	ErrInvalidRegion
	ErrInvalidService
	ErrInvalidRequestVersion
	ErrMissingSignTag
	ErrMissingSignHeadersTag
	ErrPolicyAlreadyExpired
	ErrMalformedDate
	ErrMalformedPresignedDate
	ErrMalformedCredentialDate
	ErrMalformedCredentialRegion
	ErrMalformedExpires
	ErrNegativeExpires
	ErrAuthHeaderEmpty
	ErrExpiredPresignRequest
	ErrRequestNotReadyYet
	ErrUnsignedHeaders
	ErrMissingDateHeader
	ErrInvalidQuerySignatureAlgo
	ErrInvalidQueryParams
	ErrBucketAlreadyOwnedByYou
	ErrInvalidDuration
	ErrBucketAlreadyExists
	ErrMetadataTooLarge
	ErrUnsupportedMetadata
	ErrMaximumExpires
	ErrSlowDown
	ErrInvalidPrefixMarker
	ErrBadRequest
	// Add new error codes here.

	// SSE-S3 related API errors
	ErrInvalidEncryptionMethod

	// Server-Side-Encryption (with Customer provided key) related API errors.
	ErrInsecureSSECustomerRequest
	ErrSSEMultipartEncrypted
	ErrSSEEncryptedObject
	ErrInvalidEncryptionParameters
	ErrInvalidSSECustomerAlgorithm
	ErrInvalidSSECustomerKey
	ErrMissingSSECustomerKey
	ErrMissingSSECustomerKeyMD5
	ErrSSECustomerKeyMD5Mismatch
	ErrInvalidSSECustomerParameters
	ErrIncompatibleEncryptionMethod
	ErrKMSNotConfigured
	ErrKMSAuthFailure

	ErrNoAccessKey
	ErrInvalidToken

	// Bucket notification related errors.
	ErrEventNotification
	ErrARNNotification
	ErrRegionNotification
	ErrOverlappingFilterNotification
	ErrFilterNameInvalid
	ErrFilterNamePrefix
	ErrFilterNameSuffix
	ErrFilterValueInvalid
	ErrOverlappingConfigs
	ErrUnsupportedNotification

	// S3 extended errors.
	ErrContentSHA256Mismatch

	// Add new extended error codes here.

	// Minio extended errors.
	ErrReadQuorum
	ErrWriteQuorum
	ErrStorageFull
	ErrRequestBodyParse
	ErrObjectExistsAsDirectory
	ErrPolicyNesting
	ErrInvalidObjectName
	ErrInvalidResourceName
	ErrServerNotInitialized
	ErrOperationTimedOut
	ErrInvalidRequest
	// Minio storage class error codes
	ErrInvalidStorageClass
	ErrBackendDown
	// Add new extended error codes here.
	// Please open a https://github.com/minio/minio/issues before adding
	// new error codes here.

	ErrMalformedJSON
	ErrAdminNoSuchUser
	ErrAdminNoSuchPolicy
	ErrAdminInvalidArgument
	ErrAdminInvalidAccessKey
	ErrAdminInvalidSecretKey
	ErrAdminConfigNoQuorum
	ErrAdminConfigTooLarge
	ErrAdminConfigBadJSON
	ErrAdminConfigDuplicateKeys
	ErrAdminCredentialsMismatch
	ErrInsecureClientRequest
	ErrObjectTampered

	ErrHealNotImplemented
	ErrHealNoSuchProcess
	ErrHealInvalidClientToken
	ErrHealMissingBucket
	ErrHealAlreadyRunning
	ErrHealOverlappingPaths
	ErrIncorrectContinuationToken

	//S3 Select Errors
	ErrEmptyRequestBody
	ErrUnsupportedFunction
	ErrInvalidExpressionType
	ErrBusy
	ErrUnauthorizedAccess
	ErrExpressionTooLong
	ErrIllegalSQLFunctionArgument
	ErrInvalidKeyPath
	ErrInvalidCompressionFormat
	ErrInvalidFileHeaderInfo
	ErrInvalidJSONType
	ErrInvalidQuoteFields
	ErrInvalidRequestParameter
	ErrInvalidDataType
	ErrInvalidTextEncoding
	ErrInvalidDataSource
	ErrInvalidTableAlias
	ErrMissingRequiredParameter
	ErrObjectSerializationConflict
	ErrUnsupportedSQLOperation
	ErrUnsupportedSQLStructure
	ErrUnsupportedSyntax
	ErrUnsupportedRangeHeader
	ErrLexerInvalidChar
	ErrLexerInvalidOperator
	ErrLexerInvalidLiteral
	ErrLexerInvalidIONLiteral
	ErrParseExpectedDatePart
	ErrParseExpectedKeyword
	ErrParseExpectedTokenType
	ErrParseExpected2TokenTypes
	ErrParseExpectedNumber
	ErrParseExpectedRightParenBuiltinFunctionCall
	ErrParseExpectedTypeName
	ErrParseExpectedWhenClause
	ErrParseUnsupportedToken
	ErrParseUnsupportedLiteralsGroupBy
	ErrParseExpectedMember
	ErrParseUnsupportedSelect
	ErrParseUnsupportedCase
	ErrParseUnsupportedCaseClause
	ErrParseUnsupportedAlias
	ErrParseUnsupportedSyntax
	ErrParseUnknownOperator
	ErrParseMissingIdentAfterAt
	ErrParseUnexpectedOperator
	ErrParseUnexpectedTerm
	ErrParseUnexpectedToken
	ErrParseUnexpectedKeyword
	ErrParseExpectedExpression
	ErrParseExpectedLeftParenAfterCast
	ErrParseExpectedLeftParenValueConstructor
	ErrParseExpectedLeftParenBuiltinFunctionCall
	ErrParseExpectedArgumentDelimiter
	ErrParseCastArity
	ErrParseInvalidTypeParam
	ErrParseEmptySelect
	ErrParseSelectMissingFrom
	ErrParseExpectedIdentForGroupName
	ErrParseExpectedIdentForAlias
	ErrParseUnsupportedCallWithStar
	ErrParseNonUnaryAgregateFunctionCall
	ErrParseMalformedJoin
	ErrParseExpectedIdentForAt
	ErrParseAsteriskIsNotAloneInSelectList
	ErrParseCannotMixSqbAndWildcardInSelectList
	ErrParseInvalidContextForWildcardInSelectList
	ErrIncorrectSQLFunctionArgumentType
	ErrValueParseFailure
	ErrEvaluatorInvalidArguments
	ErrIntegerOverflow
	ErrLikeInvalidInputs
	ErrCastFailed
	ErrInvalidCast
	ErrEvaluatorInvalidTimestampFormatPattern
	ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing
	ErrEvaluatorTimestampFormatPatternDuplicateFields
	ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch
	ErrEvaluatorUnterminatedTimestampFormatPatternToken
	ErrEvaluatorInvalidTimestampFormatPatternToken
	ErrEvaluatorInvalidTimestampFormatPatternSymbol
	ErrEvaluatorBindingDoesNotExist
	ErrMissingHeaders
	ErrInvalidColumnIndex

	ErrAdminConfigNotificationTargetsFailed
	ErrAdminProfilerNotEnabled
	ErrInvalidDecompressedSize
)

// error code to APIError structure, these fields carry respective
// descriptions for all the error responses.
var errorCodeResponse = map[APIErrorCode]APIError{
	ErrInvalidCopyDest: {
		Code:           "InvalidRequest",
		Description:    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopySource: {
		Code:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMetadataDirective: {
		Code:           "InvalidArgument",
		Description:    "Unknown metadata directive.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidStorageClass: {
		Code:           "InvalidStorageClass",
		Description:    "Invalid storage class.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequestBody: {
		Code:           "InvalidArgument",
		Description:    "Body shouldn't be set for this request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxUploads: {
		Code:           "InvalidArgument",
		Description:    "Argument max-uploads must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxKeys: {
		Code:           "InvalidArgument",
		Description:    "Argument maxKeys must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxParts: {
		Code:           "InvalidArgument",
		Description:    "Argument max-parts must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartNumberMarker: {
		Code:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPolicyDocument: {
		Code:           "InvalidPolicyDocument",
		Description:    "The content of the form does not meet the conditions specified in the policy document.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrBadDigest: {
		Code:           "BadDigest",
		Description:    "The Content-Md5 you specified did not match what we received.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEntityTooSmall: {
		Code:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEntityTooLarge: {
		Code:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncompleteBody: {
		Code:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	},
	ErrInvalidAccessKeyID: {
		Code:           "InvalidAccessKeyId",
		Description:    "The access key ID you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidBucketName: {
		Code:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-Md5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRange: {
		Code:           "InvalidRange",
		Description:    "The requested range is not satisfiable",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
	},
	ErrMalformedXML: {
		Code:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingContentLength: {
		Code:           "MissingContentLength",
		Description:    "You must provide the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusLengthRequired,
	},
	ErrMissingContentMD5: {
		Code:           "MissingContentMD5",
		Description:    "Missing required header for this request: Content-Md5.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingRequestBodyError: {
		Code:           "MissingRequestBodyError",
		Description:    "Request body is empty.",
		HTTPStatusCode: http.StatusLengthRequired,
	},
	ErrNoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchBucketPolicy: {
		Code:           "NoSuchBucketPolicy",
		Description:    "The bucket policy does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchUpload: {
		Code:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchVersion: {
		Code:           "NoSuchVersion",
		Description:    "Indicates that the version ID specified in the request does not match an existing version.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNotImplemented: {
		Code:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	ErrPreconditionFailed: {
		Code:           "PreconditionFailed",
		Description:    "At least one of the pre-conditions you specified did not hold",
		HTTPStatusCode: http.StatusPreconditionFailed,
	},
	ErrRequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrSignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrMethodNotAllowed: {
		Code:           "MethodNotAllowed",
		Description:    "The specified method is not allowed against this resource.",
		HTTPStatusCode: http.StatusMethodNotAllowed,
	},
	ErrInvalidPart: {
		Code:           "InvalidPart",
		Description:    "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartOrder: {
		Code:           "InvalidPartOrder",
		Description:    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidObjectState: {
		Code:           "InvalidObjectState",
		Description:    "The operation is not valid for the current state of the object.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAuthorizationHeaderMalformed: {
		Code:           "AuthorizationHeaderMalformed",
		Description:    "The authorization header is malformed; the region is wrong; expecting 'us-east-1'.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPOSTRequest: {
		Code:           "MalformedPOSTRequest",
		Description:    "The body of your POST request is not well-formed multipart/form-data.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPOSTFileRequired: {
		Code:           "InvalidArgument",
		Description:    "POST requires exactly one file upload per request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSignatureVersionNotSupported: {
		Code:           "InvalidRequest",
		Description:    "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketNotEmpty: {
		Code:           "BucketNotEmpty",
		Description:    "The bucket you tried to delete is not empty",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrBucketAlreadyExists: {
		Code:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrAllAccessDisabled: {
		Code:           "AllAccessDisabled",
		Description:    "All access to this bucket has been disabled.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrMalformedPolicy: {
		Code:           "MalformedPolicy",
		Description:    "Policy has invalid resource.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingFields: {
		Code:           "MissingFields",
		Description:    "Missing fields in request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingCredTag: {
		Code:           "InvalidRequest",
		Description:    "Missing Credential field for this request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCredMalformed: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedDate: {
		Code:           "MalformedDate",
		Description:    "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPresignedDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\"",
		HTTPStatusCode: http.StatusBadRequest,
	},
	// FIXME: Should contain the invalid param set as seen in https://github.com/minio/minio/issues/2385.
	// right Description:    "Error parsing the X-Amz-Credential parameter; incorrect date format \"%s\". This date in the credential must be in the format \"yyyyMMdd\".",
	// Need changes to make sure variable messages can be constructed.
	ErrMalformedCredentialDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; incorrect date format \"%s\". This date in the credential must be in the format \"yyyyMMdd\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	// FIXME: Should contain the invalid param set as seen in https://github.com/minio/minio/issues/2385.
	// right Description:    "Error parsing the X-Amz-Credential parameter; the region 'us-east-' is wrong; expecting 'us-east-1'".
	// Need changes to make sure variable messages can be constructed.
	ErrMalformedCredentialRegion: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; the region is wrong;",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRegion: {
		Code:           "InvalidRegion",
		Description:    "Region does not match.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	// FIXME: Should contain the invalid param set as seen in https://github.com/minio/minio/issues/2385.
	// right Description:   "Error parsing the X-Amz-Credential parameter; incorrect service \"s4\". This endpoint belongs to \"s3\".".
	// Need changes to make sure variable messages can be constructed.
	ErrInvalidService: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; incorrect service. This endpoint belongs to \"s3\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	// FIXME: Should contain the invalid param set as seen in https://github.com/minio/minio/issues/2385.
	// Description:   "Error parsing the X-Amz-Credential parameter; incorrect terminal "aws4_reque". This endpoint uses "aws4_request".
	// Need changes to make sure variable messages can be constructed.
	ErrInvalidRequestVersion: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; incorrect terminal. This endpoint uses \"aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignTag: {
		Code:           "AccessDenied",
		Description:    "Signature header missing Signature field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignHeadersTag: {
		Code:           "InvalidArgument",
		Description:    "Signature header missing SignedHeaders field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPolicyAlreadyExpired: {
		Code:           "AccessDenied",
		Description:    "Invalid according to Policy: Policy expired.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires should be a number",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNegativeExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be non-negative",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAuthHeaderEmpty: {
		Code:           "InvalidArgument",
		Description:    "Authorization header is invalid -- one and only one ' ' (space) required.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingDateHeader: {
		Code:           "AccessDenied",
		Description:    "AWS authentication requires a valid Date or x-amz-date header",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQuerySignatureAlgo: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrExpiredPresignRequest: {
		Code:           "AccessDenied",
		Description:    "Request has expired",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrRequestNotReadyYet: {
		Code:           "AccessDenied",
		Description:    "Request is not valid yet",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrSlowDown: {
		Code:           "SlowDown",
		Description:    "Please reduce your request",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrInvalidPrefixMarker: {
		Code:           "InvalidPrefixMarker",
		Description:    "Invalid marker prefix combination",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBadRequest: {
		Code:           "BadRequest",
		Description:    "400 BadRequest",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// FIXME: Actual XML error response also contains the header which missed in list of signed header parameters.
	ErrUnsignedHeaders: {
		Code:           "AccessDenied",
		Description:    "There were headers present in the request which were not signed",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQueryParams: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketAlreadyOwnedByYou: {
		Code:           "BucketAlreadyOwnedByYou",
		Description:    "Your previous request to create the named bucket succeeded and you already own it.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrInvalidDuration: {
		Code:           "InvalidDuration",
		Description:    "Duration provided in the request is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	/// Bucket notification related errors.
	ErrEventNotification: {
		Code:           "InvalidArgument",
		Description:    "A specified event is not supported for notifications.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrARNNotification: {
		Code:           "InvalidArgument",
		Description:    "A specified destination ARN does not exist or is not well-formed. Verify the destination ARN.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRegionNotification: {
		Code:           "InvalidArgument",
		Description:    "A specified destination is in a different region than the bucket. You must use a destination that resides in the same region as the bucket.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrOverlappingFilterNotification: {
		Code:           "InvalidArgument",
		Description:    "An object key name filtering rule defined with overlapping prefixes, overlapping suffixes, or overlapping combinations of prefixes and suffixes for the same event types.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrFilterNameInvalid: {
		Code:           "InvalidArgument",
		Description:    "filter rule name must be either prefix or suffix",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrFilterNamePrefix: {
		Code:           "InvalidArgument",
		Description:    "Cannot specify more than one prefix rule in a filter.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrFilterNameSuffix: {
		Code:           "InvalidArgument",
		Description:    "Cannot specify more than one suffix rule in a filter.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrFilterValueInvalid: {
		Code:           "InvalidArgument",
		Description:    "Size of filter rule value cannot exceed 1024 bytes in UTF-8 representation",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrOverlappingConfigs: {
		Code:           "InvalidArgument",
		Description:    "Configurations overlap. Configurations on the same bucket cannot share a common event type.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedNotification: {
		Code:           "UnsupportedNotification",
		Description:    "Minio server does not support Topic or Cloud Function based notifications.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopyPartRange: {
		Code:           "InvalidArgument",
		Description:    "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopyPartRangeSource: {
		Code:           "InvalidArgument",
		Description:    "Range specified is not valid for source object",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMetadataTooLarge: {
		Code:           "InvalidArgument",
		Description:    "Your metadata headers exceed the maximum allowed metadata size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidEncryptionMethod: {
		Code:           "InvalidRequest",
		Description:    "The encryption method specified is not supported",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInsecureSSECustomerRequest: {
		Code:           "InvalidRequest",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSEMultipartEncrypted: {
		Code:           "InvalidRequest",
		Description:    "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSEEncryptedObject: {
		Code:           "InvalidRequest",
		Description:    "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidEncryptionParameters: {
		Code:           "InvalidRequest",
		Description:    "The encryption parameters are not applicable to this object.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidSSECustomerAlgorithm: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidSSECustomerKey: {
		Code:           "InvalidArgument",
		Description:    "The secret key was invalid for the specified algorithm.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSSECustomerKey: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSSECustomerKeyMD5: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide the client calculated MD5 of the secret key.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyMD5Mismatch: {
		Code:           "InvalidArgument",
		Description:    "The calculated MD5 hash of the key did not match the hash that was provided.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidSSECustomerParameters: {
		Code:           "InvalidArgument",
		Description:    "The provided encryption parameters did not match the ones used originally.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncompatibleEncryptionMethod: {
		Code:           "InvalidArgument",
		Description:    "Server side encryption specified with both SSE-C and SSE-S3 headers",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSNotConfigured: {
		Code:           "InvalidArgument",
		Description:    "Server side encryption specified but KMS is not configured",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSAuthFailure: {
		Code:           "InvalidArgument",
		Description:    "Server side encryption specified but KMS authorization failed",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoAccessKey: {
		Code:           "AccessDenied",
		Description:    "No AWSAccessKey was presented",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidToken: {
		Code:           "InvalidTokenId",
		Description:    "The security token included in the request is invalid",
		HTTPStatusCode: http.StatusForbidden,
	},

	/// S3 extensions.
	ErrContentSHA256Mismatch: {
		Code:           "XAmzContentSHA256Mismatch",
		Description:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	/// Minio extensions.
	ErrStorageFull: {
		Code:           "XMinioStorageFull",
		Description:    "Storage backend has reached its minimum free disk threshold. Please delete a few objects to proceed.",
		HTTPStatusCode: http.StatusInsufficientStorage,
	},
	ErrRequestBodyParse: {
		Code:           "XMinioRequestBodyParse",
		Description:    "The request body failed to parse.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectExistsAsDirectory: {
		Code:           "XMinioObjectExistsAsDirectory",
		Description:    "Object name already exists as a directory.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrReadQuorum: {
		Code:           "XMinioReadQuorum",
		Description:    "Multiple disk failures, unable to reconstruct data.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrWriteQuorum: {
		Code:           "XMinioWriteQuorum",
		Description:    "Multiple disks failures, unable to write data.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrPolicyNesting: {
		Code:           "XMinioPolicyNesting",
		Description:    "New bucket policy conflicts with an existing policy. Please try again with new prefix.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrInvalidObjectName: {
		Code:           "XMinioInvalidObjectName",
		Description:    "Object name contains unsupported characters.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidResourceName: {
		Code:           "XMinioInvalidResourceName",
		Description:    "Resource name contains bad components such as \"..\" or \".\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrServerNotInitialized: {
		Code:           "XMinioServerNotInitialized",
		Description:    "Server not initialized, please try again.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrMalformedJSON: {
		Code:           "XMinioMalformedJSON",
		Description:    "The JSON you provided was not well-formed or did not validate against our published format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminNoSuchUser: {
		Code:           "XMinioAdminNoSuchUser",
		Description:    "The specified user does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminNoSuchPolicy: {
		Code:           "XMinioAdminNoSuchPolicy",
		Description:    "The canned policy does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminInvalidArgument: {
		Code:           "XMinioAdminInvalidArgument",
		Description:    "Invalid arguments specified.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminInvalidAccessKey: {
		Code:           "XMinioAdminInvalidAccessKey",
		Description:    "The access key is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminInvalidSecretKey: {
		Code:           "XMinioAdminInvalidSecretKey",
		Description:    "The secret key is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigNoQuorum: {
		Code:           "XMinioAdminConfigNoQuorum",
		Description:    "Configuration update failed because server quorum was not met",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrAdminConfigTooLarge: {
		Code: "XMinioAdminConfigTooLarge",
		Description: fmt.Sprintf("Configuration data provided exceeds the allowed maximum of %d bytes",
			maxEConfigJSONSize),
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigBadJSON: {
		Code:           "XMinioAdminConfigBadJSON",
		Description:    "JSON configuration provided is of incorrect format",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigDuplicateKeys: {
		Code:           "XMinioAdminConfigDuplicateKeys",
		Description:    "JSON configuration provided has objects with duplicate keys",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigNotificationTargetsFailed: {
		Code:           "XMinioAdminNotificationTargetsTestFailed",
		Description:    "Configuration update failed due an unsuccessful attempt to connect to one or more notification servers",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminProfilerNotEnabled: {
		Code:           "XMinioAdminProfilerNotEnabled",
		Description:    "Unable to perform the requested operation because profiling is not enabled",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminCredentialsMismatch: {
		Code:           "XMinioAdminCredentialsMismatch",
		Description:    "Credentials in config mismatch with server environment variables",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrInsecureClientRequest: {
		Code:           "XMinioInsecureClientRequest",
		Description:    "Cannot respond to plain-text request from TLS-encrypted server",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrOperationTimedOut: {
		Code:           "XMinioServerTimedOut",
		Description:    "A timeout occurred while trying to lock a resource",
		HTTPStatusCode: http.StatusRequestTimeout,
	},
	ErrUnsupportedMetadata: {
		Code:           "InvalidArgument",
		Description:    "Your metadata headers are not supported.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectTampered: {
		Code:           "XMinioObjectTampered",
		Description:    errObjectTampered.Error(),
		HTTPStatusCode: http.StatusPartialContent,
	},
	ErrMaximumExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Generic Invalid-Request error. Should be used for response errors only for unlikely
	// corner case errors for which introducing new APIErrorCode is not worth it. LogIf()
	// should be used to log the error at the source of the error for debugging purposes.
	ErrInvalidRequest: {
		Code:           "InvalidRequest",
		Description:    "Invalid Request",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrHealNotImplemented: {
		Code:           "XMinioHealNotImplemented",
		Description:    "This server does not implement heal functionality.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrHealNoSuchProcess: {
		Code:           "XMinioHealNoSuchProcess",
		Description:    "No such heal process is running on the server",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrHealInvalidClientToken: {
		Code:           "XMinioHealInvalidClientToken",
		Description:    "Client token mismatch",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrHealMissingBucket: {
		Code:           "XMinioHealMissingBucket",
		Description:    "A heal start request with a non-empty object-prefix parameter requires a bucket to be specified.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrHealAlreadyRunning: {
		Code:           "XMinioHealAlreadyRunning",
		Description:    "",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrHealOverlappingPaths: {
		Code:           "XMinioHealOverlappingPaths",
		Description:    "",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBackendDown: {
		Code:           "XMinioBackendDown",
		Description:    "Object storage backend is unreachable",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrIncorrectContinuationToken: {
		Code:           "InvalidArgument",
		Description:    "The continuation token provided is incorrect",
		HTTPStatusCode: http.StatusBadRequest,
	},
	//S3 Select API Errors
	ErrEmptyRequestBody: {
		Code:           "EmptyRequestBody",
		Description:    "Request body cannot be empty.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedFunction: {
		Code:           "UnsupportedFunction",
		Description:    "Encountered an unsupported SQL function.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidDataSource: {
		Code:           "InvalidDataSource",
		Description:    "Invalid data source type. Only CSV and JSON are supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidExpressionType: {
		Code:           "InvalidExpressionType",
		Description:    "The ExpressionType is invalid. Only SQL expressions are supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBusy: {
		Code:           "Busy",
		Description:    "The service is unavailable. Please retry.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrUnauthorizedAccess: {
		Code:           "UnauthorizedAccess",
		Description:    "You are not authorized to perform this operation",
		HTTPStatusCode: http.StatusUnauthorized,
	},
	ErrExpressionTooLong: {
		Code:           "ExpressionTooLong",
		Description:    "The SQL expression is too long: The maximum byte-length for the SQL expression is 256 KB.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIllegalSQLFunctionArgument: {
		Code:           "IllegalSqlFunctionArgument",
		Description:    "Illegal argument was used in the SQL function.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidKeyPath: {
		Code:           "InvalidKeyPath",
		Description:    "Key path in the SQL expression is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCompressionFormat: {
		Code:           "InvalidCompressionFormat",
		Description:    "The file is not in a supported compression format. Only GZIP is supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidFileHeaderInfo: {
		Code:           "InvalidFileHeaderInfo",
		Description:    "The FileHeaderInfo is invalid. Only NONE, USE, and IGNORE are supported.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidJSONType: {
		Code:           "InvalidJsonType",
		Description:    "The JsonType is invalid. Only DOCUMENT and LINES are supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQuoteFields: {
		Code:           "InvalidQuoteFields",
		Description:    "The QuoteFields is invalid. Only ALWAYS and ASNEEDED are supported.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequestParameter: {
		Code:           "InvalidRequestParameter",
		Description:    "The value of a parameter in SelectRequest element is invalid. Check the service API documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidDataType: {
		Code:           "InvalidDataType",
		Description:    "The SQL expression contains an invalid data type.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidTextEncoding: {
		Code:           "InvalidTextEncoding",
		Description:    "Invalid encoding type. Only UTF-8 encoding is supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidTableAlias: {
		Code:           "InvalidTableAlias",
		Description:    "The SQL expression contains an invalid table alias.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingRequiredParameter: {
		Code:           "MissingRequiredParameter",
		Description:    "The SelectRequest entity is missing a required parameter. Check the service documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectSerializationConflict: {
		Code:           "ObjectSerializationConflict",
		Description:    "The SelectRequest entity can only contain one of CSV or JSON. Check the service documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedSQLOperation: {
		Code:           "UnsupportedSqlOperation",
		Description:    "Encountered an unsupported SQL operation.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedSQLStructure: {
		Code:           "UnsupportedSqlStructure",
		Description:    "Encountered an unsupported SQL structure. Check the SQL Reference.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedSyntax: {
		Code:           "UnsupportedSyntax",
		Description:    "Encountered invalid syntax.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedRangeHeader: {
		Code:           "UnsupportedRangeHeader",
		Description:    "Range header is not supported for this operation.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLexerInvalidChar: {
		Code:           "LexerInvalidChar",
		Description:    "The SQL expression contains an invalid character.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLexerInvalidOperator: {
		Code:           "LexerInvalidOperator",
		Description:    "The SQL expression contains an invalid literal.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLexerInvalidLiteral: {
		Code:           "LexerInvalidLiteral",
		Description:    "The SQL expression contains an invalid operator.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLexerInvalidIONLiteral: {
		Code:           "LexerInvalidIONLiteral",
		Description:    "The SQL expression contains an invalid operator.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedDatePart: {
		Code:           "ParseExpectedDatePart",
		Description:    "Did not find the expected date part in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedKeyword: {
		Code:           "ParseExpectedKeyword",
		Description:    "Did not find the expected keyword in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedTokenType: {
		Code:           "ParseExpectedTokenType",
		Description:    "Did not find the expected token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpected2TokenTypes: {
		Code:           "ParseExpected2TokenTypes",
		Description:    "Did not find the expected token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedNumber: {
		Code:           "ParseExpectedNumber",
		Description:    "Did not find the expected number in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedRightParenBuiltinFunctionCall: {
		Code:           "ParseExpectedRightParenBuiltinFunctionCall",
		Description:    "Did not find the expected right parenthesis character in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedTypeName: {
		Code:           "ParseExpectedTypeName",
		Description:    "Did not find the expected type name in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedWhenClause: {
		Code:           "ParseExpectedWhenClause",
		Description:    "Did not find the expected WHEN clause in the SQL expression. CASE is not supported.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedToken: {
		Code:           "ParseUnsupportedToken",
		Description:    "The SQL expression contains an unsupported token.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedLiteralsGroupBy: {
		Code:           "ParseUnsupportedLiteralsGroupBy",
		Description:    "The SQL expression contains an unsupported use of GROUP BY.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedMember: {
		Code:           "ParseExpectedMember",
		Description:    "The SQL expression contains an unsupported use of MEMBER.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedSelect: {
		Code:           "ParseUnsupportedSelect",
		Description:    "The SQL expression contains an unsupported use of SELECT.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedCase: {
		Code:           "ParseUnsupportedCase",
		Description:    "The SQL expression contains an unsupported use of CASE.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedCaseClause: {
		Code:           "ParseUnsupportedCaseClause",
		Description:    "The SQL expression contains an unsupported use of CASE.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedAlias: {
		Code:           "ParseUnsupportedAlias",
		Description:    "The SQL expression contains an unsupported use of ALIAS.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedSyntax: {
		Code:           "ParseUnsupportedSyntax",
		Description:    "The SQL expression contains unsupported syntax.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnknownOperator: {
		Code:           "ParseUnknownOperator",
		Description:    "The SQL expression contains an invalid operator.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseMissingIdentAfterAt: {
		Code:           "ParseMissingIdentAfterAt",
		Description:    "Did not find the expected identifier after the @ symbol in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnexpectedOperator: {
		Code:           "ParseUnexpectedOperator",
		Description:    "The SQL expression contains an unexpected operator.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnexpectedTerm: {
		Code:           "ParseUnexpectedTerm",
		Description:    "The SQL expression contains an unexpected term.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnexpectedToken: {
		Code:           "ParseUnexpectedToken",
		Description:    "The SQL expression contains an unexpected token.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnexpectedKeyword: {
		Code:           "ParseUnexpectedKeyword",
		Description:    "The SQL expression contains an unexpected keyword.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedExpression: {
		Code:           "ParseExpectedExpression",
		Description:    "Did not find the expected SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedLeftParenAfterCast: {
		Code:           "ParseExpectedLeftParenAfterCast",
		Description:    "Did not find expected the left parenthesis in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedLeftParenValueConstructor: {
		Code:           "ParseExpectedLeftParenValueConstructor",
		Description:    "Did not find expected the left parenthesis in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedLeftParenBuiltinFunctionCall: {
		Code:           "ParseExpectedLeftParenBuiltinFunctionCall",
		Description:    "Did not find the expected left parenthesis in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedArgumentDelimiter: {
		Code:           "ParseExpectedArgumentDelimiter",
		Description:    "Did not find the expected argument delimiter in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseCastArity: {
		Code:           "ParseCastArity",
		Description:    "The SQL expression CAST has incorrect arity.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseInvalidTypeParam: {
		Code:           "ParseInvalidTypeParam",
		Description:    "The SQL expression contains an invalid parameter value.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseEmptySelect: {
		Code:           "ParseEmptySelect",
		Description:    "The SQL expression contains an empty SELECT.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseSelectMissingFrom: {
		Code:           "ParseSelectMissingFrom",
		Description:    "GROUP is not supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedIdentForGroupName: {
		Code:           "ParseExpectedIdentForGroupName",
		Description:    "GROUP is not supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedIdentForAlias: {
		Code:           "ParseExpectedIdentForAlias",
		Description:    "Did not find the expected identifier for the alias in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseUnsupportedCallWithStar: {
		Code:           "ParseUnsupportedCallWithStar",
		Description:    "Only COUNT with (*) as a parameter is supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseNonUnaryAgregateFunctionCall: {
		Code:           "ParseNonUnaryAgregateFunctionCall",
		Description:    "Only one argument is supported for aggregate functions in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseMalformedJoin: {
		Code:           "ParseMalformedJoin",
		Description:    "JOIN is not supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseExpectedIdentForAt: {
		Code:           "ParseExpectedIdentForAt",
		Description:    "Did not find the expected identifier for AT name in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseAsteriskIsNotAloneInSelectList: {
		Code:           "ParseAsteriskIsNotAloneInSelectList",
		Description:    "Other expressions are not allowed in the SELECT list when '*' is used without dot notation in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseCannotMixSqbAndWildcardInSelectList: {
		Code:           "ParseCannotMixSqbAndWildcardInSelectList",
		Description:    "Cannot mix [] and * in the same expression in a SELECT list in SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrParseInvalidContextForWildcardInSelectList: {
		Code:           "ParseInvalidContextForWildcardInSelectList",
		Description:    "Invalid use of * in SELECT list in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncorrectSQLFunctionArgumentType: {
		Code:           "IncorrectSqlFunctionArgumentType",
		Description:    "Incorrect type of arguments in function call in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrValueParseFailure: {
		Code:           "ValueParseFailure",
		Description:    "Time stamp parse failure in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorInvalidArguments: {
		Code:           "EvaluatorInvalidArguments",
		Description:    "Incorrect number of arguments in the function call in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIntegerOverflow: {
		Code:           "IntegerOverflow",
		Description:    "Int overflow or underflow in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLikeInvalidInputs: {
		Code:           "LikeInvalidInputs",
		Description:    "Invalid argument given to the LIKE clause in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCastFailed: {
		Code:           "CastFailed",
		Description:    "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCast: {
		Code:           "InvalidCast",
		Description:    "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorInvalidTimestampFormatPattern: {
		Code:           "EvaluatorInvalidTimestampFormatPattern",
		Description:    "Time stamp format pattern requires additional fields in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing: {
		Code:           "EvaluatorInvalidTimestampFormatPatternSymbolForParsing",
		Description:    "Time stamp format pattern contains a valid format symbol that cannot be applied to time stamp parsing in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorTimestampFormatPatternDuplicateFields: {
		Code:           "EvaluatorTimestampFormatPatternDuplicateFields",
		Description:    "Time stamp format pattern contains multiple format specifiers representing the time stamp field in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch: {
		Code:           "EvaluatorUnterminatedTimestampFormatPatternToken",
		Description:    "Time stamp format pattern contains unterminated token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorUnterminatedTimestampFormatPatternToken: {
		Code:           "EvaluatorInvalidTimestampFormatPatternToken",
		Description:    "Time stamp format pattern contains an invalid token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorInvalidTimestampFormatPatternToken: {
		Code:           "EvaluatorInvalidTimestampFormatPatternToken",
		Description:    "Time stamp format pattern contains an invalid token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorInvalidTimestampFormatPatternSymbol: {
		Code:           "EvaluatorInvalidTimestampFormatPatternSymbol",
		Description:    "Time stamp format pattern contains an invalid symbol in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEvaluatorBindingDoesNotExist: {
		Code:           "ErrEvaluatorBindingDoesNotExist",
		Description:    "A column name or a path provided does not exist in the SQL expression",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingHeaders: {
		Code:           "MissingHeaders",
		Description:    "Some headers in the query are missing from the file. Check the file and try again.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidColumnIndex: {
		Code:           "InvalidColumnIndex",
		Description:    "The column index is invalid. Please check the service documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidDecompressedSize: {
		Code:           "XMinioInvalidDecompressedSize",
		Description:    "The data provided is unfit for decompression",
		HTTPStatusCode: http.StatusBadRequest,
	},
	// Add your error structure here.
}

// toAPIErrorCode - Converts embedded errors. Convenience
// function written to handle all cases where we have known types of
// errors returned by underlying layers.
func toAPIErrorCode(ctx context.Context, err error) (apiErr APIErrorCode) {
	if err == nil {
		return ErrNone
	}

	// Verify if the underlying error is signature mismatch.
	switch err {
	case errInvalidArgument:
		apiErr = ErrAdminInvalidArgument
	case errNoSuchUser:
		apiErr = ErrAdminNoSuchUser
	case errNoSuchPolicy:
		apiErr = ErrAdminNoSuchPolicy
	case errSignatureMismatch:
		apiErr = ErrSignatureDoesNotMatch
	case errInvalidRange:
		apiErr = ErrInvalidRange
	case errDataTooLarge:
		apiErr = ErrEntityTooLarge
	case errDataTooSmall:
		apiErr = ErrEntityTooSmall
	case auth.ErrInvalidAccessKeyLength:
		apiErr = ErrAdminInvalidAccessKey
	case auth.ErrInvalidSecretKeyLength:
		apiErr = ErrAdminInvalidSecretKey
	// SSE errors
	case errInvalidEncryptionParameters:
		apiErr = ErrInvalidEncryptionParameters
	case crypto.ErrInvalidEncryptionMethod:
		apiErr = ErrInvalidEncryptionMethod
	case crypto.ErrInvalidCustomerAlgorithm:
		apiErr = ErrInvalidSSECustomerAlgorithm
	case crypto.ErrInvalidCustomerKey:
		apiErr = ErrInvalidSSECustomerKey
	case crypto.ErrMissingCustomerKey:
		apiErr = ErrMissingSSECustomerKey
	case crypto.ErrMissingCustomerKeyMD5:
		apiErr = ErrMissingSSECustomerKeyMD5
	case crypto.ErrCustomerKeyMD5Mismatch:
		apiErr = ErrSSECustomerKeyMD5Mismatch
	case errObjectTampered:
		apiErr = ErrObjectTampered
	case errEncryptedObject:
		apiErr = ErrSSEEncryptedObject
	case errInvalidSSEParameters:
		apiErr = ErrInvalidSSECustomerParameters
	case crypto.ErrInvalidCustomerKey, crypto.ErrSecretKeyMismatch:
		apiErr = ErrAccessDenied // no access without correct key
	case crypto.ErrIncompatibleEncryptionMethod:
		apiErr = ErrIncompatibleEncryptionMethod
	case errKMSNotConfigured:
		apiErr = ErrKMSNotConfigured
	case crypto.ErrKMSAuthLogin:
		apiErr = ErrKMSAuthFailure
	case errOperationTimedOut, context.Canceled, context.DeadlineExceeded:
		apiErr = ErrOperationTimedOut
	}
	switch err {
	case s3select.ErrBusy:
		apiErr = ErrBusy
	case s3select.ErrUnauthorizedAccess:
		apiErr = ErrUnauthorizedAccess
	case s3select.ErrExpressionTooLong:
		apiErr = ErrExpressionTooLong
	case s3select.ErrIllegalSQLFunctionArgument:
		apiErr = ErrIllegalSQLFunctionArgument
	case s3select.ErrInvalidKeyPath:
		apiErr = ErrInvalidKeyPath
	case s3select.ErrInvalidCompressionFormat:
		apiErr = ErrInvalidCompressionFormat
	case s3select.ErrInvalidFileHeaderInfo:
		apiErr = ErrInvalidFileHeaderInfo
	case s3select.ErrInvalidJSONType:
		apiErr = ErrInvalidJSONType
	case s3select.ErrInvalidQuoteFields:
		apiErr = ErrInvalidQuoteFields
	case s3select.ErrInvalidRequestParameter:
		apiErr = ErrInvalidRequestParameter
	case s3select.ErrInvalidDataType:
		apiErr = ErrInvalidDataType
	case s3select.ErrInvalidTextEncoding:
		apiErr = ErrInvalidTextEncoding
	case s3select.ErrInvalidTableAlias:
		apiErr = ErrInvalidTableAlias
	case s3select.ErrMissingRequiredParameter:
		apiErr = ErrMissingRequiredParameter
	case s3select.ErrObjectSerializationConflict:
		apiErr = ErrObjectSerializationConflict
	case s3select.ErrUnsupportedSQLOperation:
		apiErr = ErrUnsupportedSQLOperation
	case s3select.ErrUnsupportedSQLStructure:
		apiErr = ErrUnsupportedSQLStructure
	case s3select.ErrUnsupportedSyntax:
		apiErr = ErrUnsupportedSyntax
	case s3select.ErrUnsupportedRangeHeader:
		apiErr = ErrUnsupportedRangeHeader
	case s3select.ErrLexerInvalidChar:
		apiErr = ErrLexerInvalidChar
	case s3select.ErrLexerInvalidOperator:
		apiErr = ErrLexerInvalidOperator
	case s3select.ErrLexerInvalidLiteral:
		apiErr = ErrLexerInvalidLiteral
	case s3select.ErrLexerInvalidIONLiteral:
		apiErr = ErrLexerInvalidIONLiteral
	case s3select.ErrParseExpectedDatePart:
		apiErr = ErrParseExpectedDatePart
	case s3select.ErrParseExpectedKeyword:
		apiErr = ErrParseExpectedKeyword
	case s3select.ErrParseExpectedTokenType:
		apiErr = ErrParseExpectedTokenType
	case s3select.ErrParseExpected2TokenTypes:
		apiErr = ErrParseExpected2TokenTypes
	case s3select.ErrParseExpectedNumber:
		apiErr = ErrParseExpectedNumber
	case s3select.ErrParseExpectedRightParenBuiltinFunctionCall:
		apiErr = ErrParseExpectedRightParenBuiltinFunctionCall
	case s3select.ErrParseExpectedTypeName:
		apiErr = ErrParseExpectedTypeName
	case s3select.ErrParseExpectedWhenClause:
		apiErr = ErrParseExpectedWhenClause
	case s3select.ErrParseUnsupportedToken:
		apiErr = ErrParseUnsupportedToken
	case s3select.ErrParseUnsupportedLiteralsGroupBy:
		apiErr = ErrParseUnsupportedLiteralsGroupBy
	case s3select.ErrParseExpectedMember:
		apiErr = ErrParseExpectedMember
	case s3select.ErrParseUnsupportedSelect:
		apiErr = ErrParseUnsupportedSelect
	case s3select.ErrParseUnsupportedCase:
		apiErr = ErrParseUnsupportedCase
	case s3select.ErrParseUnsupportedCaseClause:
		apiErr = ErrParseUnsupportedCaseClause
	case s3select.ErrParseUnsupportedAlias:
		apiErr = ErrParseUnsupportedAlias
	case s3select.ErrParseUnsupportedSyntax:
		apiErr = ErrParseUnsupportedSyntax
	case s3select.ErrParseUnknownOperator:
		apiErr = ErrParseUnknownOperator
	case s3select.ErrParseMissingIdentAfterAt:
		apiErr = ErrParseMissingIdentAfterAt
	case s3select.ErrParseUnexpectedOperator:
		apiErr = ErrParseUnexpectedOperator
	case s3select.ErrParseUnexpectedTerm:
		apiErr = ErrParseUnexpectedTerm
	case s3select.ErrParseUnexpectedToken:
		apiErr = ErrParseUnexpectedToken
	case s3select.ErrParseUnexpectedKeyword:
		apiErr = ErrParseUnexpectedKeyword
	case s3select.ErrParseExpectedExpression:
		apiErr = ErrParseExpectedExpression
	case s3select.ErrParseExpectedLeftParenAfterCast:
		apiErr = ErrParseExpectedLeftParenAfterCast
	case s3select.ErrParseExpectedLeftParenValueConstructor:
		apiErr = ErrParseExpectedLeftParenValueConstructor
	case s3select.ErrParseExpectedLeftParenBuiltinFunctionCall:
		apiErr = ErrParseExpectedLeftParenBuiltinFunctionCall
	case s3select.ErrParseExpectedArgumentDelimiter:
		apiErr = ErrParseExpectedArgumentDelimiter
	case s3select.ErrParseCastArity:
		apiErr = ErrParseCastArity
	case s3select.ErrParseInvalidTypeParam:
		apiErr = ErrParseInvalidTypeParam
	case s3select.ErrParseEmptySelect:
		apiErr = ErrParseEmptySelect
	case s3select.ErrParseSelectMissingFrom:
		apiErr = ErrParseSelectMissingFrom
	case s3select.ErrParseExpectedIdentForGroupName:
		apiErr = ErrParseExpectedIdentForGroupName
	case s3select.ErrParseExpectedIdentForAlias:
		apiErr = ErrParseExpectedIdentForAlias
	case s3select.ErrParseUnsupportedCallWithStar:
		apiErr = ErrParseUnsupportedCallWithStar
	case s3select.ErrParseNonUnaryAgregateFunctionCall:
		apiErr = ErrParseNonUnaryAgregateFunctionCall
	case s3select.ErrParseMalformedJoin:
		apiErr = ErrParseMalformedJoin
	case s3select.ErrParseExpectedIdentForAt:
		apiErr = ErrParseExpectedIdentForAt
	case s3select.ErrParseAsteriskIsNotAloneInSelectList:
		apiErr = ErrParseAsteriskIsNotAloneInSelectList
	case s3select.ErrParseCannotMixSqbAndWildcardInSelectList:
		apiErr = ErrParseCannotMixSqbAndWildcardInSelectList
	case s3select.ErrParseInvalidContextForWildcardInSelectList:
		apiErr = ErrParseInvalidContextForWildcardInSelectList
	case s3select.ErrIncorrectSQLFunctionArgumentType:
		apiErr = ErrIncorrectSQLFunctionArgumentType
	case s3select.ErrValueParseFailure:
		apiErr = ErrValueParseFailure
	case s3select.ErrIntegerOverflow:
		apiErr = ErrIntegerOverflow
	case s3select.ErrLikeInvalidInputs:
		apiErr = ErrLikeInvalidInputs
	case s3select.ErrCastFailed:
		apiErr = ErrCastFailed
	case s3select.ErrInvalidCast:
		apiErr = ErrInvalidCast
	case s3select.ErrEvaluatorInvalidTimestampFormatPattern:
		apiErr = ErrEvaluatorInvalidTimestampFormatPattern
	case s3select.ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing:
		apiErr = ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing
	case s3select.ErrEvaluatorTimestampFormatPatternDuplicateFields:
		apiErr = ErrEvaluatorTimestampFormatPatternDuplicateFields
	case s3select.ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch:
		apiErr = ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch
	case s3select.ErrEvaluatorUnterminatedTimestampFormatPatternToken:
		apiErr = ErrEvaluatorUnterminatedTimestampFormatPatternToken
	case s3select.ErrEvaluatorInvalidTimestampFormatPatternToken:
		apiErr = ErrEvaluatorInvalidTimestampFormatPatternToken
	case s3select.ErrEvaluatorInvalidTimestampFormatPatternSymbol:
		apiErr = ErrEvaluatorInvalidTimestampFormatPatternSymbol
	case s3select.ErrEvaluatorBindingDoesNotExist:
		apiErr = ErrEvaluatorBindingDoesNotExist
	case s3select.ErrMissingHeaders:
		apiErr = ErrMissingHeaders
	case format.ErrParseInvalidPathComponent:
		apiErr = ErrMissingHeaders
	case format.ErrInvalidColumnIndex:
		apiErr = ErrInvalidColumnIndex
	}

	// Compression errors
	switch err {
	case errInvalidDecompressedSize:
		apiErr = ErrInvalidDecompressedSize
	}

	if apiErr != ErrNone {
		// If there was a match in the above switch case.
		return apiErr
	}

	// etcd specific errors, a key is always a bucket for us return
	// ErrNoSuchBucket in such a case.
	if err == dns.ErrNoEntriesFound {
		return ErrNoSuchBucket
	}

	switch err.(type) {
	case StorageFull:
		apiErr = ErrStorageFull
	case hash.BadDigest:
		apiErr = ErrBadDigest
	case AllAccessDisabled:
		apiErr = ErrAllAccessDisabled
	case IncompleteBody:
		apiErr = ErrIncompleteBody
	case ObjectExistsAsDirectory:
		apiErr = ErrObjectExistsAsDirectory
	case PrefixAccessDenied:
		apiErr = ErrAccessDenied
	case BucketNameInvalid:
		apiErr = ErrInvalidBucketName
	case BucketNotFound:
		apiErr = ErrNoSuchBucket
	case BucketAlreadyOwnedByYou:
		apiErr = ErrBucketAlreadyOwnedByYou
	case BucketNotEmpty:
		apiErr = ErrBucketNotEmpty
	case BucketAlreadyExists:
		apiErr = ErrBucketAlreadyExists
	case BucketExists:
		apiErr = ErrBucketAlreadyOwnedByYou
	case ObjectNotFound:
		apiErr = ErrNoSuchKey
	case ObjectAlreadyExists:
		apiErr = ErrMethodNotAllowed
	case ObjectNameInvalid:
		apiErr = ErrInvalidObjectName
	case InvalidUploadID:
		apiErr = ErrNoSuchUpload
	case InvalidPart:
		apiErr = ErrInvalidPart
	case InsufficientWriteQuorum:
		apiErr = ErrWriteQuorum
	case InsufficientReadQuorum:
		apiErr = ErrReadQuorum
	case UnsupportedDelimiter:
		apiErr = ErrNotImplemented
	case InvalidMarkerPrefixCombination:
		apiErr = ErrNotImplemented
	case InvalidUploadIDKeyCombination:
		apiErr = ErrNotImplemented
	case MalformedUploadID:
		apiErr = ErrNoSuchUpload
	case PartTooSmall:
		apiErr = ErrEntityTooSmall
	case SignatureDoesNotMatch:
		apiErr = ErrSignatureDoesNotMatch
	case hash.SHA256Mismatch:
		apiErr = ErrContentSHA256Mismatch
	case ObjectTooLarge:
		apiErr = ErrEntityTooLarge
	case ObjectTooSmall:
		apiErr = ErrEntityTooSmall
	case NotImplemented:
		apiErr = ErrNotImplemented
	case PartTooBig:
		apiErr = ErrEntityTooLarge
	case UnsupportedMetadata:
		apiErr = ErrUnsupportedMetadata
	case BucketPolicyNotFound:
		apiErr = ErrNoSuchBucketPolicy
	case *event.ErrInvalidEventName:
		apiErr = ErrEventNotification
	case *event.ErrInvalidARN:
		apiErr = ErrARNNotification
	case *event.ErrARNNotFound:
		apiErr = ErrARNNotification
	case *event.ErrUnknownRegion:
		apiErr = ErrRegionNotification
	case *event.ErrInvalidFilterName:
		apiErr = ErrFilterNameInvalid
	case *event.ErrFilterNamePrefix:
		apiErr = ErrFilterNamePrefix
	case *event.ErrFilterNameSuffix:
		apiErr = ErrFilterNameSuffix
	case *event.ErrInvalidFilterValue:
		apiErr = ErrFilterValueInvalid
	case *event.ErrDuplicateEventName:
		apiErr = ErrOverlappingConfigs
	case *event.ErrDuplicateQueueConfiguration:
		apiErr = ErrOverlappingFilterNotification
	case *event.ErrUnsupportedConfiguration:
		apiErr = ErrUnsupportedNotification
	case BackendDown:
		apiErr = ErrBackendDown
	case crypto.Error:
		apiErr = ErrObjectTampered
	default:
		apiErr = ErrInternalError
		// Make sure to log the errors which we cannot translate
		// to a meaningful S3 API errors. This is added to aid in
		// debugging unexpected/unhandled errors.
		logger.LogIf(ctx, err)
	}

	return apiErr
}

// getAPIError provides API Error for input API error code.
func getAPIError(code APIErrorCode) APIError {
	if apiErr, ok := errorCodeResponse[code]; ok {
		return apiErr
	}
	return errorCodeResponse[ErrInternalError]
}

// getErrorResponse gets in standard error and resource value and
// provides a encodable populated response values
func getAPIErrorResponse(err APIError, resource, requestid string) APIErrorResponse {
	return APIErrorResponse{
		Code:      err.Code,
		Message:   err.Description,
		Resource:  resource,
		RequestID: requestid,
		HostID:    "3L137",
	}
}
