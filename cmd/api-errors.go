/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"google.golang.org/api/googleapi"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/config/dns"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/replication"

	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/bucket/versioning"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
	Key            string
}

// APIErrorResponse - error response format
type APIErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string
	Message    string
	Key        string `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName string `xml:"BucketName,omitempty" json:"BucketName,omitempty"`
	Resource   string
	Region     string `xml:"Region,omitempty" json:"Region,omitempty"`
	RequestID  string `xml:"RequestId" json:"RequestId"`
	HostID     string `xml:"HostId" json:"HostId"`
}

// APIErrorCode type of error status.
type APIErrorCode int

//go:generate stringer -type=APIErrorCode -trimprefix=Err $GOFILE

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrNone APIErrorCode = iota
	ErrAccessDenied
	ErrBadDigest
	ErrEntityTooSmall
	ErrEntityTooLarge
	ErrPolicyTooLarge
	ErrIncompleteBody
	ErrInternalError
	ErrInvalidAccessKeyID
	ErrInvalidBucketName
	ErrInvalidDigest
	ErrInvalidRange
	ErrInvalidRangePartNumber
	ErrInvalidCopyPartRange
	ErrInvalidCopyPartRangeSource
	ErrInvalidMaxKeys
	ErrInvalidEncodingMethod
	ErrInvalidMaxUploads
	ErrInvalidMaxParts
	ErrInvalidPartNumberMarker
	ErrInvalidPartNumber
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
	ErrMissingSecurityHeader
	ErrNoSuchBucket
	ErrNoSuchBucketPolicy
	ErrNoSuchBucketLifecycle
	ErrNoSuchLifecycleConfiguration
	ErrNoSuchBucketSSEConfig
	ErrNoSuchCORSConfiguration
	ErrNoSuchWebsiteConfiguration
	ErrReplicationConfigurationNotFoundError
	ErrRemoteDestinationNotFoundError
	ErrReplicationDestinationMissingLock
	ErrRemoteTargetNotFoundError
	ErrReplicationRemoteConnectionError
	ErrBucketRemoteIdenticalToSource
	ErrBucketRemoteAlreadyExists
	ErrBucketRemoteLabelInUse
	ErrBucketRemoteArnTypeInvalid
	ErrBucketRemoteArnInvalid
	ErrBucketRemoteRemoveDisallowed
	ErrRemoteTargetNotVersionedError
	ErrReplicationSourceNotVersionedError
	ErrReplicationNeedsVersioningError
	ErrReplicationBucketNeedsVersioningError
	ErrObjectRestoreAlreadyInProgress
	ErrNoSuchKey
	ErrNoSuchUpload
	ErrInvalidVersionID
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
	ErrInvalidServiceS3
	ErrInvalidServiceSTS
	ErrInvalidRequestVersion
	ErrMissingSignTag
	ErrMissingSignHeadersTag
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
	ErrKeyTooLongError
	ErrInvalidBucketObjectLockConfiguration
	ErrObjectLockConfigurationNotFound
	ErrObjectLockConfigurationNotAllowed
	ErrNoSuchObjectLockConfiguration
	ErrObjectLocked
	ErrInvalidRetentionDate
	ErrPastObjectLockRetainDate
	ErrUnknownWORMModeDirective
	ErrBucketTaggingNotFound
	ErrObjectLockInvalidHeaders
	ErrInvalidTagDirective
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

	// MinIO extended errors.
	ErrReadQuorum
	ErrWriteQuorum
	ErrParentIsObject
	ErrStorageFull
	ErrRequestBodyParse
	ErrObjectExistsAsDirectory
	ErrInvalidObjectName
	ErrInvalidObjectNamePrefixSlash
	ErrInvalidResourceName
	ErrServerNotInitialized
	ErrOperationTimedOut
	ErrClientDisconnected
	ErrOperationMaxedOut
	ErrInvalidRequest
	// MinIO storage class error codes
	ErrInvalidStorageClass
	ErrBackendDown
	// Add new extended error codes here.
	// Please open a https://github.com/minio/minio/issues before adding
	// new error codes here.

	ErrMalformedJSON
	ErrAdminNoSuchUser
	ErrAdminNoSuchGroup
	ErrAdminGroupNotEmpty
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
	// Bucket Quota error codes
	ErrAdminBucketQuotaExceeded
	ErrAdminNoSuchQuotaConfiguration

	ErrHealNotImplemented
	ErrHealNoSuchProcess
	ErrHealInvalidClientToken
	ErrHealMissingBucket
	ErrHealAlreadyRunning
	ErrHealOverlappingPaths
	ErrIncorrectContinuationToken

	// S3 Select Errors
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
	ErrAddUserInvalidArgument
	ErrAdminAccountNotEligible
	ErrAccountNotEligible
	ErrServiceAccountNotFound
	ErrPostPolicyConditionInvalidFormat
)

type errorCodeMap map[APIErrorCode]APIError

func (e errorCodeMap) ToAPIErrWithErr(errCode APIErrorCode, err error) APIError {
	apiErr, ok := e[errCode]
	if !ok {
		apiErr = e[ErrInternalError]
	}
	if err != nil {
		apiErr.Description = fmt.Sprintf("%s (%s)", apiErr.Description, err)
	}
	if globalServerRegion != "" {
		switch errCode {
		case ErrAuthorizationHeaderMalformed:
			apiErr.Description = fmt.Sprintf("The authorization header is malformed; the region is wrong; expecting '%s'.", globalServerRegion)
			return apiErr
		}
	}
	return apiErr
}

func (e errorCodeMap) ToAPIErr(errCode APIErrorCode) APIError {
	return e.ToAPIErrWithErr(errCode, nil)
}

// error code to APIError structure, these fields carry respective
// descriptions for all the error responses.
var errorCodes = errorCodeMap{
	ErrInvalidCopyDest: {
		Code:           "InvalidRequest",
		Description:    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidCopyDest",
	},
	ErrInvalidCopySource: {
		Code:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidCopySource",
	},
	ErrInvalidMetadataDirective: {
		Code:           "InvalidArgument",
		Description:    "Unknown metadata directive.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidMetadataDirective",
	},
	ErrInvalidStorageClass: {
		Code:           "InvalidStorageClass",
		Description:    "Invalid storage class.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidStorageClass",
	},
	ErrInvalidRequestBody: {
		Code:           "InvalidArgument",
		Description:    "Body shouldn't be set for this request.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidRequestBody",
	},
	ErrInvalidMaxUploads: {
		Code:           "InvalidArgument",
		Description:    "Argument max-uploads must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidMaxUploads",
	},
	ErrInvalidMaxKeys: {
		Code:           "InvalidArgument",
		Description:    "Argument maxKeys must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidMaxKeys",
	},
	ErrInvalidEncodingMethod: {
		Code:           "InvalidArgument",
		Description:    "Invalid Encoding Method specified in Request",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidEncodingMethod",
	},
	ErrInvalidMaxParts: {
		Code:           "InvalidArgument",
		Description:    "Argument max-parts must be an integer between 0 and 2147483647",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidMaxParts",
	},
	ErrInvalidPartNumberMarker: {
		Code:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidPartNumberMarker",
	},
	ErrInvalidPartNumber: {
		Code:           "InvalidPartNumber",
		Description:    "The requested partnumber is not satisfiable",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
		Key:            "ErrInvalidPartNumber",
	},
	ErrInvalidPolicyDocument: {
		Code:           "InvalidPolicyDocument",
		Description:    "The content of the form does not meet the conditions specified in the policy document.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidPolicyDocument",
	},
	ErrAccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied.",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrAccessDenied",
	},
	ErrBadDigest: {
		Code:           "BadDigest",
		Description:    "The Content-Md5 you specified did not match what we received.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBadDigest",
	},
	ErrEntityTooSmall: {
		Code:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEntityTooSmall",
	},
	ErrEntityTooLarge: {
		Code:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEntityTooLarge",
	},
	ErrPolicyTooLarge: {
		Code:           "PolicyTooLarge",
		Description:    "Policy exceeds the maximum allowed document size.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrPolicyTooLarge",
	},
	ErrIncompleteBody: {
		Code:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrIncompleteBody",
	},
	ErrInternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
		Key:            "ErrInternalError",
	},
	ErrInvalidAccessKeyID: {
		Code:           "InvalidAccessKeyId",
		Description:    "The Access Key Id you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrInvalidAccessKeyID",
	},
	ErrInvalidBucketName: {
		Code:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidBucketName",
	},
	ErrInvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-Md5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidDigest",
	},
	ErrInvalidRange: {
		Code:           "InvalidRange",
		Description:    "The requested range is not satisfiable",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
		Key:            "ErrInvalidRange",
	},
	ErrInvalidRangePartNumber: {
		Code:           "InvalidRequest",
		Description:    "Cannot specify both Range header and partNumber query parameter",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedXML: {
		Code:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedXML",
	},
	ErrMissingContentLength: {
		Code:           "MissingContentLength",
		Description:    "You must provide the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusLengthRequired,
		Key:            "ErrMissingContentLength",
	},
	ErrMissingContentMD5: {
		Code:           "MissingContentMD5",
		Description:    "Missing required header for this request: Content-Md5.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingContentMD5",
	},
	ErrMissingSecurityHeader: {
		Code:           "MissingSecurityHeader",
		Description:    "Your request was missing a required header",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingSecurityHeader",
	},
	ErrMissingRequestBodyError: {
		Code:           "MissingRequestBodyError",
		Description:    "Request body is empty.",
		HTTPStatusCode: http.StatusLengthRequired,
		Key:            "ErrMissingRequestBodyError",
	},
	ErrNoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchBucket",
	},
	ErrNoSuchBucketPolicy: {
		Code:           "NoSuchBucketPolicy",
		Description:    "The bucket policy does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchBucketPolicy",
	},
	ErrNoSuchBucketLifecycle: {
		Code:           "NoSuchBucketLifecycle",
		Description:    "The bucket lifecycle configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchBucketLifecycle",
	},
	ErrNoSuchLifecycleConfiguration: {
		Code:           "NoSuchLifecycleConfiguration",
		Description:    "The lifecycle configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchLifecycleConfiguration",
	},
	ErrNoSuchBucketSSEConfig: {
		Code:           "ServerSideEncryptionConfigurationNotFoundError",
		Description:    "The server side encryption configuration was not found",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchBucketSSEConfig",
	},
	ErrNoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchKey",
	},
	ErrNoSuchUpload: {
		Code:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchUpload",
	},
	ErrInvalidVersionID: {
		Code:           "InvalidArgument",
		Description:    "Invalid version id specified",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidVersionID",
	},
	ErrNoSuchVersion: {
		Code:           "NoSuchVersion",
		Description:    "The specified version does not exist.",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchVersion",
	},
	ErrNotImplemented: {
		Code:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented",
		HTTPStatusCode: http.StatusNotImplemented,
		Key:            "ErrNotImplemented",
	},
	ErrPreconditionFailed: {
		Code:           "PreconditionFailed",
		Description:    "At least one of the pre-conditions you specified did not hold",
		HTTPStatusCode: http.StatusPreconditionFailed,
		Key:            "ErrPreconditionFailed",
	},
	ErrRequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrRequestTimeTooSkewed",
	},
	ErrSignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrSignatureDoesNotMatch",
	},
	ErrMethodNotAllowed: {
		Code:           "MethodNotAllowed",
		Description:    "The specified method is not allowed against this resource.",
		HTTPStatusCode: http.StatusMethodNotAllowed,
		Key:            "ErrMethodNotAllowed",
	},
	ErrInvalidPart: {
		Code:           "InvalidPart",
		Description:    "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidPart",
	},
	ErrInvalidPartOrder: {
		Code:           "InvalidPartOrder",
		Description:    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidPartOrder",
	},
	ErrInvalidObjectState: {
		Code:           "InvalidObjectState",
		Description:    "The operation is not valid for the current state of the object.",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrInvalidObjectState",
	},
	ErrAuthorizationHeaderMalformed: {
		Code:           "AuthorizationHeaderMalformed",
		Description:    "The authorization header is malformed; the region is wrong; expecting 'us-east-1'.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAuthorizationHeaderMalformed",
	},
	ErrMalformedPOSTRequest: {
		Code:           "MalformedPOSTRequest",
		Description:    "The body of your POST request is not well-formed multipart/form-data.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedPOSTRequest",
	},
	ErrPOSTFileRequired: {
		Code:           "InvalidArgument",
		Description:    "POST requires exactly one file upload per request.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrPOSTFileRequired",
	},
	ErrSignatureVersionNotSupported: {
		Code:           "InvalidRequest",
		Description:    "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrSignatureVersionNotSupported",
	},
	ErrBucketNotEmpty: {
		Code:           "BucketNotEmpty",
		Description:    "The bucket you tried to delete is not empty",
		HTTPStatusCode: http.StatusConflict,
		Key:            "ErrBucketNotEmpty",
	},
	ErrBucketAlreadyExists: {
		Code:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		HTTPStatusCode: http.StatusConflict,
		Key:            "ErrBucketAlreadyExists",
	},
	ErrAllAccessDisabled: {
		Code:           "AllAccessDisabled",
		Description:    "All access to this bucket has been disabled.",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrAllAccessDisabled",
	},
	ErrMalformedPolicy: {
		Code:           "MalformedPolicy",
		Description:    "Policy has invalid resource.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedPolicy",
	},
	ErrMissingFields: {
		Code:           "MissingFields",
		Description:    "Missing fields in request.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingFields",
	},
	ErrMissingCredTag: {
		Code:           "InvalidRequest",
		Description:    "Missing Credential field for this request.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingCredTag",
	},
	ErrCredMalformed: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrCredMalformed",
	},
	ErrMalformedDate: {
		Code:           "MalformedDate",
		Description:    "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedDate",
	},
	ErrMalformedPresignedDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\"",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedPresignedDate",
	},
	ErrMalformedCredentialDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; incorrect date format. This date in the credential must be in the format \"yyyyMMdd\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedCredentialDate",
	},
	ErrInvalidRegion: {
		Code:           "InvalidRegion",
		Description:    "Region does not match.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidRegion",
	},
	ErrInvalidServiceS3: {
		Code:           "AuthorizationParametersError",
		Description:    "Error parsing the Credential/X-Amz-Credential parameter; incorrect service. This endpoint belongs to \"s3\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidServiceS3",
	},
	ErrInvalidServiceSTS: {
		Code:           "AuthorizationParametersError",
		Description:    "Error parsing the Credential parameter; incorrect service. This endpoint belongs to \"sts\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidServiceSTS",
	},
	ErrInvalidRequestVersion: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; incorrect terminal. This endpoint uses \"aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidRequestVersion",
	},
	ErrMissingSignTag: {
		Code:           "AccessDenied",
		Description:    "Signature header missing Signature field.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingSignTag",
	},
	ErrMissingSignHeadersTag: {
		Code:           "InvalidArgument",
		Description:    "Signature header missing SignedHeaders field.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingSignHeadersTag",
	},
	ErrMalformedExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires should be a number",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedExpires",
	},
	ErrNegativeExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be non-negative",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrNegativeExpires",
	},
	ErrAuthHeaderEmpty: {
		Code:           "InvalidArgument",
		Description:    "Authorization header is invalid -- one and only one ' ' (space) required.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAuthHeaderEmpty",
	},
	ErrMissingDateHeader: {
		Code:           "AccessDenied",
		Description:    "AWS authentication requires a valid Date or x-amz-date header",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingDateHeader",
	},
	ErrInvalidQuerySignatureAlgo: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidQuerySignatureAlgo",
	},
	ErrExpiredPresignRequest: {
		Code:           "AccessDenied",
		Description:    "Request has expired",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrExpiredPresignRequest",
	},
	ErrRequestNotReadyYet: {
		Code:           "AccessDenied",
		Description:    "Request is not valid yet",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrRequestNotReadyYet",
	},
	ErrSlowDown: {
		Code:           "SlowDown",
		Description:    "Please reduce your request",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrSlowDown",
	},
	ErrInvalidPrefixMarker: {
		Code:           "InvalidPrefixMarker",
		Description:    "Invalid marker prefix combination",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidPrefixMarker",
	},
	ErrBadRequest: {
		Code:           "BadRequest",
		Description:    "400 BadRequest",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBadRequest",
	},
	ErrKeyTooLongError: {
		Code:           "KeyTooLongError",
		Description:    "Your key is too long",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrKeyTooLongError",
	},
	ErrUnsignedHeaders: {
		Code:           "AccessDenied",
		Description:    "There were headers present in the request which were not signed",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsignedHeaders",
	},
	ErrInvalidQueryParams: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidQueryParams",
	},
	ErrBucketAlreadyOwnedByYou: {
		Code:           "BucketAlreadyOwnedByYou",
		Description:    "Your previous request to create the named bucket succeeded and you already own it.",
		HTTPStatusCode: http.StatusConflict,
		Key:            "ErrBucketAlreadyOwnedByYou",
	},
	ErrInvalidDuration: {
		Code:           "InvalidDuration",
		Description:    "Duration provided in the request is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidDuration",
	},
	ErrInvalidBucketObjectLockConfiguration: {
		Code:           "InvalidRequest",
		Description:    "Bucket is missing ObjectLockConfiguration",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidBucketObjectLockConfiguration",
	},
	ErrBucketTaggingNotFound: {
		Code:           "NoSuchTagSet",
		Description:    "The TagSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrBucketTaggingNotFound",
	},
	ErrObjectLockConfigurationNotFound: {
		Code:           "ObjectLockConfigurationNotFoundError",
		Description:    "Object Lock configuration does not exist for this bucket",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrObjectLockConfigurationNotFound",
	},
	ErrObjectLockConfigurationNotAllowed: {
		Code:           "InvalidBucketState",
		Description:    "Object Lock configuration cannot be enabled on existing buckets",
		HTTPStatusCode: http.StatusConflict,
		Key:            "ErrObjectLockConfigurationNotAllowed",
	},
	ErrNoSuchCORSConfiguration: {
		Code:           "NoSuchCORSConfiguration",
		Description:    "The CORS configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchCORSConfiguration",
	},
	ErrNoSuchWebsiteConfiguration: {
		Code:           "NoSuchWebsiteConfiguration",
		Description:    "The specified bucket does not have a website configuration",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrNoSuchWebsiteConfiguration",
	},
	ErrReplicationConfigurationNotFoundError: {
		Code:           "ReplicationConfigurationNotFoundError",
		Description:    "The replication configuration was not found",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrReplicationConfigurationNotFoundError",
	},
	ErrRemoteDestinationNotFoundError: {
		Code:           "RemoteDestinationNotFoundError",
		Description:    "The remote destination bucket does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrRemoteDestinationNotFoundError",
	},
	ErrReplicationDestinationMissingLock: {
		Code:           "ReplicationDestinationMissingLockError",
		Description:    "The replication destination bucket does not have object locking enabled",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrReplicationDestinationMissingLock",
	},
	ErrRemoteTargetNotFoundError: {
		Code:           "XMinioAdminRemoteTargetNotFoundError",
		Description:    "The remote target does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrRemoteTargetNotFoundError",
	},
	ErrReplicationRemoteConnectionError: {
		Code:           "XMinioAdminReplicationRemoteConnectionError",
		Description:    "Remote service connection error - please check remote service credentials and target bucket",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrReplicationRemoteConnectionError",
	},
	ErrBucketRemoteIdenticalToSource: {
		Code:           "XMinioAdminRemoteIdenticalToSource",
		Description:    "The remote target cannot be identical to source",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBucketRemoteIdenticalToSource",
	},
	ErrBucketRemoteAlreadyExists: {
		Code:           "XMinioAdminBucketRemoteAlreadyExists",
		Description:    "The remote target already exists",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBucketRemoteAlreadyExists",
	},
	ErrBucketRemoteLabelInUse: {
		Code:           "XMinioAdminBucketRemoteLabelInUse",
		Description:    "The remote target with this label already exists",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBucketRemoteLabelInUse",
	},
	ErrBucketRemoteRemoveDisallowed: {
		Code:           "XMinioAdminRemoteRemoveDisallowed",
		Description:    "This ARN is in use by an existing configuration",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBucketRemoteRemoveDisallowed",
	},
	ErrBucketRemoteArnTypeInvalid: {
		Code:           "XMinioAdminRemoteARNTypeInvalid",
		Description:    "The bucket remote ARN type is not valid",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBucketRemoteArnTypeInvalid",
	},
	ErrBucketRemoteArnInvalid: {
		Code:           "XMinioAdminRemoteArnInvalid",
		Description:    "The bucket remote ARN does not have correct format",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrBucketRemoteArnInvalid",
	},
	ErrRemoteTargetNotVersionedError: {
		Code:           "RemoteTargetNotVersionedError",
		Description:    "The remote target does not have versioning enabled",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrRemoteTargetNotVersionedError",
	},
	ErrReplicationSourceNotVersionedError: {
		Code:           "ReplicationSourceNotVersionedError",
		Description:    "The replication source does not have versioning enabled",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrReplicationSourceNotVersionedError",
	},
	ErrReplicationNeedsVersioningError: {
		Code:           "InvalidRequest",
		Description:    "Versioning must be 'Enabled' on the bucket to apply a replication configuration",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrReplicationNeedsVersioningError",
	},
	ErrReplicationBucketNeedsVersioningError: {
		Code:           "InvalidRequest",
		Description:    "Versioning must be 'Enabled' on the bucket to add a replication target",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrReplicationBucketNeedsVersioningError",
	},
	ErrNoSuchObjectLockConfiguration: {
		Code:           "NoSuchObjectLockConfiguration",
		Description:    "The specified object does not have a ObjectLock configuration",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrNoSuchObjectLockConfiguration",
	},
	ErrObjectLocked: {
		Code:           "InvalidRequest",
		Description:    "Object is WORM protected and cannot be overwritten",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrObjectLocked",
	},
	ErrInvalidRetentionDate: {
		Code:           "InvalidRequest",
		Description:    "Date must be provided in ISO 8601 format",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidRetentionDate",
	},
	ErrPastObjectLockRetainDate: {
		Code:           "InvalidRequest",
		Description:    "the retain until date must be in the future",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrPastObjectLockRetainDate",
	},
	ErrUnknownWORMModeDirective: {
		Code:           "InvalidRequest",
		Description:    "unknown wormMode directive",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnknownWORMModeDirective",
	},
	ErrObjectLockInvalidHeaders: {
		Code:           "InvalidRequest",
		Description:    "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrObjectLockInvalidHeaders",
	},
	ErrObjectRestoreAlreadyInProgress: {
		Code:           "RestoreAlreadyInProgress",
		Description:    "Object restore is already in progress",
		HTTPStatusCode: http.StatusConflict,
		Key:            "ErrObjectRestoreAlreadyInProgress",
	},
	/// Bucket notification related errors.
	ErrEventNotification: {
		Code:           "InvalidArgument",
		Description:    "A specified event is not supported for notifications.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEventNotification",
	},
	ErrARNNotification: {
		Code:           "InvalidArgument",
		Description:    "A specified destination ARN does not exist or is not well-formed. Verify the destination ARN.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrARNNotification",
	},
	ErrRegionNotification: {
		Code:           "InvalidArgument",
		Description:    "A specified destination is in a different region than the bucket. You must use a destination that resides in the same region as the bucket.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrRegionNotification",
	},
	ErrOverlappingFilterNotification: {
		Code:           "InvalidArgument",
		Description:    "An object key name filtering rule defined with overlapping prefixes, overlapping suffixes, or overlapping combinations of prefixes and suffixes for the same event types.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrOverlappingFilterNotification",
	},
	ErrFilterNameInvalid: {
		Code:           "InvalidArgument",
		Description:    "filter rule name must be either prefix or suffix",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrFilterNameInvalid",
	},
	ErrFilterNamePrefix: {
		Code:           "InvalidArgument",
		Description:    "Cannot specify more than one prefix rule in a filter.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrFilterNamePrefix",
	},
	ErrFilterNameSuffix: {
		Code:           "InvalidArgument",
		Description:    "Cannot specify more than one suffix rule in a filter.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrFilterNameSuffix",
	},
	ErrFilterValueInvalid: {
		Code:           "InvalidArgument",
		Description:    "Size of filter rule value cannot exceed 1024 bytes in UTF-8 representation",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrFilterValueInvalid",
	},
	ErrOverlappingConfigs: {
		Code:           "InvalidArgument",
		Description:    "Configurations overlap. Configurations on the same bucket cannot share a common event type.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrOverlappingConfigs",
	},
	ErrUnsupportedNotification: {
		Code:           "UnsupportedNotification",
		Description:    "MinIO server does not support Topic or Cloud Function based notifications.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedNotification",
	},
	ErrInvalidCopyPartRange: {
		Code:           "InvalidArgument",
		Description:    "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidCopyPartRange",
	},
	ErrInvalidCopyPartRangeSource: {
		Code:           "InvalidArgument",
		Description:    "Range specified is not valid for source object",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidCopyPartRangeSource",
	},
	ErrMetadataTooLarge: {
		Code:           "MetadataTooLarge",
		Description:    "Your metadata headers exceed the maximum allowed metadata size.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMetadataTooLarge",
	},
	ErrInvalidTagDirective: {
		Code:           "InvalidArgument",
		Description:    "Unknown tag directive.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidTagDirective",
	},
	ErrInvalidEncryptionMethod: {
		Code:           "InvalidRequest",
		Description:    "The encryption method specified is not supported",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidEncryptionMethod",
	},
	ErrInsecureSSECustomerRequest: {
		Code:           "InvalidRequest",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInsecureSSECustomerRequest",
	},
	ErrSSEMultipartEncrypted: {
		Code:           "InvalidRequest",
		Description:    "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrSSEMultipartEncrypted",
	},
	ErrSSEEncryptedObject: {
		Code:           "InvalidRequest",
		Description:    "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrSSEEncryptedObject",
	},
	ErrInvalidEncryptionParameters: {
		Code:           "InvalidRequest",
		Description:    "The encryption parameters are not applicable to this object.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidEncryptionParameters",
	},
	ErrInvalidSSECustomerAlgorithm: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidSSECustomerAlgorithm",
	},
	ErrInvalidSSECustomerKey: {
		Code:           "InvalidArgument",
		Description:    "The secret key was invalid for the specified algorithm.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidSSECustomerKey",
	},
	ErrMissingSSECustomerKey: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingSSECustomerKey",
	},
	ErrMissingSSECustomerKeyMD5: {
		Code:           "InvalidArgument",
		Description:    "Requests specifying Server Side Encryption with Customer provided keys must provide the client calculated MD5 of the secret key.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingSSECustomerKeyMD5",
	},
	ErrSSECustomerKeyMD5Mismatch: {
		Code:           "InvalidArgument",
		Description:    "The calculated MD5 hash of the key did not match the hash that was provided.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrSSECustomerKeyMD5Mismatch",
	},
	ErrInvalidSSECustomerParameters: {
		Code:           "InvalidArgument",
		Description:    "The provided encryption parameters did not match the ones used originally.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidSSECustomerParameters",
	},
	ErrIncompatibleEncryptionMethod: {
		Code:           "InvalidArgument",
		Description:    "Server side encryption specified with both SSE-C and SSE-S3 headers",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrIncompatibleEncryptionMethod",
	},
	ErrKMSNotConfigured: {
		Code:           "InvalidArgument",
		Description:    "Server side encryption specified but KMS is not configured",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrKMSNotConfigured",
	},
	ErrKMSAuthFailure: {
		Code:           "InvalidArgument",
		Description:    "Server side encryption specified but KMS authorization failed",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrKMSAuthFailure",
	},
	ErrNoAccessKey: {
		Code:           "AccessDenied",
		Description:    "No AWSAccessKey was presented",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrNoAccessKey",
	},
	ErrInvalidToken: {
		Code:           "InvalidTokenId",
		Description:    "The security token included in the request is invalid",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrInvalidToken",
	},

	/// S3 extensions.
	ErrContentSHA256Mismatch: {
		Code:           "XAmzContentSHA256Mismatch",
		Description:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrContentSHA256Mismatch",
	},

	/// MinIO extensions.
	ErrStorageFull: {
		Code:           "XMinioStorageFull",
		Description:    "Storage backend has reached its minimum free disk threshold. Please delete a few objects to proceed.",
		HTTPStatusCode: http.StatusInsufficientStorage,
		Key:            "ErrStorageFull",
	},
	ErrParentIsObject: {
		Code:           "XMinioParentIsObject",
		Description:    "Object-prefix is already an object, please choose a different object-prefix name.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParentIsObject",
	},
	ErrRequestBodyParse: {
		Code:           "XMinioRequestBodyParse",
		Description:    "The request body failed to parse.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrRequestBodyParse",
	},
	ErrObjectExistsAsDirectory: {
		Code:           "XMinioObjectExistsAsDirectory",
		Description:    "Object name already exists as a directory.",
		HTTPStatusCode: http.StatusConflict,
		Key:            "ErrObjectExistsAsDirectory",
	},
	ErrInvalidObjectName: {
		Code:           "XMinioInvalidObjectName",
		Description:    "Object name contains unsupported characters.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidObjectName",
	},
	ErrInvalidObjectNamePrefixSlash: {
		Code:           "XMinioInvalidObjectName",
		Description:    "Object name contains a leading slash.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidObjectNamePrefixSlash",
	},
	ErrInvalidResourceName: {
		Code:           "XMinioInvalidResourceName",
		Description:    "Resource name contains bad components such as \"..\" or \".\".",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidResourceName",
	},
	ErrServerNotInitialized: {
		Code:           "XMinioServerNotInitialized",
		Description:    "Server not initialized, please try again.",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrServerNotInitialized",
	},
	ErrMalformedJSON: {
		Code:           "XMinioMalformedJSON",
		Description:    "The JSON you provided was not well-formed or did not validate against our published format.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMalformedJSON",
	},
	ErrAdminNoSuchUser: {
		Code:           "XMinioAdminNoSuchUser",
		Description:    "The specified user does not exist.",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrAdminNoSuchUser",
	},
	ErrAdminNoSuchGroup: {
		Code:           "XMinioAdminNoSuchGroup",
		Description:    "The specified group does not exist.",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrAdminNoSuchGroup",
	},
	ErrAdminGroupNotEmpty: {
		Code:           "XMinioAdminGroupNotEmpty",
		Description:    "The specified group is not empty - cannot remove it.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminGroupNotEmpty",
	},
	ErrAdminNoSuchPolicy: {
		Code:           "XMinioAdminNoSuchPolicy",
		Description:    "The canned policy does not exist.",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrAdminNoSuchPolicy",
	},
	ErrAdminInvalidArgument: {
		Code:           "XMinioAdminInvalidArgument",
		Description:    "Invalid arguments specified.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminInvalidArgument",
	},
	ErrAdminInvalidAccessKey: {
		Code:           "XMinioAdminInvalidAccessKey",
		Description:    "The access key is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminInvalidAccessKey",
	},
	ErrAdminInvalidSecretKey: {
		Code:           "XMinioAdminInvalidSecretKey",
		Description:    "The secret key is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminInvalidSecretKey",
	},
	ErrAdminConfigNoQuorum: {
		Code:           "XMinioAdminConfigNoQuorum",
		Description:    "Configuration update failed because server quorum was not met",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrAdminConfigNoQuorum",
	},
	ErrAdminConfigTooLarge: {
		Code: "XMinioAdminConfigTooLarge",
		Description: fmt.Sprintf("Configuration data provided exceeds the allowed maximum of %d bytes",
			maxEConfigJSONSize),
		Key:            "ErrAdminConfigTooLarge",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigBadJSON: {
		Code:           "XMinioAdminConfigBadJSON",
		Description:    "JSON configuration provided is of incorrect format",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminConfigBadJSON",
	},
	ErrAdminConfigDuplicateKeys: {
		Code:           "XMinioAdminConfigDuplicateKeys",
		Description:    "JSON configuration provided has objects with duplicate keys",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminConfigDuplicateKeys",
	},
	ErrAdminConfigNotificationTargetsFailed: {
		Code:           "XMinioAdminNotificationTargetsTestFailed",
		Description:    "Configuration update failed due an unsuccessful attempt to connect to one or more notification servers",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminConfigNotificationTargetsFailed",
	},
	ErrAdminProfilerNotEnabled: {
		Code:           "XMinioAdminProfilerNotEnabled",
		Description:    "Unable to perform the requested operation because profiling is not enabled",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminProfilerNotEnabled",
	},
	ErrAdminCredentialsMismatch: {
		Code:           "XMinioAdminCredentialsMismatch",
		Description:    "Credentials in config mismatch with server environment variables",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrAdminCredentialsMismatch",
	},
	ErrAdminBucketQuotaExceeded: {
		Code:           "XMinioAdminBucketQuotaExceeded",
		Description:    "Bucket quota exceeded",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrAdminBucketQuotaExceeded",
	},
	ErrAdminNoSuchQuotaConfiguration: {
		Code:           "XMinioAdminNoSuchQuotaConfiguration",
		Description:    "The quota configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrAdminNoSuchQuotaConfiguration",
	},
	ErrInsecureClientRequest: {
		Code:           "XMinioInsecureClientRequest",
		Description:    "Cannot respond to plain-text request from TLS-encrypted server",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInsecureClientRequest",
	},
	ErrOperationTimedOut: {
		Code:           "RequestTimeout",
		Description:    "A timeout occurred while trying to lock a resource, please reduce your request rate",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrOperationTimedOut",
	},
	ErrClientDisconnected: {
		Code:           "ClientDisconnected",
		Description:    "Client disconnected before response was ready",
		HTTPStatusCode: 499, // No official code, use nginx value.
		Key:            "ErrClientDisconnected",
	},
	ErrOperationMaxedOut: {
		Code:           "SlowDown",
		Description:    "A timeout exceeded while waiting to proceed with the request, please reduce your request rate",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrOperationMaxedOut",
	},
	ErrUnsupportedMetadata: {
		Code:           "InvalidArgument",
		Description:    "Your metadata headers are not supported.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedMetadata",
	},
	ErrObjectTampered: {
		Code:           "XMinioObjectTampered",
		Description:    errObjectTampered.Error(),
		HTTPStatusCode: http.StatusPartialContent,
		Key:            "ErrObjectTampered",
	},
	ErrMaximumExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMaximumExpires",
	},

	// Generic Invalid-Request error. Should be used for response errors only for unlikely
	// corner case errors for which introducing new APIErrorCode is not worth it. LogIf()
	// should be used to log the error at the source of the error for debugging purposes.
	ErrInvalidRequest: {
		Code:           "InvalidRequest",
		Description:    "Invalid Request",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidRequest",
	},
	ErrHealNotImplemented: {
		Code:           "XMinioHealNotImplemented",
		Description:    "This server does not implement heal functionality.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrHealNotImplemented",
	},
	ErrHealNoSuchProcess: {
		Code:           "XMinioHealNoSuchProcess",
		Description:    "No such heal process is running on the server",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrHealNoSuchProcess",
	},
	ErrHealInvalidClientToken: {
		Code:           "XMinioHealInvalidClientToken",
		Description:    "Client token mismatch",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrHealInvalidClientToken",
	},
	ErrHealMissingBucket: {
		Code:           "XMinioHealMissingBucket",
		Description:    "A heal start request with a non-empty object-prefix parameter requires a bucket to be specified.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrHealMissingBucket",
	},
	ErrHealAlreadyRunning: {
		Code:           "XMinioHealAlreadyRunning",
		Description:    "",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrHealAlreadyRunning",
	},
	ErrHealOverlappingPaths: {
		Code:           "XMinioHealOverlappingPaths",
		Description:    "",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrHealOverlappingPaths",
	},
	ErrBackendDown: {
		Code:           "XMinioBackendDown",
		Description:    "Object storage backend is unreachable",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrBackendDown",
	},
	ErrIncorrectContinuationToken: {
		Code:           "InvalidArgument",
		Description:    "The continuation token provided is incorrect",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrIncorrectContinuationToken",
	},
	//S3 Select API Errors
	ErrEmptyRequestBody: {
		Code:           "EmptyRequestBody",
		Description:    "Request body cannot be empty.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEmptyRequestBody",
	},
	ErrUnsupportedFunction: {
		Code:           "UnsupportedFunction",
		Description:    "Encountered an unsupported SQL function.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedFunction",
	},
	ErrInvalidDataSource: {
		Code:           "InvalidDataSource",
		Description:    "Invalid data source type. Only CSV and JSON are supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidDataSource",
	},
	ErrInvalidExpressionType: {
		Code:           "InvalidExpressionType",
		Description:    "The ExpressionType is invalid. Only SQL expressions are supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidExpressionType",
	},
	ErrBusy: {
		Code:           "Busy",
		Description:    "The service is unavailable. Please retry.",
		HTTPStatusCode: http.StatusServiceUnavailable,
		Key:            "ErrBusy",
	},
	ErrUnauthorizedAccess: {
		Code:           "UnauthorizedAccess",
		Description:    "You are not authorized to perform this operation",
		HTTPStatusCode: http.StatusUnauthorized,
		Key:            "ErrUnauthorizedAccess",
	},
	ErrExpressionTooLong: {
		Code:           "ExpressionTooLong",
		Description:    "The SQL expression is too long: The maximum byte-length for the SQL expression is 256 KB.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrExpressionTooLong",
	},
	ErrIllegalSQLFunctionArgument: {
		Code:           "IllegalSqlFunctionArgument",
		Description:    "Illegal argument was used in the SQL function.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrIllegalSQLFunctionArgument",
	},
	ErrInvalidKeyPath: {
		Code:           "InvalidKeyPath",
		Description:    "Key path in the SQL expression is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidKeyPath",
	},
	ErrInvalidCompressionFormat: {
		Code:           "InvalidCompressionFormat",
		Description:    "The file is not in a supported compression format. Only GZIP is supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidCompressionFormat",
	},
	ErrInvalidFileHeaderInfo: {
		Code:           "InvalidFileHeaderInfo",
		Description:    "The FileHeaderInfo is invalid. Only NONE, USE, and IGNORE are supported.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidFileHeaderInfo",
	},
	ErrInvalidJSONType: {
		Code:           "InvalidJsonType",
		Description:    "The JsonType is invalid. Only DOCUMENT and LINES are supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidJSONType",
	},
	ErrInvalidQuoteFields: {
		Code:           "InvalidQuoteFields",
		Description:    "The QuoteFields is invalid. Only ALWAYS and ASNEEDED are supported.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidQuoteFields",
	},
	ErrInvalidRequestParameter: {
		Code:           "InvalidRequestParameter",
		Description:    "The value of a parameter in SelectRequest element is invalid. Check the service API documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidRequestParameter",
	},
	ErrInvalidDataType: {
		Code:           "InvalidDataType",
		Description:    "The SQL expression contains an invalid data type.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidDataType",
	},
	ErrInvalidTextEncoding: {
		Code:           "InvalidTextEncoding",
		Description:    "Invalid encoding type. Only UTF-8 encoding is supported at this time.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidTextEncoding",
	},
	ErrInvalidTableAlias: {
		Code:           "InvalidTableAlias",
		Description:    "The SQL expression contains an invalid table alias.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidTableAlias",
	},
	ErrMissingRequiredParameter: {
		Code:           "MissingRequiredParameter",
		Description:    "The SelectRequest entity is missing a required parameter. Check the service documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingRequiredParameter",
	},
	ErrObjectSerializationConflict: {
		Code:           "ObjectSerializationConflict",
		Description:    "The SelectRequest entity can only contain one of CSV or JSON. Check the service documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrObjectSerializationConflict",
	},
	ErrUnsupportedSQLOperation: {
		Code:           "UnsupportedSqlOperation",
		Description:    "Encountered an unsupported SQL operation.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedSQLOperation",
	},
	ErrUnsupportedSQLStructure: {
		Code:           "UnsupportedSqlStructure",
		Description:    "Encountered an unsupported SQL structure. Check the SQL Reference.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedSQLStructure",
	},
	ErrUnsupportedSyntax: {
		Code:           "UnsupportedSyntax",
		Description:    "Encountered invalid syntax.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedSyntax",
	},
	ErrUnsupportedRangeHeader: {
		Code:           "UnsupportedRangeHeader",
		Description:    "Range header is not supported for this operation.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrUnsupportedRangeHeader",
	},
	ErrLexerInvalidChar: {
		Code:           "LexerInvalidChar",
		Description:    "The SQL expression contains an invalid character.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrLexerInvalidChar",
	},
	ErrLexerInvalidOperator: {
		Code:           "LexerInvalidOperator",
		Description:    "The SQL expression contains an invalid literal.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrLexerInvalidOperator",
	},
	ErrLexerInvalidLiteral: {
		Code:           "LexerInvalidLiteral",
		Description:    "The SQL expression contains an invalid operator.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrLexerInvalidLiteral",
	},
	ErrLexerInvalidIONLiteral: {
		Code:           "LexerInvalidIONLiteral",
		Description:    "The SQL expression contains an invalid operator.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrLexerInvalidIONLiteral",
	},
	ErrParseExpectedDatePart: {
		Code:           "ParseExpectedDatePart",
		Description:    "Did not find the expected date part in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedDatePart",
	},
	ErrParseExpectedKeyword: {
		Code:           "ParseExpectedKeyword",
		Description:    "Did not find the expected keyword in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedKeyword",
	},
	ErrParseExpectedTokenType: {
		Code:           "ParseExpectedTokenType",
		Description:    "Did not find the expected token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedTokenType",
	},
	ErrParseExpected2TokenTypes: {
		Code:           "ParseExpected2TokenTypes",
		Description:    "Did not find the expected token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpected2TokenTypes",
	},
	ErrParseExpectedNumber: {
		Code:           "ParseExpectedNumber",
		Description:    "Did not find the expected number in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedNumber",
	},
	ErrParseExpectedRightParenBuiltinFunctionCall: {
		Code:           "ParseExpectedRightParenBuiltinFunctionCall",
		Description:    "Did not find the expected right parenthesis character in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedRightParenBuiltinFunctionCall",
	},
	ErrParseExpectedTypeName: {
		Code:           "ParseExpectedTypeName",
		Description:    "Did not find the expected type name in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedTypeName",
	},
	ErrParseExpectedWhenClause: {
		Code:           "ParseExpectedWhenClause",
		Description:    "Did not find the expected WHEN clause in the SQL expression. CASE is not supported.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedWhenClause",
	},
	ErrParseUnsupportedToken: {
		Code:           "ParseUnsupportedToken",
		Description:    "The SQL expression contains an unsupported token.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedToken",
	},
	ErrParseUnsupportedLiteralsGroupBy: {
		Code:           "ParseUnsupportedLiteralsGroupBy",
		Description:    "The SQL expression contains an unsupported use of GROUP BY.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedLiteralsGroupBy",
	},
	ErrParseExpectedMember: {
		Code:           "ParseExpectedMember",
		Description:    "The SQL expression contains an unsupported use of MEMBER.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedMember",
	},
	ErrParseUnsupportedSelect: {
		Code:           "ParseUnsupportedSelect",
		Description:    "The SQL expression contains an unsupported use of SELECT.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedSelect",
	},
	ErrParseUnsupportedCase: {
		Code:           "ParseUnsupportedCase",
		Description:    "The SQL expression contains an unsupported use of CASE.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedCase",
	},
	ErrParseUnsupportedCaseClause: {
		Code:           "ParseUnsupportedCaseClause",
		Description:    "The SQL expression contains an unsupported use of CASE.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedCaseClause",
	},
	ErrParseUnsupportedAlias: {
		Code:           "ParseUnsupportedAlias",
		Description:    "The SQL expression contains an unsupported use of ALIAS.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedAlias",
	},
	ErrParseUnsupportedSyntax: {
		Code:           "ParseUnsupportedSyntax",
		Description:    "The SQL expression contains unsupported syntax.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedSyntax",
	},
	ErrParseUnknownOperator: {
		Code:           "ParseUnknownOperator",
		Description:    "The SQL expression contains an invalid operator.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnknownOperator",
	},
	ErrParseMissingIdentAfterAt: {
		Code:           "ParseMissingIdentAfterAt",
		Description:    "Did not find the expected identifier after the @ symbol in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseMissingIdentAfterAt",
	},
	ErrParseUnexpectedOperator: {
		Code:           "ParseUnexpectedOperator",
		Description:    "The SQL expression contains an unexpected operator.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnexpectedOperator",
	},
	ErrParseUnexpectedTerm: {
		Code:           "ParseUnexpectedTerm",
		Description:    "The SQL expression contains an unexpected term.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnexpectedTerm",
	},
	ErrParseUnexpectedToken: {
		Code:           "ParseUnexpectedToken",
		Description:    "The SQL expression contains an unexpected token.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnexpectedToken",
	},
	ErrParseUnexpectedKeyword: {
		Code:           "ParseUnexpectedKeyword",
		Description:    "The SQL expression contains an unexpected keyword.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnexpectedKeyword",
	},
	ErrParseExpectedExpression: {
		Code:           "ParseExpectedExpression",
		Description:    "Did not find the expected SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedExpression",
	},
	ErrParseExpectedLeftParenAfterCast: {
		Code:           "ParseExpectedLeftParenAfterCast",
		Description:    "Did not find expected the left parenthesis in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedLeftParenAfterCast",
	},
	ErrParseExpectedLeftParenValueConstructor: {
		Code:           "ParseExpectedLeftParenValueConstructor",
		Description:    "Did not find expected the left parenthesis in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedLeftParenValueConstructor",
	},
	ErrParseExpectedLeftParenBuiltinFunctionCall: {
		Code:           "ParseExpectedLeftParenBuiltinFunctionCall",
		Description:    "Did not find the expected left parenthesis in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedLeftParenBuiltinFunctionCall",
	},
	ErrParseExpectedArgumentDelimiter: {
		Code:           "ParseExpectedArgumentDelimiter",
		Description:    "Did not find the expected argument delimiter in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedArgumentDelimiter",
	},
	ErrParseCastArity: {
		Code:           "ParseCastArity",
		Description:    "The SQL expression CAST has incorrect arity.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseCastArity",
	},
	ErrParseInvalidTypeParam: {
		Code:           "ParseInvalidTypeParam",
		Description:    "The SQL expression contains an invalid parameter value.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseInvalidTypeParam",
	},
	ErrParseEmptySelect: {
		Code:           "ParseEmptySelect",
		Description:    "The SQL expression contains an empty SELECT.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseEmptySelect",
	},
	ErrParseSelectMissingFrom: {
		Code:           "ParseSelectMissingFrom",
		Description:    "GROUP is not supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseSelectMissingFrom",
	},
	ErrParseExpectedIdentForGroupName: {
		Code:           "ParseExpectedIdentForGroupName",
		Description:    "GROUP is not supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedIdentForGroupName",
	},
	ErrParseExpectedIdentForAlias: {
		Code:           "ParseExpectedIdentForAlias",
		Description:    "Did not find the expected identifier for the alias in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedIdentForAlias",
	},
	ErrParseUnsupportedCallWithStar: {
		Code:           "ParseUnsupportedCallWithStar",
		Description:    "Only COUNT with (*) as a parameter is supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseUnsupportedCallWithStar",
	},
	ErrParseNonUnaryAgregateFunctionCall: {
		Code:           "ParseNonUnaryAgregateFunctionCall",
		Description:    "Only one argument is supported for aggregate functions in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseNonUnaryAgregateFunctionCall",
	},
	ErrParseMalformedJoin: {
		Code:           "ParseMalformedJoin",
		Description:    "JOIN is not supported in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseMalformedJoin",
	},
	ErrParseExpectedIdentForAt: {
		Code:           "ParseExpectedIdentForAt",
		Description:    "Did not find the expected identifier for AT name in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseExpectedIdentForAt",
	},
	ErrParseAsteriskIsNotAloneInSelectList: {
		Code:           "ParseAsteriskIsNotAloneInSelectList",
		Description:    "Other expressions are not allowed in the SELECT list when '*' is used without dot notation in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseAsteriskIsNotAloneInSelectList",
	},
	ErrParseCannotMixSqbAndWildcardInSelectList: {
		Code:           "ParseCannotMixSqbAndWildcardInSelectList",
		Description:    "Cannot mix [] and * in the same expression in a SELECT list in SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseCannotMixSqbAndWildcardInSelectList",
	},
	ErrParseInvalidContextForWildcardInSelectList: {
		Code:           "ParseInvalidContextForWildcardInSelectList",
		Description:    "Invalid use of * in SELECT list in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrParseInvalidContextForWildcardInSelectList",
	},
	ErrIncorrectSQLFunctionArgumentType: {
		Code:           "IncorrectSqlFunctionArgumentType",
		Description:    "Incorrect type of arguments in function call in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrIncorrectSQLFunctionArgumentType",
	},
	ErrValueParseFailure: {
		Code:           "ValueParseFailure",
		Description:    "Time stamp parse failure in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrValueParseFailure",
	},
	ErrEvaluatorInvalidArguments: {
		Code:           "EvaluatorInvalidArguments",
		Description:    "Incorrect number of arguments in the function call in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorInvalidArguments",
	},
	ErrIntegerOverflow: {
		Code:           "IntegerOverflow",
		Description:    "Int overflow or underflow in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrIntegerOverflow",
	},
	ErrLikeInvalidInputs: {
		Code:           "LikeInvalidInputs",
		Description:    "Invalid argument given to the LIKE clause in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrLikeInvalidInputs",
	},
	ErrCastFailed: {
		Code:           "CastFailed",
		Description:    "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrCastFailed",
	},
	ErrInvalidCast: {
		Code:           "InvalidCast",
		Description:    "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidCast",
	},
	ErrEvaluatorInvalidTimestampFormatPattern: {
		Code:           "EvaluatorInvalidTimestampFormatPattern",
		Description:    "Time stamp format pattern requires additional fields in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorInvalidTimestampFormatPattern",
	},
	ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing: {
		Code:           "EvaluatorInvalidTimestampFormatPatternSymbolForParsing",
		Description:    "Time stamp format pattern contains a valid format symbol that cannot be applied to time stamp parsing in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing",
	},
	ErrEvaluatorTimestampFormatPatternDuplicateFields: {
		Code:           "EvaluatorTimestampFormatPatternDuplicateFields",
		Description:    "Time stamp format pattern contains multiple format specifiers representing the time stamp field in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorTimestampFormatPatternDuplicateFields",
	},
	ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch: {
		Code:           "EvaluatorUnterminatedTimestampFormatPatternToken",
		Description:    "Time stamp format pattern contains unterminated token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch",
	},
	ErrEvaluatorUnterminatedTimestampFormatPatternToken: {
		Code:           "EvaluatorInvalidTimestampFormatPatternToken",
		Description:    "Time stamp format pattern contains an invalid token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorUnterminatedTimestampFormatPatternToken",
	},
	ErrEvaluatorInvalidTimestampFormatPatternToken: {
		Code:           "EvaluatorInvalidTimestampFormatPatternToken",
		Description:    "Time stamp format pattern contains an invalid token in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorInvalidTimestampFormatPatternToken",
	},
	ErrEvaluatorInvalidTimestampFormatPatternSymbol: {
		Code:           "EvaluatorInvalidTimestampFormatPatternSymbol",
		Description:    "Time stamp format pattern contains an invalid symbol in the SQL expression.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorInvalidTimestampFormatPatternSymbol",
	},
	ErrEvaluatorBindingDoesNotExist: {
		Code:           "ErrEvaluatorBindingDoesNotExist",
		Description:    "A column name or a path provided does not exist in the SQL expression",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrEvaluatorBindingDoesNotExist",
	},
	ErrMissingHeaders: {
		Code:           "MissingHeaders",
		Description:    "Some headers in the query are missing from the file. Check the file and try again.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrMissingHeaders",
	},
	ErrInvalidColumnIndex: {
		Code:           "InvalidColumnIndex",
		Description:    "The column index is invalid. Please check the service documentation and try again.",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidColumnIndex",
	},
	ErrInvalidDecompressedSize: {
		Code:           "XMinioInvalidDecompressedSize",
		Description:    "The data provided is unfit for decompression",
		HTTPStatusCode: http.StatusBadRequest,
		Key:            "ErrInvalidDecompressedSize",
	},
	ErrAddUserInvalidArgument: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "User is not allowed to be same as admin access key",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrAddUserInvalidArgument",
	},
	ErrAdminAccountNotEligible: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "The administrator key is not eligible for this operation",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrAdminAccountNotEligible",
	},
	ErrAccountNotEligible: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "The account key is not eligible for this operation",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrAccountNotEligible",
	},
	ErrServiceAccountNotFound: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "The specified service account is not found",
		HTTPStatusCode: http.StatusNotFound,
		Key:            "ErrServiceAccountNotFound",
	},
	ErrPostPolicyConditionInvalidFormat: {
		Code:           "PostPolicyInvalidKeyName",
		Description:    "Invalid according to Policy: Policy Condition failed",
		HTTPStatusCode: http.StatusForbidden,
		Key:            "ErrPostPolicyConditionInvalidFormat",
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

	// Only return ErrClientDisconnected if the provided context is actually canceled.
	// This way downstream context.Canceled will still report ErrOperationTimedOut
	select {
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return ErrClientDisconnected
		}
	default:
	}

	switch err {
	case errInvalidArgument:
		apiErr = ErrAdminInvalidArgument
	case errNoSuchUser:
		apiErr = ErrAdminNoSuchUser
	case errNoSuchGroup:
		apiErr = ErrAdminNoSuchGroup
	case errGroupNotEmpty:
		apiErr = ErrAdminGroupNotEmpty
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
	case errAuthentication:
		apiErr = ErrAccessDenied
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
	case context.Canceled, context.DeadlineExceeded:
		apiErr = ErrOperationTimedOut
	case errDiskNotFound:
		apiErr = ErrSlowDown
	case objectlock.ErrInvalidRetentionDate:
		apiErr = ErrInvalidRetentionDate
	case objectlock.ErrPastObjectLockRetainDate:
		apiErr = ErrPastObjectLockRetainDate
	case objectlock.ErrUnknownWORMModeDirective:
		apiErr = ErrUnknownWORMModeDirective
	case objectlock.ErrObjectLockInvalidHeaders:
		apiErr = ErrObjectLockInvalidHeaders
	case objectlock.ErrMalformedXML:
		apiErr = ErrMalformedXML
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
	case ParentIsObject:
		apiErr = ErrParentIsObject
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
	case MethodNotAllowed:
		apiErr = ErrMethodNotAllowed
	case InvalidVersionID:
		apiErr = ErrInvalidVersionID
	case VersionNotFound:
		apiErr = ErrNoSuchVersion
	case ObjectAlreadyExists:
		apiErr = ErrMethodNotAllowed
	case ObjectNameInvalid:
		apiErr = ErrInvalidObjectName
	case ObjectNamePrefixAsSlash:
		apiErr = ErrInvalidObjectNamePrefixSlash
	case InvalidUploadID:
		apiErr = ErrNoSuchUpload
	case InvalidPart:
		apiErr = ErrInvalidPart
	case InsufficientWriteQuorum:
		apiErr = ErrSlowDown
	case InsufficientReadQuorum:
		apiErr = ErrSlowDown
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
	case BucketLifecycleNotFound:
		apiErr = ErrNoSuchLifecycleConfiguration
	case BucketSSEConfigNotFound:
		apiErr = ErrNoSuchBucketSSEConfig
	case BucketTaggingNotFound:
		apiErr = ErrBucketTaggingNotFound
	case BucketObjectLockConfigNotFound:
		apiErr = ErrObjectLockConfigurationNotFound
	case BucketQuotaConfigNotFound:
		apiErr = ErrAdminNoSuchQuotaConfiguration
	case BucketReplicationConfigNotFound:
		apiErr = ErrReplicationConfigurationNotFoundError
	case BucketRemoteDestinationNotFound:
		apiErr = ErrRemoteDestinationNotFoundError
	case BucketReplicationDestinationMissingLock:
		apiErr = ErrReplicationDestinationMissingLock
	case BucketRemoteTargetNotFound:
		apiErr = ErrRemoteTargetNotFoundError
	case BucketRemoteConnectionErr:
		apiErr = ErrReplicationRemoteConnectionError
	case BucketRemoteAlreadyExists:
		apiErr = ErrBucketRemoteAlreadyExists
	case BucketRemoteLabelInUse:
		apiErr = ErrBucketRemoteLabelInUse
	case BucketRemoteArnTypeInvalid:
		apiErr = ErrBucketRemoteArnTypeInvalid
	case BucketRemoteArnInvalid:
		apiErr = ErrBucketRemoteArnInvalid
	case BucketRemoteRemoveDisallowed:
		apiErr = ErrBucketRemoteRemoveDisallowed
	case BucketRemoteTargetNotVersioned:
		apiErr = ErrRemoteTargetNotVersionedError
	case BucketReplicationSourceNotVersioned:
		apiErr = ErrReplicationSourceNotVersionedError
	case BucketQuotaExceeded:
		apiErr = ErrAdminBucketQuotaExceeded
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
	case OperationTimedOut:
		apiErr = ErrOperationTimedOut
	case BackendDown:
		apiErr = ErrBackendDown
	case ObjectNameTooLong:
		apiErr = ErrKeyTooLongError
	case dns.ErrInvalidBucketName:
		apiErr = ErrInvalidBucketName
	case dns.ErrBucketConflict:
		apiErr = ErrBucketAlreadyExists
	default:
		var ie, iw int
		// This work-around is to handle the issue golang/go#30648
		if _, ferr := fmt.Fscanf(strings.NewReader(err.Error()),
			"request declared a Content-Length of %d but only wrote %d bytes",
			&ie, &iw); ferr != nil {
			apiErr = ErrInternalError
			// Make sure to log the errors which we cannot translate
			// to a meaningful S3 API errors. This is added to aid in
			// debugging unexpected/unhandled errors.
			logger.LogIf(ctx, err)
		} else if ie > iw {
			apiErr = ErrIncompleteBody
		} else {
			apiErr = ErrInternalError
			// Make sure to log the errors which we cannot translate
			// to a meaningful S3 API errors. This is added to aid in
			// debugging unexpected/unhandled errors.
			logger.LogIf(ctx, err)
		}
	}

	return apiErr
}

var noError = APIError{}

// toAPIError - Converts embedded errors. Convenience
// function written to handle all cases where we have known types of
// errors returned by underlying layers.
func toAPIError(ctx context.Context, err error) APIError {
	if err == nil {
		return noError
	}

	var apiErr = errorCodes.ToAPIErr(toAPIErrorCode(ctx, err))
	e, ok := err.(dns.ErrInvalidBucketName)
	if ok {
		code := toAPIErrorCode(ctx, e)
		apiErr = errorCodes.ToAPIErrWithErr(code, e)
	}

	if apiErr.Code == "InternalError" {
		// If we see an internal error try to interpret
		// any underlying errors if possible depending on
		// their internal error types. This code is only
		// useful with gateway implementations.
		switch e := err.(type) {
		case InvalidArgument:
			apiErr = APIError{
				Code:           "InvalidArgument",
				Description:    e.Error(),
				HTTPStatusCode: errorCodes[ErrInvalidRequest].HTTPStatusCode,
			}
		case *xml.SyntaxError:
			apiErr = APIError{
				Code: "MalformedXML",
				Description: fmt.Sprintf("%s (%s)", errorCodes[ErrMalformedXML].Description,
					e.Error()),
				HTTPStatusCode: errorCodes[ErrMalformedXML].HTTPStatusCode,
			}
		case url.EscapeError:
			apiErr = APIError{
				Code: "XMinioInvalidObjectName",
				Description: fmt.Sprintf("%s (%s)", errorCodes[ErrInvalidObjectName].Description,
					e.Error()),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case versioning.Error:
			apiErr = APIError{
				Code:           "IllegalVersioningConfigurationException",
				Description:    fmt.Sprintf("Versioning configuration specified in the request is invalid. (%s)", e.Error()),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case lifecycle.Error:
			apiErr = APIError{
				Code:           "InvalidRequest",
				Description:    e.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case replication.Error:
			apiErr = APIError{
				Code:           "MalformedXML",
				Description:    e.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case tags.Error:
			apiErr = APIError{
				Code:           e.Code(),
				Description:    e.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case policy.Error:
			apiErr = APIError{
				Code:           "MalformedPolicy",
				Description:    e.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case crypto.Error:
			apiErr = APIError{
				Code:           "XMinIOEncryptionError",
				Description:    e.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case minio.ErrorResponse:
			apiErr = APIError{
				Code:           e.Code,
				Description:    e.Message,
				HTTPStatusCode: e.StatusCode,
			}
			if globalIsGateway && strings.Contains(e.Message, "KMS is not configured") {
				apiErr = APIError{
					Code:           "NotImplemented",
					Description:    e.Message,
					HTTPStatusCode: http.StatusNotImplemented,
				}
			}
		case *googleapi.Error:
			apiErr = APIError{
				Code:           "XGCSInternalError",
				Description:    e.Message,
				HTTPStatusCode: e.Code,
			}
			// GCS may send multiple errors, just pick the first one
			// since S3 only sends one Error XML response.
			if len(e.Errors) >= 1 {
				apiErr.Code = e.Errors[0].Reason

			}
		case azblob.StorageError:
			apiErr = APIError{
				Code:           string(e.ServiceCode()),
				Description:    e.Error(),
				HTTPStatusCode: e.Response().StatusCode,
			}
			// Add more Gateway SDKs here if any in future.
		default:
			apiErr = APIError{
				Code:           apiErr.Code,
				Description:    fmt.Sprintf("%s: cause(%v)", apiErr.Description, err),
				HTTPStatusCode: apiErr.HTTPStatusCode,
			}
		}
	}

	return apiErr
}

// getAPIError provides API Error for input API error code.
func getAPIError(code APIErrorCode) APIError {
	if apiErr, ok := errorCodes[code]; ok {
		return apiErr
	}
	return errorCodes.ToAPIErr(ErrInternalError)
}

// getErrorResponse gets in standard error and resource value and
// provides a encodable populated response values
func getAPIErrorResponse(ctx context.Context, err APIError, resource, requestID, hostID string) APIErrorResponse {
	reqInfo := logger.GetReqInfo(ctx)
	return APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: reqInfo.BucketName,
		Key:        reqInfo.ObjectName,
		Resource:   resource,
		Region:     globalServerRegion,
		RequestID:  requestID,
		HostID:     hostID,
	}
}
