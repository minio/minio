// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/minio/minio/internal/ioutil"
	"google.golang.org/api/googleapi"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"

	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/versioning"
	levent "github.com/minio/minio/internal/config/lambda/event"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/pkg/v3/policy"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
	ObjectSize     string
	RangeRequested string
}

// APIErrorResponse - error response format
type APIErrorResponse struct {
	XMLName          xml.Name `xml:"Error" json:"-"`
	Code             string
	Message          string
	Key              string `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName       string `xml:"BucketName,omitempty" json:"BucketName,omitempty"`
	Resource         string
	Region           string `xml:"Region,omitempty" json:"Region,omitempty"`
	RequestID        string `xml:"RequestId" json:"RequestId"`
	HostID           string `xml:"HostId" json:"HostId"`
	ActualObjectSize string `xml:"ActualObjectSize,omitempty" json:"ActualObjectSize,omitempty"`
	RangeRequested   string `xml:"RangeRequested,omitempty" json:"RangeRequested,omitempty"`
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
	ErrAccessKeyDisabled
	ErrInvalidArgument
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
	ErrInvalidLifecycleWithObjectLock
	ErrNoSuchBucketSSEConfig
	ErrNoSuchCORSConfiguration
	ErrNoSuchWebsiteConfiguration
	ErrReplicationConfigurationNotFoundError
	ErrRemoteDestinationNotFoundError
	ErrReplicationDestinationMissingLock
	ErrRemoteTargetNotFoundError
	ErrReplicationRemoteConnectionError
	ErrReplicationBandwidthLimitError
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
	ErrReplicationDenyEditError
	ErrRemoteTargetDenyAddError
	ErrReplicationNoExistingObjects
	ErrReplicationValidationError
	ErrReplicationPermissionCheckError
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
	ErrMissingPart
	ErrAuthorizationHeaderMalformed
	ErrMalformedPOSTRequest
	ErrPOSTFileRequired
	ErrSignatureVersionNotSupported
	ErrBucketNotEmpty
	ErrAllAccessDisabled
	ErrPolicyInvalidVersion
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
	ErrUnsupportedHostHeader
	ErrMaximumExpires
	ErrSlowDownRead
	ErrSlowDownWrite
	ErrMaxVersionsExceeded
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
	ErrPolicyAlreadyAttached
	ErrPolicyNotAttached
	ErrExcessData
	ErrPolicyInvalidName
	ErrNoTokenRevokeType
	ErrAdminOpenIDNotEnabled
	ErrAdminNoSuchAccessKey
	// Add new error codes here.

	// SSE-S3/SSE-KMS related API errors
	ErrInvalidEncryptionMethod
	ErrInvalidEncryptionKeyID

	// Server-Side-Encryption (with Customer provided key) related API errors.
	ErrInsecureSSECustomerRequest
	ErrSSEMultipartEncrypted
	ErrSSEEncryptedObject
	ErrInvalidEncryptionParameters
	ErrInvalidEncryptionParametersSSEC

	ErrInvalidSSECustomerAlgorithm
	ErrInvalidSSECustomerKey
	ErrMissingSSECustomerKey
	ErrMissingSSECustomerKeyMD5
	ErrSSECustomerKeyMD5Mismatch
	ErrInvalidSSECustomerParameters
	ErrIncompatibleEncryptionMethod
	ErrKMSNotConfigured
	ErrKMSKeyNotFoundException
	ErrKMSDefaultKeyAlreadyConfigured

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
	ErrContentChecksumMismatch

	// Add new extended error codes here.

	// MinIO extended errors.
	ErrStorageFull
	ErrRequestBodyParse
	ErrObjectExistsAsDirectory
	ErrInvalidObjectName
	ErrInvalidObjectNamePrefixSlash
	ErrInvalidResourceName
	ErrInvalidLifecycleQueryParameter
	ErrServerNotInitialized
	ErrBucketMetadataNotInitialized
	ErrRequestTimedout
	ErrClientDisconnected
	ErrTooManyRequests
	ErrInvalidRequest
	ErrTransitionStorageClassNotFoundError
	// MinIO storage class error codes
	ErrInvalidStorageClass
	ErrBackendDown
	// Add new extended error codes here.
	// Please open a https://github.com/minio/minio/issues before adding
	// new error codes here.

	ErrMalformedJSON
	ErrAdminNoSuchUser
	ErrAdminNoSuchUserLDAPWarn
	ErrAdminLDAPExpectedLoginName
	ErrAdminNoSuchGroup
	ErrAdminGroupNotEmpty
	ErrAdminGroupDisabled
	ErrAdminInvalidGroupName
	ErrAdminNoSuchJob
	ErrAdminNoSuchPolicy
	ErrAdminPolicyChangeAlreadyApplied
	ErrAdminInvalidArgument
	ErrAdminInvalidAccessKey
	ErrAdminInvalidSecretKey
	ErrAdminConfigNoQuorum
	ErrAdminConfigTooLarge
	ErrAdminConfigBadJSON
	ErrAdminNoSuchConfigTarget
	ErrAdminConfigEnvOverridden
	ErrAdminConfigDuplicateKeys
	ErrAdminConfigInvalidIDPType
	ErrAdminConfigLDAPNonDefaultConfigName
	ErrAdminConfigLDAPValidation
	ErrAdminConfigIDPCfgNameAlreadyExists
	ErrAdminConfigIDPCfgNameDoesNotExist
	ErrInsecureClientRequest
	ErrObjectTampered
	ErrAdminLDAPNotEnabled

	// Site-Replication errors
	ErrSiteReplicationInvalidRequest
	ErrSiteReplicationPeerResp
	ErrSiteReplicationBackendIssue
	ErrSiteReplicationServiceAccountError
	ErrSiteReplicationBucketConfigError
	ErrSiteReplicationBucketMetaError
	ErrSiteReplicationIAMError
	ErrSiteReplicationConfigMissing
	ErrSiteReplicationIAMConfigMismatch

	// Pool rebalance errors
	ErrAdminRebalanceAlreadyStarted
	ErrAdminRebalanceNotStarted

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
	ErrParseNonUnaryAggregateFunctionCall
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
	ErrAddUserValidUTF
	ErrAdminResourceInvalidArgument
	ErrAdminAccountNotEligible
	ErrAccountNotEligible
	ErrAdminServiceAccountNotFound
	ErrPostPolicyConditionInvalidFormat

	ErrInvalidChecksum

	// Lambda functions
	ErrLambdaARNInvalid
	ErrLambdaARNNotFound

	// New Codes for GetObjectAttributes and GetObjectVersionAttributes
	ErrInvalidAttributeName

	ErrAdminNoAccessKey
	ErrAdminNoSecretKey

	ErrIAMNotInitialized

	apiErrCodeEnd // This is used only for the testing code
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
	if region := globalSite.Region(); region != "" {
		if errCode == ErrAuthorizationHeaderMalformed {
			apiErr.Description = fmt.Sprintf("The authorization header is malformed; the region is wrong; expecting '%s'.", region)
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
	ErrInvalidEncodingMethod: {
		Code:           "InvalidArgument",
		Description:    "Invalid Encoding Method specified in Request",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxParts: {
		Code:           "InvalidArgument",
		Description:    "Part number must be an integer between 1 and 10000, inclusive",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartNumberMarker: {
		Code:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartNumber: {
		Code:           "InvalidPartNumber",
		Description:    "The requested partnumber is not satisfiable",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
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
	ErrExcessData: {
		Code:           "ExcessData",
		Description:    "More data provided than indicated content length",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPolicyInvalidName: {
		Code:           "PolicyInvalidName",
		Description:    "Policy name may not contain comma",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminOpenIDNotEnabled: {
		Code:           "OpenIDNotEnabled",
		Description:    "No enabled OpenID Connect identity providers",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPolicyTooLarge: {
		Code:           "PolicyTooLarge",
		Description:    "Policy exceeds the maximum allowed document size.",
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
		Description:    "The Access Key Id you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAccessKeyDisabled: {
		Code:           "InvalidAccessKeyId",
		Description:    "Your account is disabled; please contact your administrator.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidArgument: {
		Code:           "InvalidArgument",
		Description:    "Invalid argument",
		HTTPStatusCode: http.StatusBadRequest,
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
	ErrInvalidRangePartNumber: {
		Code:           "InvalidRequest",
		Description:    "Cannot specify both Range header and partNumber query parameter",
		HTTPStatusCode: http.StatusBadRequest,
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
		Description:    "Missing or invalid required header for this request: Content-Md5 or Amz-Content-Checksum",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSecurityHeader: {
		Code:           "MissingSecurityHeader",
		Description:    "Your request was missing a required header",
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
	ErrNoSuchBucketLifecycle: {
		Code:           "NoSuchBucketLifecycle",
		Description:    "The bucket lifecycle configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchLifecycleConfiguration: {
		Code:           "NoSuchLifecycleConfiguration",
		Description:    "The lifecycle configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidLifecycleWithObjectLock: {
		Code:           "InvalidLifecycleWithObjectLock",
		Description:    "The lifecycle configuration containing MaxNoncurrentVersions is not supported with object locking",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchBucketSSEConfig: {
		Code:           "ServerSideEncryptionConfigurationNotFoundError",
		Description:    "The server side encryption configuration was not found",
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
	ErrInvalidVersionID: {
		Code:           "InvalidArgument",
		Description:    "Invalid version id specified",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchVersion: {
		Code:           "NoSuchVersion",
		Description:    "The specified version does not exist.",
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
	ErrMissingPart: {
		Code:           "InvalidRequest",
		Description:    "You must specify at least one part",
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
		Description:    "All access to this resource has been disabled.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrPolicyInvalidVersion: {
		Code:           "MalformedPolicy",
		Description:    "The policy must contain a valid version string",
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
	ErrMalformedCredentialDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; incorrect date format. This date in the credential must be in the format \"yyyyMMdd\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRegion: {
		Code:           "InvalidRegion",
		Description:    "Region does not match.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidServiceS3: {
		Code:           "AuthorizationParametersError",
		Description:    "Error parsing the Credential/X-Amz-Credential parameter; incorrect service. This endpoint belongs to \"s3\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidServiceSTS: {
		Code:           "AuthorizationParametersError",
		Description:    "Error parsing the Credential parameter; incorrect service. This endpoint belongs to \"sts\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
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
	ErrSlowDownRead: {
		Code:           "SlowDownRead",
		Description:    "Resource requested is unreadable, please reduce your request rate",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSlowDownWrite: {
		Code:           "SlowDownWrite",
		Description:    "Resource requested is unwritable, please reduce your request rate",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrMaxVersionsExceeded: {
		Code:           "MaxVersionsExceeded",
		Description:    "You've exceeded the limit on the number of versions you can create on this object",
		HTTPStatusCode: http.StatusBadRequest,
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
	ErrKeyTooLongError: {
		Code:           "KeyTooLongError",
		Description:    "Your key is too long",
		HTTPStatusCode: http.StatusBadRequest,
	},
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
	ErrInvalidBucketObjectLockConfiguration: {
		Code:           "InvalidRequest",
		Description:    "Bucket is missing ObjectLockConfiguration",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketTaggingNotFound: {
		Code:           "NoSuchTagSet",
		Description:    "The TagSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrObjectLockConfigurationNotFound: {
		Code:           "ObjectLockConfigurationNotFoundError",
		Description:    "Object Lock configuration does not exist for this bucket",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrObjectLockConfigurationNotAllowed: {
		Code:           "InvalidBucketState",
		Description:    "Object Lock configuration cannot be enabled on existing buckets",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrNoSuchCORSConfiguration: {
		Code:           "NoSuchCORSConfiguration",
		Description:    "The CORS configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchWebsiteConfiguration: {
		Code:           "NoSuchWebsiteConfiguration",
		Description:    "The specified bucket does not have a website configuration",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrReplicationConfigurationNotFoundError: {
		Code:           "ReplicationConfigurationNotFoundError",
		Description:    "The replication configuration was not found",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrRemoteDestinationNotFoundError: {
		Code:           "RemoteDestinationNotFoundError",
		Description:    "The remote destination bucket does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrReplicationDestinationMissingLock: {
		Code:           "ReplicationDestinationMissingLockError",
		Description:    "The replication destination bucket does not have object locking enabled",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRemoteTargetNotFoundError: {
		Code:           "XMinioAdminRemoteTargetNotFoundError",
		Description:    "The remote target does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrReplicationRemoteConnectionError: {
		Code:           "XMinioAdminReplicationRemoteConnectionError",
		Description:    "Remote service connection error",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrReplicationBandwidthLimitError: {
		Code:           "XMinioAdminReplicationBandwidthLimitError",
		Description:    "Bandwidth limit for remote target must be at least 100MBps",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationNoExistingObjects: {
		Code:           "XMinioReplicationNoExistingObjects",
		Description:    "No matching ExistingObjects rule enabled",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRemoteTargetDenyAddError: {
		Code:           "XMinioAdminRemoteTargetDenyAdd",
		Description:    "Cannot add remote target endpoint since this server is in a cluster replication setup",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationDenyEditError: {
		Code:           "XMinioReplicationDenyEdit",
		Description:    "Cannot alter local replication config since this server is in a cluster replication setup",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketRemoteIdenticalToSource: {
		Code:           "XMinioAdminRemoteIdenticalToSource",
		Description:    "The remote target cannot be identical to source",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketRemoteAlreadyExists: {
		Code:           "XMinioAdminBucketRemoteAlreadyExists",
		Description:    "The remote target already exists",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketRemoteLabelInUse: {
		Code:           "XMinioAdminBucketRemoteLabelInUse",
		Description:    "The remote target with this label already exists",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketRemoteRemoveDisallowed: {
		Code:           "XMinioAdminRemoteRemoveDisallowed",
		Description:    "This ARN is in use by an existing configuration",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketRemoteArnTypeInvalid: {
		Code:           "XMinioAdminRemoteARNTypeInvalid",
		Description:    "The bucket remote ARN type is not valid",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBucketRemoteArnInvalid: {
		Code:           "XMinioAdminRemoteArnInvalid",
		Description:    "The bucket remote ARN does not have correct format",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRemoteTargetNotVersionedError: {
		Code:           "RemoteTargetNotVersionedError",
		Description:    "The remote target does not have versioning enabled",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationSourceNotVersionedError: {
		Code:           "ReplicationSourceNotVersionedError",
		Description:    "The replication source does not have versioning enabled",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationNeedsVersioningError: {
		Code:           "InvalidRequest",
		Description:    "Versioning must be 'Enabled' on the bucket to apply a replication configuration",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationBucketNeedsVersioningError: {
		Code:           "InvalidRequest",
		Description:    "Versioning must be 'Enabled' on the bucket to add a replication target",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationValidationError: {
		Code:           "InvalidRequest",
		Description:    "Replication validation failed on target",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrReplicationPermissionCheckError: {
		Code:           "ReplicationPermissionCheck",
		Description:    "X-Minio-Source-Replication-Check cannot be specified in request. Request cannot be completed",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchObjectLockConfiguration: {
		Code:           "NoSuchObjectLockConfiguration",
		Description:    "The specified object does not have a ObjectLock configuration",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectLocked: {
		Code:           "InvalidRequest",
		Description:    "Object is WORM protected and cannot be overwritten",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRetentionDate: {
		Code:           "InvalidRequest",
		Description:    "Date must be provided in ISO 8601 format",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPastObjectLockRetainDate: {
		Code:           "InvalidRequest",
		Description:    "the retain until date must be in the future",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnknownWORMModeDirective: {
		Code:           "InvalidRequest",
		Description:    "unknown wormMode directive",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectLockInvalidHeaders: {
		Code:           "InvalidRequest",
		Description:    "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectRestoreAlreadyInProgress: {
		Code:           "RestoreAlreadyInProgress",
		Description:    "Object restore is already in progress",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrTransitionStorageClassNotFoundError: {
		Code:           "TransitionStorageClassNotFoundError",
		Description:    "The transition storage class was not found",
		HTTPStatusCode: http.StatusNotFound,
	},

	// Bucket notification related errors.
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
		Description:    "MinIO server does not support Topic or Cloud Function based notifications.",
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
		Code:           "MetadataTooLarge",
		Description:    "Your metadata headers exceed the maximum allowed metadata size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidTagDirective: {
		Code:           "InvalidArgument",
		Description:    "Unknown tag directive.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidEncryptionMethod: {
		Code:           "InvalidArgument",
		Description:    "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncompatibleEncryptionMethod: {
		Code:           "InvalidArgument",
		Description:    "Server Side Encryption with Customer provided key is incompatible with the encryption method specified",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidEncryptionKeyID: {
		Code:           "InvalidRequest",
		Description:    "The specified KMS KeyID contains unsupported characters",
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
	ErrInvalidEncryptionParametersSSEC: {
		Code:           "InvalidRequest",
		Description:    "SSE-C encryption parameters are not supported on replicated bucket.",
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
	ErrKMSNotConfigured: {
		Code:           "NotImplemented",
		Description:    "Server side encryption specified but KMS is not configured",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	ErrKMSKeyNotFoundException: {
		Code:           "KMS.NotFoundException",
		Description:    "Invalid keyId",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSDefaultKeyAlreadyConfigured: {
		Code:           "KMS.DefaultKeyAlreadyConfiguredException",
		Description:    "A default encryption already exists and cannot be changed on KMS",
		HTTPStatusCode: http.StatusConflict,
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
	ErrNoTokenRevokeType: {
		Code:           "InvalidArgument",
		Description:    "No token revoke type specified and one could not be inferred from the request",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminNoSuchAccessKey: {
		Code:           "XMinioAdminNoSuchAccessKey",
		Description:    "The specified access key does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},

	// S3 extensions.
	ErrContentSHA256Mismatch: {
		Code:           "XAmzContentSHA256Mismatch",
		Description:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrContentChecksumMismatch: {
		Code:           "XAmzContentChecksumMismatch",
		Description:    "The provided 'x-amz-checksum' header does not match what was computed.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// MinIO extensions.
	ErrStorageFull: {
		Code:           "XMinioStorageFull",
		Description:    "Storage backend has reached its minimum free drive threshold. Please delete a few objects to proceed.",
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
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidObjectName: {
		Code:           "XMinioInvalidObjectName",
		Description:    "Object name contains unsupported characters.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidObjectNamePrefixSlash: {
		Code:           "XMinioInvalidObjectName",
		Description:    "Object name contains a leading slash.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidResourceName: {
		Code:           "XMinioInvalidResourceName",
		Description:    "Resource name contains bad components such as \"..\" or \".\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrServerNotInitialized: {
		Code:           "XMinioServerNotInitialized",
		Description:    "Server not initialized yet, please try again.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrIAMNotInitialized: {
		Code:           "XMinioIAMNotInitialized",
		Description:    "IAM sub-system not initialized yet, please try again.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrBucketMetadataNotInitialized: {
		Code:           "XMinioBucketMetadataNotInitialized",
		Description:    "Bucket metadata not initialized yet, please try again.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrMalformedJSON: {
		Code:           "XMinioMalformedJSON",
		Description:    "The JSON you provided was not well-formed or did not validate against our published format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidLifecycleQueryParameter: {
		Code:           "XMinioInvalidLifecycleParameter",
		Description:    "The boolean value provided for withUpdatedAt query parameter was invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminNoSuchUser: {
		Code:           "XMinioAdminNoSuchUser",
		Description:    "The specified user does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminNoSuchUserLDAPWarn: {
		Code:           "XMinioAdminNoSuchUser",
		Description:    "The specified user does not exist. If you meant a user in LDAP, use `mc idp ldap`",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminNoSuchGroup: {
		Code:           "XMinioAdminNoSuchGroup",
		Description:    "The specified group does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminNoSuchJob: {
		Code:           "XMinioAdminNoSuchJob",
		Description:    "The specified job does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminGroupNotEmpty: {
		Code:           "XMinioAdminGroupNotEmpty",
		Description:    "The specified group is not empty - cannot remove it.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminGroupDisabled: {
		Code:           "XMinioAdminGroupDisabled",
		Description:    "The specified group is disabled.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminNoSuchPolicy: {
		Code:           "XMinioAdminNoSuchPolicy",
		Description:    "The canned policy does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrAdminPolicyChangeAlreadyApplied: {
		Code:           "XMinioAdminPolicyChangeAlreadyApplied",
		Description:    "The specified policy change is already in effect.",
		HTTPStatusCode: http.StatusBadRequest,
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
	ErrAdminNoAccessKey: {
		Code:           "XMinioAdminNoAccessKey",
		Description:    "No access key was provided.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminNoSecretKey: {
		Code:           "XMinioAdminNoSecretKey",
		Description:    "No secret key was provided.",
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
	ErrAdminNoSuchConfigTarget: {
		Code:           "XMinioAdminNoSuchConfigTarget",
		Description:    "No such named configuration target exists",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigBadJSON: {
		Code:           "XMinioAdminConfigBadJSON",
		Description:    "JSON configuration provided is of incorrect format",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigEnvOverridden: {
		Code:           "XMinioAdminConfigEnvOverridden",
		Description:    "Unable to update config via Admin API due to environment variable override",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigDuplicateKeys: {
		Code:           "XMinioAdminConfigDuplicateKeys",
		Description:    "JSON configuration provided has objects with duplicate keys",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigInvalidIDPType: {
		Code:           "XMinioAdminConfigInvalidIDPType",
		Description:    fmt.Sprintf("Invalid IDP configuration type - must be one of %v", madmin.ValidIDPConfigTypes),
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigLDAPNonDefaultConfigName: {
		Code:           "XMinioAdminConfigLDAPNonDefaultConfigName",
		Description:    "Only a single LDAP configuration is supported - config name must be empty or `_`",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigLDAPValidation: {
		Code:           "XMinioAdminConfigLDAPValidation",
		Description:    "LDAP Configuration validation failed",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigIDPCfgNameAlreadyExists: {
		Code:           "XMinioAdminConfigIDPCfgNameAlreadyExists",
		Description:    "An IDP configuration with the given name already exists",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminConfigIDPCfgNameDoesNotExist: {
		Code:           "XMinioAdminConfigIDPCfgNameDoesNotExist",
		Description:    "No such IDP configuration exists",
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
	ErrAdminBucketQuotaExceeded: {
		Code:           "XMinioAdminBucketQuotaExceeded",
		Description:    "Bucket quota exceeded",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminNoSuchQuotaConfiguration: {
		Code:           "XMinioAdminNoSuchQuotaConfiguration",
		Description:    "The quota configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInsecureClientRequest: {
		Code:           "XMinioInsecureClientRequest",
		Description:    "Cannot respond to plain-text request from TLS-encrypted server",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRequestTimedout: {
		Code:           "RequestTimeout",
		Description:    "A timeout occurred while trying to lock a resource, please reduce your request rate",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrClientDisconnected: {
		Code:           "ClientDisconnected",
		Description:    "Client disconnected before response was ready",
		HTTPStatusCode: 499, // No official code, use nginx value.
	},
	ErrTooManyRequests: {
		Code:           "TooManyRequests",
		Description:    "Please reduce your request rate",
		HTTPStatusCode: http.StatusTooManyRequests,
	},
	ErrUnsupportedMetadata: {
		Code:           "InvalidArgument",
		Description:    "Your metadata headers are not supported.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsupportedHostHeader: {
		Code:           "InvalidArgument",
		Description:    "Your Host header is malformed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectTampered: {
		Code:           "XMinioObjectTampered",
		Description:    errObjectTampered.Error(),
		HTTPStatusCode: http.StatusPartialContent,
	},

	ErrSiteReplicationInvalidRequest: {
		Code:           "XMinioSiteReplicationInvalidRequest",
		Description:    "Invalid site-replication request",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSiteReplicationPeerResp: {
		Code:           "XMinioSiteReplicationPeerResp",
		Description:    "Error received when contacting a peer site",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSiteReplicationBackendIssue: {
		Code:           "XMinioSiteReplicationBackendIssue",
		Description:    "Error when requesting object layer backend",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSiteReplicationServiceAccountError: {
		Code:           "XMinioSiteReplicationServiceAccountError",
		Description:    "Site replication related service account error",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSiteReplicationBucketConfigError: {
		Code:           "XMinioSiteReplicationBucketConfigError",
		Description:    "Error while configuring replication on a bucket",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSiteReplicationBucketMetaError: {
		Code:           "XMinioSiteReplicationBucketMetaError",
		Description:    "Error while replicating bucket metadata",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSiteReplicationIAMError: {
		Code:           "XMinioSiteReplicationIAMError",
		Description:    "Error while replicating an IAM item",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSiteReplicationConfigMissing: {
		Code:           "XMinioSiteReplicationConfigMissingError",
		Description:    "Site not found in site replication configuration",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSiteReplicationIAMConfigMismatch: {
		Code:           "XMinioSiteReplicationIAMConfigMismatch",
		Description:    "IAM configuration mismatch between sites",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminRebalanceAlreadyStarted: {
		Code:           "XMinioAdminRebalanceAlreadyStarted",
		Description:    "Pool rebalance is already started",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrAdminRebalanceNotStarted: {
		Code:           "XMinioAdminRebalanceNotStarted",
		Description:    "Pool rebalance is not started",
		HTTPStatusCode: http.StatusNotFound,
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
		Description:    "Remote backend is unreachable",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncorrectContinuationToken: {
		Code:           "InvalidArgument",
		Description:    "The continuation token provided is incorrect",
		HTTPStatusCode: http.StatusBadRequest,
	},
	// S3 Select API Errors
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
		Code:           "ServerBusy",
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
	ErrParseNonUnaryAggregateFunctionCall: {
		Code:           "ParseNonUnaryAggregateFunctionCall",
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
	ErrAddUserInvalidArgument: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "Credential is not allowed to be same as admin access key",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAdminResourceInvalidArgument: {
		Code:           "XMinioInvalidResource",
		Description:    "Policy, user or group names are not allowed to begin or end with space characters",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminAccountNotEligible: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "The administrator key is not eligible for this operation",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAccountNotEligible: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "The account key is not eligible for this operation",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAdminServiceAccountNotFound: {
		Code:           "XMinioInvalidIAMCredentials",
		Description:    "The specified service account is not found",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrPostPolicyConditionInvalidFormat: {
		Code:           "PostPolicyInvalidKeyName",
		Description:    "Invalid according to Policy: Policy Condition failed",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidChecksum: {
		Code:           "InvalidArgument",
		Description:    "Invalid checksum provided.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLambdaARNInvalid: {
		Code:           "LambdaARNInvalid",
		Description:    "The specified lambda ARN is invalid",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrLambdaARNNotFound: {
		Code:           "LambdaARNNotFound",
		Description:    "The specified lambda ARN does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrPolicyAlreadyAttached: {
		Code:           "XMinioPolicyAlreadyAttached",
		Description:    "The specified policy is already attached.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrPolicyNotAttached: {
		Code:           "XMinioPolicyNotAttached",
		Description:    "The specified policy is not found.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidAttributeName: {
		Code:           "InvalidArgument",
		Description:    "Invalid attribute name specified.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminLDAPNotEnabled: {
		Code:           "XMinioLDAPNotEnabled",
		Description:    "LDAP is not enabled. LDAP must be enabled to make LDAP requests.",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	ErrAdminLDAPExpectedLoginName: {
		Code:           "XMinioLDAPExpectedLoginName",
		Description:    "Expected LDAP short username but was given full DN.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAdminInvalidGroupName: {
		Code:           "XMinioInvalidGroupName",
		Description:    "The group name is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAddUserValidUTF: {
		Code:           "XMinioInvalidUTF",
		Description:    "Invalid UTF-8 character detected.",
		HTTPStatusCode: http.StatusBadRequest,
	},
}

// toAPIErrorCode - Converts embedded errors. Convenience
// function written to handle all cases where we have known types of
// errors returned by underlying layers.
func toAPIErrorCode(ctx context.Context, err error) (apiErr APIErrorCode) {
	if err == nil {
		return ErrNone
	}

	// Errors that are generated by net.Conn and any context errors must be handled here.
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ErrRequestTimedout
	}

	// Only return ErrClientDisconnected if the provided context is actually canceled.
	// This way downstream context.Canceled will still report ErrRequestTimedout
	if contextCanceled(ctx) && errors.Is(ctx.Err(), context.Canceled) {
		return ErrClientDisconnected
	}

	// Unwrap the error first
	err = unwrapAll(err)

	switch err {
	case errInvalidArgument:
		apiErr = ErrAdminInvalidArgument
	case errNoSuchPolicy:
		apiErr = ErrAdminNoSuchPolicy
	case errNoSuchUser:
		apiErr = ErrAdminNoSuchUser
	case errNoSuchUserLDAPWarn:
		apiErr = ErrAdminNoSuchUserLDAPWarn
	case errNoSuchServiceAccount:
		apiErr = ErrAdminServiceAccountNotFound
	case errNoSuchAccessKey:
		apiErr = ErrAdminNoSuchAccessKey
	case errNoSuchGroup:
		apiErr = ErrAdminNoSuchGroup
	case errGroupNotEmpty:
		apiErr = ErrAdminGroupNotEmpty
	case errGroupNameContainsReservedChars:
		apiErr = ErrAdminInvalidGroupName
	case errNoSuchJob:
		apiErr = ErrAdminNoSuchJob
	case errNoPolicyToAttachOrDetach:
		apiErr = ErrAdminPolicyChangeAlreadyApplied
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
	case auth.ErrContainsReservedChars:
		apiErr = ErrAdminInvalidAccessKey
	case auth.ErrInvalidAccessKeyLength:
		apiErr = ErrAdminInvalidAccessKey
	case auth.ErrInvalidSecretKeyLength:
		apiErr = ErrAdminInvalidSecretKey
	case auth.ErrNoAccessKeyWithSecretKey:
		apiErr = ErrAdminNoAccessKey
	case auth.ErrNoSecretKeyWithAccessKey:
		apiErr = ErrAdminNoSecretKey
	case errInvalidStorageClass:
		apiErr = ErrInvalidStorageClass
	case errErasureReadQuorum:
		apiErr = ErrSlowDownRead
	case errErasureWriteQuorum:
		apiErr = ErrSlowDownWrite
	case errMaxVersionsExceeded:
		apiErr = ErrMaxVersionsExceeded
	// SSE errors
	case errInvalidEncryptionParameters:
		apiErr = ErrInvalidEncryptionParameters
	case errInvalidEncryptionParametersSSEC:
		apiErr = ErrInvalidEncryptionParametersSSEC
	case crypto.ErrInvalidEncryptionMethod:
		apiErr = ErrInvalidEncryptionMethod
	case crypto.ErrInvalidEncryptionKeyID:
		apiErr = ErrInvalidEncryptionKeyID
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
	case errKMSKeyNotFound:
		apiErr = ErrKMSKeyNotFoundException
	case errKMSDefaultKeyAlreadyConfigured:
		apiErr = ErrKMSDefaultKeyAlreadyConfigured
	case context.Canceled:
		apiErr = ErrClientDisconnected
	case context.DeadlineExceeded:
		apiErr = ErrRequestTimedout
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
	case errInvalidMaxParts:
		apiErr = ErrInvalidMaxParts
	case ioutil.ErrOverread:
		apiErr = ErrExcessData
	case errServerNotInitialized:
		apiErr = ErrServerNotInitialized
	case errBucketMetadataNotInitialized:
		apiErr = ErrBucketMetadataNotInitialized
	case hash.ErrInvalidChecksum:
		apiErr = ErrInvalidChecksum
	}

	// Compression errors
	if err == errInvalidDecompressedSize {
		apiErr = ErrInvalidDecompressedSize
	}

	if apiErr != ErrNone {
		// If there was a match in the above switch case.
		return apiErr
	}

	// etcd specific errors, a key is always a bucket for us return
	// ErrNoSuchBucket in such a case.
	if errors.Is(err, dns.ErrNoEntriesFound) {
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
	case MethodNotAllowed:
		apiErr = ErrMethodNotAllowed
	case ObjectLocked:
		apiErr = ErrObjectLocked
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
		apiErr = ErrSlowDownWrite
	case InsufficientReadQuorum:
		apiErr = ErrSlowDownRead
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
	case hash.ChecksumMismatch:
		apiErr = ErrContentChecksumMismatch
	case hash.SizeTooSmall:
		apiErr = ErrEntityTooSmall
	case hash.SizeTooLarge:
		apiErr = ErrEntityTooLarge
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
	case BucketRemoteTargetNotFound:
		apiErr = ErrRemoteTargetNotFoundError
	case RemoteTargetConnectionErr:
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
	case TransitionStorageClassNotFound:
		apiErr = ErrTransitionStorageClassNotFoundError
	case InvalidObjectState:
		apiErr = ErrInvalidObjectState
	case PreConditionFailed:
		apiErr = ErrPreconditionFailed
	case BucketQuotaExceeded:
		apiErr = ErrAdminBucketQuotaExceeded
	case *event.ErrInvalidEventName:
		apiErr = ErrEventNotification
	case *event.ErrInvalidARN:
		apiErr = ErrARNNotification
	case *event.ErrARNNotFound:
		apiErr = ErrARNNotification
	case *levent.ErrInvalidARN:
		apiErr = ErrLambdaARNInvalid
	case *levent.ErrARNNotFound:
		apiErr = ErrLambdaARNNotFound
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
		apiErr = ErrRequestTimedout
	case BackendDown:
		apiErr = ErrBackendDown
	case ObjectNameTooLong:
		apiErr = ErrKeyTooLongError
	case dns.ErrInvalidBucketName:
		apiErr = ErrInvalidBucketName
	case dns.ErrBucketConflict:
		apiErr = ErrBucketAlreadyExists
	default:
		if strings.Contains(err.Error(), "request declared a Content-Length") {
			apiErr = ErrIncompleteBody
		} else {
			apiErr = ErrInternalError
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

	apiErr := errorCodes.ToAPIErr(toAPIErrorCode(ctx, err))
	switch apiErr.Code {
	case "NotImplemented":
		apiErr = APIError{
			Code:           apiErr.Code,
			Description:    fmt.Sprintf("%s (%v)", apiErr.Description, err),
			HTTPStatusCode: apiErr.HTTPStatusCode,
		}
	case "XMinioBackendDown":
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, err)
	case "InternalError":
		// If we see an internal error try to interpret
		// any underlying errors if possible depending on
		// their internal error types.
		switch e := err.(type) {
		case kms.Error:
			apiErr = APIError{
				Code:           e.APICode,
				Description:    e.Err,
				HTTPStatusCode: e.Code,
			}
		case batchReplicationJobError:
			apiErr = APIError{
				Description:    e.Description,
				Code:           e.Code,
				HTTPStatusCode: e.HTTPStatusCode,
			}
		case InvalidRange:
			apiErr = APIError{
				Code:           "InvalidRange",
				Description:    e.Error(),
				HTTPStatusCode: errorCodes[ErrInvalidRange].HTTPStatusCode,
				ObjectSize:     strconv.FormatInt(e.ResourceSize, 10),
				RangeRequested: fmt.Sprintf("%d-%d", e.OffsetBegin, e.OffsetEnd),
			}
		case InvalidArgument:
			apiErr = APIError{
				Code:           "InvalidArgument",
				Description:    e.Error(),
				HTTPStatusCode: errorCodes[ErrInvalidRequest].HTTPStatusCode,
			}
		case *xml.SyntaxError:
			apiErr = APIError{
				Code:           "MalformedXML",
				Description:    fmt.Sprintf("%s (%s)", errorCodes[ErrMalformedXML].Description, e),
				HTTPStatusCode: errorCodes[ErrMalformedXML].HTTPStatusCode,
			}
		case url.EscapeError:
			apiErr = APIError{
				Code:           "XMinioInvalidObjectName",
				Description:    fmt.Sprintf("%s (%s)", errorCodes[ErrInvalidObjectName].Description, e),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case versioning.Error:
			apiErr = APIError{
				Code:           "IllegalVersioningConfigurationException",
				Description:    fmt.Sprintf("Versioning configuration specified in the request is invalid. (%s)", e),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case lifecycle.Error:
			apiErr = APIError{
				Code:           "InvalidArgument",
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
				Code:           "XMinioEncryptionError",
				Description:    e.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case minio.ErrorResponse:
			apiErr = APIError{
				Code:           e.Code,
				Description:    e.Message,
				HTTPStatusCode: e.StatusCode,
			}
			if strings.Contains(e.Message, "KMS is not configured") {
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
		case *azcore.ResponseError:
			apiErr = APIError{
				Code:           e.ErrorCode,
				Description:    e.Error(),
				HTTPStatusCode: e.StatusCode,
			}
			// Add more other SDK related errors here if any in future.
		default:
			//nolint:gocritic
			if errors.Is(err, errMalformedEncoding) || errors.Is(err, errChunkTooBig) || errors.Is(err, strconv.ErrRange) {
				apiErr = APIError{
					Code:           "BadRequest",
					Description:    err.Error(),
					HTTPStatusCode: http.StatusBadRequest,
				}
			} else {
				apiErr = APIError{
					Code:           apiErr.Code,
					Description:    fmt.Sprintf("%s: cause(%v)", apiErr.Description, err),
					HTTPStatusCode: apiErr.HTTPStatusCode,
				}
			}
		}
	}

	if apiErr.Code == "InternalError" {
		// Make sure to log the errors which we cannot translate
		// to a meaningful S3 API errors. This is added to aid in
		// debugging unexpected/unhandled errors.
		internalLogIf(ctx, err)
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

// getAPIErrorResponse gets in standard error and resource value and
// provides a encodable populated response values
func getAPIErrorResponse(ctx context.Context, err APIError, resource, requestID, hostID string) APIErrorResponse {
	reqInfo := logger.GetReqInfo(ctx)
	return APIErrorResponse{
		Code:             err.Code,
		Message:          err.Description,
		BucketName:       reqInfo.BucketName,
		Key:              reqInfo.ObjectName,
		Resource:         resource,
		Region:           globalSite.Region(),
		RequestID:        requestID,
		HostID:           hostID,
		ActualObjectSize: err.ObjectSize,
		RangeRequested:   err.RangeRequested,
	}
}
