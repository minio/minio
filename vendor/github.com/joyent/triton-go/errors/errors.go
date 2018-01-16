//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package errors

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

// APIError represents an error code and message along with
// the status code of the HTTP request which resulted in the error
// message. Error codes used by the Triton API are listed at
// https://apidocs.joyent.com/cloudapi/#cloudapi-http-responses
// Error codes used by the Manta API are listed at
// https://apidocs.joyent.com/manta/api.html#errors
type APIError struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
}

// Error implements interface Error on the APIError type.
func (e APIError) Error() string {
	return strings.Trim(fmt.Sprintf("%+q", e.Code), `"`) + ": " + strings.Trim(fmt.Sprintf("%+q", e.Message), `"`)
}

// ClientError represents an error code and message returned
// when connecting to the triton-go client
type ClientError struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
}

// Error implements interface Error on the ClientError type.
func (e ClientError) Error() string {
	return strings.Trim(fmt.Sprintf("%+q", e.Code), `"`) + ": " + strings.Trim(fmt.Sprintf("%+q", e.Message), `"`)
}

func IsAuthSchemeError(err error) bool {
	return IsSpecificError(err, "AuthScheme")
}

func IsAuthorizationError(err error) bool {
	return IsSpecificError(err, "Authorization")
}

func IsBadRequestError(err error) bool {
	return IsSpecificError(err, "BadRequest")
}

func IsChecksumError(err error) bool {
	return IsSpecificError(err, "Checksum")
}

func IsConcurrentRequestError(err error) bool {
	return IsSpecificError(err, "ConcurrentRequest")
}

func IsContentLengthError(err error) bool {
	return IsSpecificError(err, "ContentLength")
}

func IsContentMD5MismatchError(err error) bool {
	return IsSpecificError(err, "ContentMD5Mismatch")
}

func IsEntityExistsError(err error) bool {
	return IsSpecificError(err, "EntityExists")
}

func IsInvalidArgumentError(err error) bool {
	return IsSpecificError(err, "InvalidArgument")
}

func IsInvalidAuthTokenError(err error) bool {
	return IsSpecificError(err, "InvalidAuthToken")
}

func IsInvalidCredentialsError(err error) bool {
	return IsSpecificError(err, "InvalidCredentials")
}

func IsInvalidDurabilityLevelError(err error) bool {
	return IsSpecificError(err, "InvalidDurabilityLevel")
}

func IsInvalidKeyIdError(err error) bool {
	return IsSpecificError(err, "InvalidKeyId")
}

func IsInvalidJobError(err error) bool {
	return IsSpecificError(err, "InvalidJob")
}

func IsInvalidLinkError(err error) bool {
	return IsSpecificError(err, "InvalidLink")
}

func IsInvalidLimitError(err error) bool {
	return IsSpecificError(err, "InvalidLimit")
}

func IsInvalidSignatureError(err error) bool {
	return IsSpecificError(err, "InvalidSignature")
}

func IsInvalidUpdateError(err error) bool {
	return IsSpecificError(err, "InvalidUpdate")
}

func IsDirectoryDoesNotExistError(err error) bool {
	return IsSpecificError(err, "DirectoryDoesNotExist")
}

func IsDirectoryExistsError(err error) bool {
	return IsSpecificError(err, "DirectoryExists")
}

func IsDirectoryNotEmptyError(err error) bool {
	return IsSpecificError(err, "DirectoryNotEmpty")
}

func IsDirectoryOperationError(err error) bool {
	return IsSpecificError(err, "DirectoryOperation")
}

func IsInternalError(err error) bool {
	return IsSpecificError(err, "Internal")
}

func IsJobNotFoundError(err error) bool {
	return IsSpecificError(err, "JobNotFound")
}

func IsJobStateError(err error) bool {
	return IsSpecificError(err, "JobState")
}

func IsKeyDoesNotExistError(err error) bool {
	return IsSpecificError(err, "KeyDoesNotExist")
}

func IsNotAcceptableError(err error) bool {
	return IsSpecificError(err, "NotAcceptable")
}

func IsNotEnoughSpaceError(err error) bool {
	return IsSpecificError(err, "NotEnoughSpace")
}

func IsLinkNotFoundError(err error) bool {
	return IsSpecificError(err, "LinkNotFound")
}

func IsLinkNotObjectError(err error) bool {
	return IsSpecificError(err, "LinkNotObject")
}

func IsLinkRequiredError(err error) bool {
	return IsSpecificError(err, "LinkRequired")
}

func IsParentNotDirectoryError(err error) bool {
	return IsSpecificError(err, "ParentNotDirectory")
}

func IsPreconditionFailedError(err error) bool {
	return IsSpecificError(err, "PreconditionFailed")
}

func IsPreSignedRequestError(err error) bool {
	return IsSpecificError(err, "PreSignedRequest")
}

func IsRequestEntityTooLargeError(err error) bool {
	return IsSpecificError(err, "RequestEntityTooLarge")
}

func IsResourceNotFoundError(err error) bool {
	return IsSpecificError(err, "ResourceNotFound")
}

func IsRootDirectoryError(err error) bool {
	return IsSpecificError(err, "RootDirectory")
}

func IsServiceUnavailableError(err error) bool {
	return IsSpecificError(err, "ServiceUnavailable")
}

func IsSSLRequiredError(err error) bool {
	return IsSpecificError(err, "SSLRequired")
}

func IsUploadTimeoutError(err error) bool {
	return IsSpecificError(err, "UploadTimeout")
}

func IsUserDoesNotExistError(err error) bool {
	return IsSpecificError(err, "UserDoesNotExist")
}

func IsBadRequest(err error) bool {
	return IsSpecificError(err, "BadRequest")
}

func IsInUseError(err error) bool {
	return IsSpecificError(err, "InUseError")
}

func IsInvalidArgument(err error) bool {
	return IsSpecificError(err, "InvalidArgument")
}

func IsInvalidCredentials(err error) bool {
	return IsSpecificError(err, "InvalidCredentials")
}

func IsInvalidHeader(err error) bool {
	return IsSpecificError(err, "InvalidHeader")
}

func IsInvalidVersion(err error) bool {
	return IsSpecificError(err, "InvalidVersion")
}

func IsMissingParameter(err error) bool {
	return IsSpecificError(err, "MissingParameter")
}

func IsNotAuthorized(err error) bool {
	return IsSpecificError(err, "NotAuthorized")
}

func IsRequestThrottled(err error) bool {
	return IsSpecificError(err, "RequestThrottled")
}

func IsRequestTooLarge(err error) bool {
	return IsSpecificError(err, "RequestTooLarge")
}

func IsRequestMoved(err error) bool {
	return IsSpecificError(err, "RequestMoved")
}

func IsResourceFound(err error) bool {
	return IsSpecificError(err, "ResourceFound")
}

func IsResourceNotFound(err error) bool {
	return IsSpecificError(err, "ResourceNotFound")
}

func IsUnknownError(err error) bool {
	return IsSpecificError(err, "UnknownError")
}

func IsEmptyResponse(err error) bool {
	return IsSpecificError(err, "EmptyResponse")
}

func IsStatusNotFoundCode(err error) bool {
	return IsSpecificStatusCode(err, http.StatusNotFound)
}

func IsSpecificError(myError error, errorCode string) bool {
	switch err := errors.Cause(myError).(type) {
	case *APIError:
		if err.Code == errorCode {
			return true
		}
	}

	return false
}

func IsSpecificStatusCode(myError error, statusCode int) bool {
	switch err := errors.Cause(myError).(type) {
	case *APIError:
		if err.StatusCode == statusCode {
			return true
		}
	}

	return false
}
