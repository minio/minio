package client

import (
	"fmt"

	"github.com/hashicorp/errwrap"
)

// ClientError represents an error code and message along with the status code
// of the HTTP request which resulted in the error message.
type ClientError struct {
	StatusCode int
	Code       string
	Message    string
}

// Error implements interface Error on the TritonError type.
func (e ClientError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// MantaError represents an error code and message along with
// the status code of the HTTP request which resulted in the error
// message. Error codes used by the Manta API are listed at
// https://apidocs.joyent.com/manta/api.html#errors
type MantaError struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
}

// Error implements interface Error on the MantaError type.
func (e MantaError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// TritonError represents an error code and message along with
// the status code of the HTTP request which resulted in the error
// message. Error codes used by the Triton API are listed at
// https://apidocs.joyent.com/cloudapi/#cloudapi-http-responses
type TritonError struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
}

// Error implements interface Error on the TritonError type.
func (e TritonError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func IsAuthSchemeError(err error) bool {
	return isSpecificError(err, "AuthScheme")
}
func IsAuthorizationError(err error) bool {
	return isSpecificError(err, "Authorization")
}
func IsBadRequestError(err error) bool {
	return isSpecificError(err, "BadRequest")
}
func IsChecksumError(err error) bool {
	return isSpecificError(err, "Checksum")
}
func IsConcurrentRequestError(err error) bool {
	return isSpecificError(err, "ConcurrentRequest")
}
func IsContentLengthError(err error) bool {
	return isSpecificError(err, "ContentLength")
}
func IsContentMD5MismatchError(err error) bool {
	return isSpecificError(err, "ContentMD5Mismatch")
}
func IsEntityExistsError(err error) bool {
	return isSpecificError(err, "EntityExists")
}
func IsInvalidArgumentError(err error) bool {
	return isSpecificError(err, "InvalidArgument")
}
func IsInvalidAuthTokenError(err error) bool {
	return isSpecificError(err, "InvalidAuthToken")
}
func IsInvalidCredentialsError(err error) bool {
	return isSpecificError(err, "InvalidCredentials")
}
func IsInvalidDurabilityLevelError(err error) bool {
	return isSpecificError(err, "InvalidDurabilityLevel")
}
func IsInvalidKeyIdError(err error) bool {
	return isSpecificError(err, "InvalidKeyId")
}
func IsInvalidJobError(err error) bool {
	return isSpecificError(err, "InvalidJob")
}
func IsInvalidLinkError(err error) bool {
	return isSpecificError(err, "InvalidLink")
}
func IsInvalidLimitError(err error) bool {
	return isSpecificError(err, "InvalidLimit")
}
func IsInvalidSignatureError(err error) bool {
	return isSpecificError(err, "InvalidSignature")
}
func IsInvalidUpdateError(err error) bool {
	return isSpecificError(err, "InvalidUpdate")
}
func IsDirectoryDoesNotExistError(err error) bool {
	return isSpecificError(err, "DirectoryDoesNotExist")
}
func IsDirectoryExistsError(err error) bool {
	return isSpecificError(err, "DirectoryExists")
}
func IsDirectoryNotEmptyError(err error) bool {
	return isSpecificError(err, "DirectoryNotEmpty")
}
func IsDirectoryOperationError(err error) bool {
	return isSpecificError(err, "DirectoryOperation")
}
func IsInternalError(err error) bool {
	return isSpecificError(err, "Internal")
}
func IsJobNotFoundError(err error) bool {
	return isSpecificError(err, "JobNotFound")
}
func IsJobStateError(err error) bool {
	return isSpecificError(err, "JobState")
}
func IsKeyDoesNotExistError(err error) bool {
	return isSpecificError(err, "KeyDoesNotExist")
}
func IsNotAcceptableError(err error) bool {
	return isSpecificError(err, "NotAcceptable")
}
func IsNotEnoughSpaceError(err error) bool {
	return isSpecificError(err, "NotEnoughSpace")
}
func IsLinkNotFoundError(err error) bool {
	return isSpecificError(err, "LinkNotFound")
}
func IsLinkNotObjectError(err error) bool {
	return isSpecificError(err, "LinkNotObject")
}
func IsLinkRequiredError(err error) bool {
	return isSpecificError(err, "LinkRequired")
}
func IsParentNotDirectoryError(err error) bool {
	return isSpecificError(err, "ParentNotDirectory")
}
func IsPreconditionFailedError(err error) bool {
	return isSpecificError(err, "PreconditionFailed")
}
func IsPreSignedRequestError(err error) bool {
	return isSpecificError(err, "PreSignedRequest")
}
func IsRequestEntityTooLargeError(err error) bool {
	return isSpecificError(err, "RequestEntityTooLarge")
}
func IsResourceNotFoundError(err error) bool {
	return isSpecificError(err, "ResourceNotFound")
}
func IsRootDirectoryError(err error) bool {
	return isSpecificError(err, "RootDirectory")
}
func IsServiceUnavailableError(err error) bool {
	return isSpecificError(err, "ServiceUnavailable")
}
func IsSSLRequiredError(err error) bool {
	return isSpecificError(err, "SSLRequired")
}
func IsUploadTimeoutError(err error) bool {
	return isSpecificError(err, "UploadTimeout")
}
func IsUserDoesNotExistError(err error) bool {
	return isSpecificError(err, "UserDoesNotExist")
}

// isSpecificError checks whether the error represented by err wraps
// an underlying MantaError with code errorCode.
func isSpecificError(err error, errorCode string) bool {
	tritonErrorInterface := errwrap.GetType(err.(error), &MantaError{})
	if tritonErrorInterface == nil {
		return false
	}

	tritonErr := tritonErrorInterface.(*MantaError)
	if tritonErr.Code == errorCode {
		return true
	}

	return false
}
