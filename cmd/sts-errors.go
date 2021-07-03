// Copyright (c) 2015-2021 MinIO, Inc.
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
	"net/http"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

// writeSTSErrorRespone writes error headers
func writeSTSErrorResponse(ctx context.Context, w http.ResponseWriter, isErrCodeSTS bool, errCode STSErrorCode, errCtxt error) {
	var err STSError
	if isErrCodeSTS {
		err = stsErrCodes.ToSTSErr(errCode)
	}
	if err.Code == "InternalError" || !isErrCodeSTS {
		aerr := getAPIError(APIErrorCode(errCode))
		if aerr.Code != "InternalError" {
			err.Code = aerr.Code
			err.Description = aerr.Description
			err.HTTPStatusCode = aerr.HTTPStatusCode
		}
	}
	// Generate error response.
	stsErrorResponse := STSErrorResponse{}
	stsErrorResponse.Error.Code = err.Code
	stsErrorResponse.RequestID = w.Header().Get(xhttp.AmzRequestID)
	stsErrorResponse.Error.Message = err.Description
	if errCtxt != nil {
		stsErrorResponse.Error.Message = errCtxt.Error()
	}
	var logKind logger.Kind
	switch errCode {
	case ErrSTSInternalError, ErrSTSNotInitialized:
		logKind = logger.Minio
	default:
		logKind = logger.All
	}
	logger.LogIf(ctx, errCtxt, logKind)
	encodedErrorResponse := encodeResponse(stsErrorResponse)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeXML)
}

// STSError structure
type STSError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// STSErrorResponse - error response format
type STSErrorResponse struct {
	XMLName xml.Name `xml:"https://sts.amazonaws.com/doc/2011-06-15/ ErrorResponse" json:"-"`
	Error   struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestID string `xml:"RequestId"`
}

// STSErrorCode type of error status.
type STSErrorCode int

//go:generate stringer -type=STSErrorCode -trimprefix=Err $GOFILE

// Error codes, non exhaustive list - http://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithSAML.html
const (
	ErrSTSNone STSErrorCode = iota
	ErrSTSAccessDenied
	ErrSTSMissingParameter
	ErrSTSInvalidParameterValue
	ErrSTSWebIdentityExpiredToken
	ErrSTSClientGrantsExpiredToken
	ErrSTSInvalidClientGrantsToken
	ErrSTSMalformedPolicyDocument
	ErrSTSNotInitialized
	ErrSTSInternalError
)

type stsErrorCodeMap map[STSErrorCode]STSError

func (e stsErrorCodeMap) ToSTSErr(errCode STSErrorCode) STSError {
	apiErr, ok := e[errCode]
	if !ok {
		return e[ErrSTSInternalError]
	}
	return apiErr
}

// error code to STSError structure, these fields carry respective
// descriptions for all the error responses.
var stsErrCodes = stsErrorCodeMap{
	ErrSTSAccessDenied: {
		Code:           "AccessDenied",
		Description:    "Generating temporary credentials not allowed for this request.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrSTSMissingParameter: {
		Code:           "MissingParameter",
		Description:    "A required parameter for the specified action is not supplied.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSTSInvalidParameterValue: {
		Code:           "InvalidParameterValue",
		Description:    "An invalid or out-of-range value was supplied for the input parameter.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSTSWebIdentityExpiredToken: {
		Code:           "ExpiredToken",
		Description:    "The web identity token that was passed is expired or is not valid. Get a new identity token from the identity provider and then retry the request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSTSClientGrantsExpiredToken: {
		Code:           "ExpiredToken",
		Description:    "The client grants that was passed is expired or is not valid. Get a new client grants token from the identity provider and then retry the request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSTSInvalidClientGrantsToken: {
		Code:           "InvalidClientGrantsToken",
		Description:    "The client grants token that was passed could not be validated by MinIO.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSTSMalformedPolicyDocument: {
		Code:           "MalformedPolicyDocument",
		Description:    "The request was rejected because the policy document was malformed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSTSNotInitialized: {
		Code:           "STSNotInitialized",
		Description:    "STS API not initialized, please try again.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSTSInternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error generating credentials, please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	},
}
