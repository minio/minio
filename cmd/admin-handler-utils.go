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
	"errors"
	"net/http"

	"github.com/minio/kes"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	iampolicy "github.com/minio/pkg/iam/policy"
)

func validateAdminReq(ctx context.Context, w http.ResponseWriter, r *http.Request, action iampolicy.AdminAction) (ObjectLayer, auth.Credentials) {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return nil, auth.Credentials{}
	}

	// Validate request signature.
	cred, adminAPIErr := checkAdminRequestAuth(ctx, r, action, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return nil, cred
	}

	return objectAPI, cred
}

// AdminError - is a generic error for all admin APIs.
type AdminError struct {
	Code       string
	Message    string
	StatusCode int
}

func (ae AdminError) Error() string {
	return ae.Message
}

func toAdminAPIErr(ctx context.Context, err error) APIError {
	if err == nil {
		return noError
	}

	var apiErr APIError
	switch e := err.(type) {
	case iampolicy.Error:
		apiErr = APIError{
			Code:           "XMinioMalformedIAMPolicy",
			Description:    e.Error(),
			HTTPStatusCode: http.StatusBadRequest,
		}
	case config.Error:
		apiErr = APIError{
			Code:           "XMinioConfigError",
			Description:    e.Error(),
			HTTPStatusCode: http.StatusBadRequest,
		}
	case AdminError:
		apiErr = APIError{
			Code:           e.Code,
			Description:    e.Message,
			HTTPStatusCode: e.StatusCode,
		}
	default:
		switch {
		case errors.Is(err, errConfigNotFound):
			apiErr = APIError{
				Code:           "XMinioConfigError",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusNotFound,
			}
		case errors.Is(err, errIAMActionNotAllowed):
			apiErr = APIError{
				Code:           "XMinioIAMActionNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusForbidden,
			}
		case errors.Is(err, errIAMNotInitialized):
			apiErr = APIError{
				Code:           "XMinioIAMNotInitialized",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusServiceUnavailable,
			}
		case errors.Is(err, errPolicyInUse):
			apiErr = APIError{
				Code:           "XMinioAdminPolicyInUse",
				Description:    "The policy cannot be removed, as it is in use",
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, kes.ErrKeyExists):
			apiErr = APIError{
				Code:           "XMinioKMSKeyExists",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusConflict,
			}

		// Tier admin API errors
		case errors.Is(err, madmin.ErrTierNameEmpty):
			apiErr = APIError{
				Code:           "XMinioAdminTierNameEmpty",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, madmin.ErrTierInvalidConfig):
			apiErr = APIError{
				Code:           "XMinioAdminTierInvalidConfig",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, madmin.ErrTierInvalidConfigVersion):
			apiErr = APIError{
				Code:           "XMinioAdminTierInvalidConfigVersion",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, madmin.ErrTierTypeUnsupported):
			apiErr = APIError{
				Code:           "XMinioAdminTierTypeUnsupported",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errTierBackendInUse):
			apiErr = APIError{
				Code:           "XMinioAdminTierBackendInUse",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusConflict,
			}
		case errors.Is(err, errTierInsufficientCreds):
			apiErr = APIError{
				Code:           "XMinioAdminTierInsufficientCreds",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errIsTierPermError(err):
			apiErr = APIError{
				Code:           "XMinioAdminTierInsufficientPermissions",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		default:
			apiErr = errorCodes.ToAPIErrWithErr(toAdminAPIErrCode(ctx, err), err)
		}
	}
	return apiErr
}

// toAdminAPIErrCode - converts errErasureWriteQuorum error to admin API
// specific error.
func toAdminAPIErrCode(ctx context.Context, err error) APIErrorCode {
	switch err {
	case errErasureWriteQuorum:
		return ErrAdminConfigNoQuorum
	default:
		return toAPIErrorCode(ctx, err)
	}
}
