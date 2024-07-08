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
	"fmt"
	"net/http"

	"github.com/minio/kms-go/kes"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/policy"
)

// validateAdminReq will validate request against and return whether it is allowed.
// If any of the supplied actions are allowed it will be successful.
// If nil ObjectLayer is returned, the operation is not permitted.
// When nil ObjectLayer has been returned an error has always been sent to w.
func validateAdminReq(ctx context.Context, w http.ResponseWriter, r *http.Request, actions ...policy.AdminAction) (ObjectLayer, auth.Credentials) {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return nil, auth.Credentials{}
	}

	for _, action := range actions {
		// Validate request signature.
		cred, adminAPIErr := checkAdminRequestAuth(ctx, r, action, "")
		switch adminAPIErr {
		case ErrNone:
			return objectAPI, cred
		case ErrAccessDenied:
			// Try another
			continue
		default:
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
			return nil, cred
		}
	}
	writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
	return nil, auth.Credentials{}
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
	case policy.Error:
		apiErr = APIError{
			Code:           "XMinioMalformedIAMPolicy",
			Description:    e.Error(),
			HTTPStatusCode: http.StatusBadRequest,
		}
	case config.ErrConfigNotFound:
		apiErr = APIError{
			Code:           "XMinioConfigNotFoundError",
			Description:    e.Error(),
			HTTPStatusCode: http.StatusNotFound,
		}
	case config.ErrConfigGeneric:
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
	case SRError:
		apiErr = errorCodes.ToAPIErrWithErr(e.Code, e.Cause)
	case decomError:
		apiErr = APIError{
			Code:           "XMinioDecommissionNotAllowed",
			Description:    e.Err,
			HTTPStatusCode: http.StatusBadRequest,
		}
	default:
		switch {
		case errors.Is(err, errTooManyPolicies):
			apiErr = APIError{
				Code:           "XMinioAdminInvalidRequest",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errDecommissionAlreadyRunning):
			apiErr = APIError{
				Code:           "XMinioDecommissionNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errDecommissionComplete):
			apiErr = APIError{
				Code:           "XMinioDecommissionNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errDecommissionRebalanceAlreadyRunning):
			apiErr = APIError{
				Code:           "XMinioDecommissionNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errRebalanceDecommissionAlreadyRunning):
			apiErr = APIError{
				Code:           "XMinioRebalanceNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
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
		case errors.Is(err, errIAMServiceAccountNotAllowed):
			apiErr = APIError{
				Code:           "XMinioIAMServiceAccountNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errIAMNotInitialized):
			apiErr = APIError{
				Code:           "XMinioIAMNotInitialized",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusServiceUnavailable,
			}
		case errors.Is(err, errPolicyInUse):
			apiErr = APIError{
				Code:           "XMinioIAMPolicyInUse",
				Description:    "The policy cannot be removed, as it is in use",
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errSessionPolicyTooLarge):
			apiErr = APIError{
				Code:           "XMinioIAMServiceAccountSessionPolicyTooLarge",
				Description:    err.Error(),
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
		case errIsTierPermError(err):
			apiErr = APIError{
				Code:           "XMinioAdminTierInsufficientPermissions",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errTierInvalidConfig):
			apiErr = APIError{
				Code:           "XMinioAdminTierInvalidConfig",
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
	if errors.Is(err, errErasureWriteQuorum) {
		return ErrAdminConfigNoQuorum
	}
	return toAPIErrorCode(ctx, err)
}

// wraps export error for more context
func exportError(ctx context.Context, err error, fname, entity string) APIError {
	if entity == "" {
		return toAPIError(ctx, fmt.Errorf("error exporting %s with: %w", fname, err))
	}
	return toAPIError(ctx, fmt.Errorf("error exporting %s from %s with: %w", entity, fname, err))
}

// wraps import error for more context
func importError(ctx context.Context, err error, fname, entity string) APIError {
	if entity == "" {
		return toAPIError(ctx, fmt.Errorf("error importing %s with: %w", fname, err))
	}
	return toAPIError(ctx, fmt.Errorf("error importing %s from %s with: %w", entity, fname, err))
}

// wraps import error for more context
func importErrorWithAPIErr(ctx context.Context, apiErr APIErrorCode, err error, fname, entity string) APIError {
	if entity == "" {
		return errorCodes.ToAPIErrWithErr(apiErr, fmt.Errorf("error importing %s with: %w", fname, err))
	}
	return errorCodes.ToAPIErrWithErr(apiErr, fmt.Errorf("error importing %s from %s with: %w", entity, fname, err))
}
