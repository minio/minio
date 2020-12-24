/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
 *
 */

package cmd

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

var (
	// error returned when remote tier already exists
	errTierAlreadyExists = AdminError{
		Code:       "XMinioAdminTierAlreadyExists",
		Message:    "Specified remote tier already exists",
		StatusCode: http.StatusConflict,
	}
	// error returned when remote tier is not found
	errTierNotFound = AdminError{
		Code:       "XMinioAdminTierNotFound",
		Message:    "Specified remote tier was not found",
		StatusCode: http.StatusNotFound,
	}
	// error returned when remote tier name is not in uppercase
	errTierNameNotUppercase = AdminError{
		Code:       "XMinioAdminTierNameNotUpperCase",
		Message:    "Tier name must be in uppercase",
		StatusCode: http.StatusBadRequest,
	}
	// error returned when remote tier bucket is not found
	errTierBucketNotFound = AdminError{
		Code:       "XMinioAdminTierBucketNotFound",
		Message:    "Remote tier bucket not found",
		StatusCode: http.StatusBadRequest,
	}
	// error returned when remote tier credentials are invalid.
	errTierInvalidCredentials = AdminError{
		Code:       "XMinioAdminTierInvalidCredentials",
		Message:    "Invalid remote tier credentials",
		StatusCode: http.StatusBadRequest,
	}
)

func (api adminAPIHandlers) AddTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddTier")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	objectAPI, cred := validateAdminUsersReq(ctx, w, r, iampolicy.SetTierAction)
	if objectAPI == nil || globalNotificationSys == nil || globalTierConfigMgr == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var cfg madmin.TierConfig
	if err := json.Unmarshal(reqBytes, &cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = globalTierConfigMgr.Add(cfg)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = saveGlobalTierConfig()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.LoadTransitionTierConfig(ctx)

	writeSuccessNoContent(w)
}

func (api adminAPIHandlers) ListTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListTier")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.ListTierAction)
	if objectAPI == nil || globalNotificationSys == nil || globalTierConfigMgr == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	tiers := globalTierConfigMgr.ListTiers()
	data, err := json.Marshal(tiers)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, data)
}

func (api adminAPIHandlers) EditTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "EditTier")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	objectAPI, cred := validateAdminUsersReq(ctx, w, r, iampolicy.SetTierAction)
	if objectAPI == nil || globalNotificationSys == nil || globalTierConfigMgr == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	vars := mux.Vars(r)
	scName := vars["tier"]

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var creds madmin.TierCreds
	if err := json.Unmarshal(reqBytes, &creds); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Edit(scName, creds); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := saveGlobalTierConfig(); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.LoadTransitionTierConfig(ctx)

	writeSuccessNoContent(w)
}
