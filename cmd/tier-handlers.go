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
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
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

	objAPI, cred := validateAdminUsersReq(ctx, w, r, iampolicy.SetTierAction)
	if objAPI == nil || globalNotificationSys == nil || globalTierConfigMgr == nil {
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
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(reqBytes, &cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = globalTierConfigMgr.Add(ctx, cfg)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = globalTierConfigMgr.Save(ctx, objAPI)
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

	objAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.ListTierAction)
	if objAPI == nil || globalNotificationSys == nil || globalTierConfigMgr == nil {
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

	objAPI, cred := validateAdminUsersReq(ctx, w, r, iampolicy.SetTierAction)
	if objAPI == nil || globalNotificationSys == nil || globalTierConfigMgr == nil {
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
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(reqBytes, &creds); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Edit(ctx, scName, creds); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Save(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.LoadTransitionTierConfig(ctx)

	writeSuccessNoContent(w)
}
