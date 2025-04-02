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
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
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
	// error returned when reserved internal names are used.
	errTierReservedName = AdminError{
		Code:       "XMinioAdminTierReserved",
		Message:    "Cannot use reserved tier name",
		StatusCode: http.StatusBadRequest,
	}
)

func (api adminAPIHandlers) AddTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, cred := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var cfg madmin.TierConfig
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(reqBytes, &cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var ignoreInUse bool
	if forceStr := r.Form.Get("force"); forceStr != "" {
		ignoreInUse, _ = strconv.ParseBool(forceStr)
	}

	// Disallow remote tiers with internal storage class names
	switch cfg.Name {
	case storageclass.STANDARD, storageclass.RRS:
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTierReservedName), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = globalTierConfigMgr.Add(ctx, cfg, ignoreInUse)
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
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	tiers := globalTierConfigMgr.ListTiers()
	data, err := json.Marshal(tiers)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.Header().Set(tierCfgRefreshAtHdr, globalTierConfigMgr.refreshedAt().String())
	writeSuccessResponseJSON(w, data)
}

func (api adminAPIHandlers) EditTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, cred := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
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
	json := jsoniter.ConfigCompatibleWithStandardLibrary
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

func (api adminAPIHandlers) RemoveTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}

	vars := mux.Vars(r)
	tier := vars["tier"]
	force := r.Form.Get("force") == "true"

	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Remove(ctx, tier, force); err != nil {
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

func (api adminAPIHandlers) VerifyTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	vars := mux.Vars(r)
	tier := vars["tier"]
	if err := globalTierConfigMgr.Verify(ctx, tier); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessNoContent(w)
}

func (api adminAPIHandlers) TierStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	dui, err := loadDataUsageFromBackend(ctx, objAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	tierStats := dui.tierStats()
	dailyStats := globalNotificationSys.GetLastDayTierStats(ctx)
	tierStats = dailyStats.addToTierInfo(tierStats)

	data, err := json.Marshal(tierStats)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}
