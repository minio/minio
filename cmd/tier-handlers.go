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
)

func (api adminAPIHandlers) AddTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddTier")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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
	if err := loadGlobalTransitionTierConfig(); err != nil {
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
	if err := loadGlobalTransitionTierConfig(); err != nil {
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
