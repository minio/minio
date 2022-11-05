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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/cache"
	"github.com/minio/minio/internal/config/etcd"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	idplugin "github.com/minio/minio/internal/config/identity/plugin"
	polplugin "github.com/minio/minio/internal/config/policy/plugin"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
)

// DelConfigKVHandler - DELETE /minio/admin/v3/del-config-kv
func (a adminAPIHandlers) DelConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteConfigKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	password := cred.SecretKey
	kvBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	subSys, _, _, err := config.GetSubSys(string(kvBytes))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = cfg.DelFrom(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg, subSys); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	if err = saveServerConfig(ctx, objectAPI, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// freshly retrieve the config so that default values are loaded for reset config
	if cfg, err = getValidConfig(objectAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	dynamic := config.SubSystemsDynamic.Contains(subSys)
	if dynamic {
		applyDynamic(ctx, objectAPI, cfg, subSys, r, w)
	}
}

func applyDynamic(ctx context.Context, objectAPI ObjectLayer, cfg config.Config, subSys string,
	r *http.Request, w http.ResponseWriter,
) {
	// Apply dynamic values.
	if err := applyDynamicConfigForSubSys(GlobalContext, objectAPI, cfg, subSys); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.SignalConfigReload(subSys)
	// Tell the client that dynamic config was applied.
	w.Header().Set(madmin.ConfigAppliedHeader, madmin.ConfigAppliedTrue)
}

// SetConfigKVHandler - PUT /minio/admin/v3/set-config-kv
func (a adminAPIHandlers) SetConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetConfigKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	password := cred.SecretKey
	kvBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	dynamic, err := cfg.ReadConfig(bytes.NewReader(kvBytes))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	subSys, _, _, err := config.GetSubSys(string(kvBytes))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg, subSys); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	// Update the actual server config on disk.
	if err = saveServerConfig(ctx, objectAPI, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Write to the config input KV to history.
	if err = saveServerConfigHistory(ctx, objectAPI, kvBytes); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if dynamic {
		applyDynamic(ctx, objectAPI, cfg, subSys, r, w)
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetConfigKVHandler - GET /minio/admin/v3/get-config-kv?key={key}
//
// `key` can be one of three forms:
// 1. `subsys:target` -> request for config of a single subsystem and target pair.
// 2. `subsys:` -> request for config of a single subsystem and the default target.
// 3. `subsys` -> request for config of all targets for the given subsystem.
func (a adminAPIHandlers) GetConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetConfigKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	cfg := globalServerConfig.Clone()
	vars := mux.Vars(r)
	key := vars["key"]

	var subSys, target string
	{
		ws := strings.SplitN(key, madmin.SubSystemSeparator, 2)
		subSys = ws[0]
		if len(ws) == 2 {
			if ws[1] == "" {
				target = madmin.Default
			} else {
				target = ws[1]
			}
		}
	}

	subSysConfigs, err := cfg.GetSubsysInfo(subSys, target)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var s strings.Builder
	for _, subSysConfig := range subSysConfigs {
		subSysConfig.WriteTo(&s, false)
	}

	password := cred.SecretKey
	econfigData, err := madmin.EncryptData(password, []byte(s.String()))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

func (a adminAPIHandlers) ClearConfigHistoryKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ClearConfigHistoryKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	restoreID := vars["restoreId"]
	if restoreID == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	if restoreID == "all" {
		chEntries, err := listServerConfigHistory(ctx, objectAPI, false, -1)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		for _, chEntry := range chEntries {
			if err = delServerConfigHistory(ctx, objectAPI, chEntry.RestoreID); err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
		}
	} else if err := delServerConfigHistory(ctx, objectAPI, restoreID); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// RestoreConfigHistoryKVHandler - restores a config with KV settings for the given KV id.
func (a adminAPIHandlers) RestoreConfigHistoryKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RestoreConfigHistoryKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	restoreID := vars["restoreId"]
	if restoreID == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	kvBytes, err := readServerConfigHistory(ctx, objectAPI, restoreID)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if _, err = cfg.ReadConfig(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg, ""); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	if err = saveServerConfig(ctx, objectAPI, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	delServerConfigHistory(ctx, objectAPI, restoreID)
}

// ListConfigHistoryKVHandler - lists all the KV ids.
func (a adminAPIHandlers) ListConfigHistoryKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListConfigHistoryKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	count, err := strconv.Atoi(vars["count"])
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	chEntries, err := listServerConfigHistory(ctx, objectAPI, true, count)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(chEntries)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	password := cred.SecretKey
	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

// HelpConfigKVHandler - GET /minio/admin/v3/help-config-kv?subSys={subSys}&key={key}
func (a adminAPIHandlers) HelpConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HelpConfigKV")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)

	subSys := vars["subSys"]
	key := vars["key"]

	_, envOnly := r.Form["env"]

	rd, err := GetHelp(subSys, key, envOnly)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	json.NewEncoder(w).Encode(rd)
}

// SetConfigHandler - PUT /minio/admin/v3/config
func (a adminAPIHandlers) SetConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	password := cred.SecretKey
	kvBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	cfg := newServerConfig()
	if _, err = cfg.ReadConfig(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg, ""); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	// Update the actual server config on disk.
	if err = saveServerConfig(ctx, objectAPI, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Write to the config input KV to history.
	if err = saveServerConfigHistory(ctx, objectAPI, kvBytes); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetConfigHandler - GET /minio/admin/v3/config
// Get config.json of this minio setup.
func (a adminAPIHandlers) GetConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	cfg := globalServerConfig.Clone()

	var s strings.Builder
	hkvs := config.HelpSubSysMap[""]
	for _, hkv := range hkvs {
		// We ignore the error below, as we cannot get one.
		cfgSubsysItems, _ := cfg.GetSubsysInfo(hkv.Key, "")

		for _, item := range cfgSubsysItems {
			off := item.Config.Get(config.Enable) == config.EnableOff
			switch hkv.Key {
			case config.EtcdSubSys:
				off = !etcd.Enabled(item.Config)
			case config.CacheSubSys:
				off = !cache.Enabled(item.Config)
			case config.StorageClassSubSys:
				off = !storageclass.Enabled(item.Config)
			case config.PolicyPluginSubSys:
				off = !polplugin.Enabled(item.Config)
			case config.IdentityOpenIDSubSys:
				off = !openid.Enabled(item.Config)
			case config.IdentityLDAPSubSys:
				off = !xldap.Enabled(item.Config)
			case config.IdentityTLSSubSys:
				off = !globalSTSTLSConfig.Enabled
			case config.IdentityPluginSubSys:
				off = !idplugin.Enabled(item.Config)
			}
			item.WriteTo(&s, off)
		}
	}

	password := cred.SecretKey
	econfigData, err := madmin.EncryptData(password, []byte(s.String()))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}
