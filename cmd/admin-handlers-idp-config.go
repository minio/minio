// Copyright (c) 2015-2022 MinIO, Inc.
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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
)

// List of implemented ID config types.
var idCfgTypes = set.CreateStringSet("openid")

// SetIdentityProviderCfg:
//
// PUT <admin-prefix>/id-cfg?type=openid&name=dex1
func (a adminAPIHandlers) SetIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetIdentityCfg")

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
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	cfgType := mux.Vars(r)["type"]
	if !idCfgTypes.Contains(cfgType) {
		// TODO: change this to invalid type error when implementation
		// is complete.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	var cfgDataBuilder strings.Builder
	switch cfgType {
	case "openid":
		fmt.Fprintf(&cfgDataBuilder, "identity_openid")
	}

	// Ensure body content type is opaque.
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/octet-stream" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	// Subsystem configuration name could be empty.
	cfgName := mux.Vars(r)["name"]
	if cfgName != "" {
		fmt.Fprintf(&cfgDataBuilder, "%s%s", config.SubSystemSeparator, cfgName)
	}

	fmt.Fprintf(&cfgDataBuilder, "%s%s", config.KvSpaceSeparator, string(reqBytes))

	cfgData := cfgDataBuilder.String()
	subSys, _, _, err := config.GetSubSys(cfgData)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	dynamic, err := cfg.ReadConfig(strings.NewReader(cfgData))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// IDP config is not dynamic. Sanity check.
	if dynamic {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
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
	if err = saveServerConfigHistory(ctx, objectAPI, []byte(cfgData)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetIdentityProviderCfg:
//
// GET <admin-prefix>/id-cfg?type=openid&name=dex_test
//
// GetIdentityProviderCfg returns a list of configured IDPs on the server if
// name is empty. If name is non-empty, returns the configuration details for
// the IDP of the given type and configuration name. The configuration name for
// the default ("un-named") configuration target is `_`.
func (a adminAPIHandlers) GetIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetIdentityProviderCfg")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	cfgType := mux.Vars(r)["type"]
	cfgName := r.Form.Get("name")
	password := cred.SecretKey

	if !idCfgTypes.Contains(cfgType) {
		// TODO: change this to invalid type error when implementation
		// is complete.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	// If no cfgName is provided, we list.
	if cfgName == "" {
		a.listIdentityProviders(ctx, w, r, cfgType, password)
		return
	}

	cfg := globalServerConfig.Clone()

	cfgInfos, err := globalOpenIDConfig.GetConfigInfo(cfg, cfgName)
	if err != nil {
		if err == openid.ErrProviderConfigNotFound {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchConfigTarget), r.URL)
			return
		}

		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	res := madmin.IDPConfig{
		Type: cfgType,
		Name: cfgName,
		Info: cfgInfos,
	}
	data, err := json.Marshal(res)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

func (a adminAPIHandlers) listIdentityProviders(ctx context.Context, w http.ResponseWriter, r *http.Request, cfgType, password string) {
	// var subSys string
	switch cfgType {
	case "openid":
		// subSys = config.IdentityOpenIDSubSys
	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	cfg := globalServerConfig.Clone()
	cfgList, err := globalOpenIDConfig.GetConfigList(cfg)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(cfgList)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

// DeleteIdentityProviderCfg:
//
// DELETE <admin-prefix>/id-cfg?type=openid&name=dex_test
func (a adminAPIHandlers) DeleteIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteIdentityProviderCfg")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	cfgType := mux.Vars(r)["type"]
	cfgName := mux.Vars(r)["name"]
	if !idCfgTypes.Contains(cfgType) {
		// TODO: change this to invalid type error when implementation
		// is complete.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	cfg := globalServerConfig.Clone()

	cfgInfos, err := globalOpenIDConfig.GetConfigInfo(cfg, cfgName)
	if err != nil {
		if err == openid.ErrProviderConfigNotFound {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchConfigTarget), r.URL)
			return
		}

		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	hasEnv := false
	for _, ci := range cfgInfos {
		if ci.IsCfg && ci.IsEnv {
			hasEnv = true
			break
		}
	}

	if hasEnv {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigEnvOverridden), r.URL)
		return
	}

	var subSys string
	switch cfgType {
	case "openid":
		subSys = config.IdentityOpenIDSubSys
	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	cfg, err = readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = cfg.DelKVS(fmt.Sprintf("%s:%s", subSys, cfgName)); err != nil {
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

	dynamic := config.SubSystemsDynamic.Contains(subSys)
	if dynamic {
		applyDynamic(ctx, objectAPI, cfg, subSys, r, w)
	}
}
