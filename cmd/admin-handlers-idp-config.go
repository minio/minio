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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
	"github.com/minio/pkg/ldap"
)

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

	idpCfgType := mux.Vars(r)["type"]
	if !madmin.ValidIDPConfigTypes.Contains(idpCfgType) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigInvalidIDPType), r.URL)
		return
	}

	var cfgDataBuilder strings.Builder
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		fmt.Fprintf(&cfgDataBuilder, "identity_openid")
	case madmin.LDAPIDPCfg:
		fmt.Fprintf(&cfgDataBuilder, "identity_ldap")
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
		if idpCfgType == madmin.LDAPIDPCfg {
			// LDAP does not support multiple configurations. So this must be
			// empty.
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
			return
		}

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

		var validationErr ldap.Validation
		if errors.As(err, &validationErr) {
			// If we got an LDAP validation error, we need to send appropriate
			// error message back to client (likely mc).
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigLDAPValidation),
				validationErr.FormatError(), r.URL)
			return
		}

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

	idpCfgType := mux.Vars(r)["type"]
	cfgName := r.Form.Get("name")
	password := cred.SecretKey

	if !madmin.ValidIDPConfigTypes.Contains(idpCfgType) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigInvalidIDPType), r.URL)
		return
	}

	// If no cfgName is provided, we list.
	if cfgName == "" {
		a.listIdentityProviders(ctx, w, r, idpCfgType, password)
		return
	}

	cfg := globalServerConfig.Clone()
	var cfgInfos []madmin.IDPCfgInfo
	var err error
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		cfgInfos, err = globalOpenIDConfig.GetConfigInfo(cfg, cfgName)
	case madmin.LDAPIDPCfg:
		cfgInfos, err = globalLDAPConfig.GetConfigInfo(cfg, cfgName)
	}
	if err != nil {
		if errors.Is(err, openid.ErrProviderConfigNotFound) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchConfigTarget), r.URL)
			return
		}

		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	res := madmin.IDPConfig{
		Type: idpCfgType,
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

func (a adminAPIHandlers) listIdentityProviders(ctx context.Context, w http.ResponseWriter, r *http.Request, idpCfgType, password string) {
	var cfgList []madmin.IDPListItem
	var err error
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		cfg := globalServerConfig.Clone()
		cfgList, err = globalOpenIDConfig.GetConfigList(cfg)
	case madmin.LDAPIDPCfg:
		cfg := globalServerConfig.Clone()
		cfgList, err = globalLDAPConfig.GetConfigList(cfg)

	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

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

	idpCfgType := mux.Vars(r)["type"]
	cfgName := mux.Vars(r)["name"]
	if !madmin.ValidIDPConfigTypes.Contains(idpCfgType) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigInvalidIDPType), r.URL)
		return
	}

	cfgCopy := globalServerConfig.Clone()
	var subSys string
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		subSys = config.IdentityOpenIDSubSys
		cfgInfos, err := globalOpenIDConfig.GetConfigInfo(cfgCopy, cfgName)
		if err != nil {
			if errors.Is(err, openid.ErrProviderConfigNotFound) {
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
	case madmin.LDAPIDPCfg:
		subSys = config.IdentityLDAPSubSys
		cfgInfos, err := globalLDAPConfig.GetConfigInfo(cfgCopy, cfgName)
		if err != nil {
			if errors.Is(err, openid.ErrProviderConfigNotFound) {
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
	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	cfgKey := fmt.Sprintf("%s:%s", subSys, cfgName)
	if cfgName == madmin.Default {
		cfgKey = subSys
	}
	if err = cfg.DelKVS(cfgKey); err != nil {
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
