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

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	cfgldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/ldap"
	"github.com/minio/pkg/v3/policy"
)

func addOrUpdateIDPHandler(ctx context.Context, w http.ResponseWriter, r *http.Request, isUpdate bool) {
	objectAPI, cred := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	// Ensure body content type is opaque to ensure that request body has not
	// been interpreted as form data.
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/octet-stream" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	idpCfgType := mux.Vars(r)["type"]
	if !madmin.ValidIDPConfigTypes.Contains(idpCfgType) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigInvalidIDPType), r.URL)
		return
	}

	var subSys string
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		subSys = madmin.IdentityOpenIDSubSys
	case madmin.LDAPIDPCfg:
		subSys = madmin.IdentityLDAPSubSys
	}

	cfgName := mux.Vars(r)["name"]
	cfgTarget := madmin.Default
	if cfgName != "" {
		cfgTarget = cfgName
		if idpCfgType == madmin.LDAPIDPCfg && cfgName != madmin.Default {
			// LDAP does not support multiple configurations. So cfgName must be
			// empty or `madmin.Default`.
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigLDAPNonDefaultConfigName), r.URL)
			return
		}
	}

	// Check that this is a valid Create vs Update API call.
	s := globalServerConfig.Clone()
	if apiErrCode := handleCreateUpdateValidation(s, subSys, cfgTarget, isUpdate); apiErrCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(apiErrCode), r.URL)
		return
	}

	cfgData := ""
	{
		tgtSuffix := ""
		if cfgTarget != madmin.Default {
			tgtSuffix = config.SubSystemSeparator + cfgTarget
		}
		cfgData = subSys + tgtSuffix + config.KvSpaceSeparator + string(reqBytes)
	}

	cfg, err := readServerConfig(ctx, objectAPI, nil)
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
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), "", r.URL)
		return
	}

	if err = validateConfig(ctx, cfg, subSys); err != nil {
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

func handleCreateUpdateValidation(s config.Config, subSys, cfgTarget string, isUpdate bool) APIErrorCode {
	if cfgTarget != madmin.Default {
		// This cannot give an error at this point.
		subSysTargets, _ := s.GetAvailableTargets(subSys)
		subSysTargetsSet := set.CreateStringSet(subSysTargets...)
		if isUpdate && !subSysTargetsSet.Contains(cfgTarget) {
			return ErrAdminConfigIDPCfgNameDoesNotExist
		}
		if !isUpdate && subSysTargetsSet.Contains(cfgTarget) {
			return ErrAdminConfigIDPCfgNameAlreadyExists
		}

		return ErrNone
	}

	// For the default configuration name, since it will always be an available
	// target, we need to check if a configuration value has been set previously
	// to figure out if this is a valid create or update API call.

	// This cannot really error (FIXME: improve the type for GetConfigInfo)
	var cfgInfos []madmin.IDPCfgInfo
	switch subSys {
	case madmin.IdentityOpenIDSubSys:
		cfgInfos, _ = globalIAMSys.OpenIDConfig.GetConfigInfo(s, cfgTarget)
	case madmin.IdentityLDAPSubSys:
		cfgInfos, _ = globalIAMSys.LDAPConfig.GetConfigInfo(s, cfgTarget)
	}

	if len(cfgInfos) > 0 && !isUpdate {
		return ErrAdminConfigIDPCfgNameAlreadyExists
	}
	if len(cfgInfos) == 0 && isUpdate {
		return ErrAdminConfigIDPCfgNameDoesNotExist
	}
	return ErrNone
}

// AddIdentityProviderCfg: adds a new IDP config for openid/ldap.
//
// PUT <admin-prefix>/idp-cfg/openid/dex1 -> create named config `dex1`
//
// PUT <admin-prefix>/idp-cfg/openid/_ -> create (default) named config `_`
func (a adminAPIHandlers) AddIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	addOrUpdateIDPHandler(ctx, w, r, false)
}

// UpdateIdentityProviderCfg: updates an existing IDP config for openid/ldap.
//
// POST <admin-prefix>/idp-cfg/openid/dex1 -> update named config `dex1`
//
// POST <admin-prefix>/idp-cfg/openid/_ -> update (default) named config `_`
func (a adminAPIHandlers) UpdateIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	addOrUpdateIDPHandler(ctx, w, r, true)
}

// ListIdentityProviderCfg:
//
// GET <admin-prefix>/idp-cfg/openid -> lists openid provider configs.
func (a adminAPIHandlers) ListIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}
	password := cred.SecretKey

	idpCfgType := mux.Vars(r)["type"]
	if !madmin.ValidIDPConfigTypes.Contains(idpCfgType) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigInvalidIDPType), r.URL)
		return
	}

	var cfgList []madmin.IDPListItem
	var err error
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		cfg := globalServerConfig.Clone()
		cfgList, err = globalIAMSys.OpenIDConfig.GetConfigList(cfg)
	case madmin.LDAPIDPCfg:
		cfg := globalServerConfig.Clone()
		cfgList, err = globalIAMSys.LDAPConfig.GetConfigList(cfg)

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

// GetIdentityProviderCfg:
//
// GET <admin-prefix>/idp-cfg/openid/dex_test
func (a adminAPIHandlers) GetIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	idpCfgType := mux.Vars(r)["type"]
	cfgName := mux.Vars(r)["name"]
	password := cred.SecretKey

	if !madmin.ValidIDPConfigTypes.Contains(idpCfgType) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigInvalidIDPType), r.URL)
		return
	}

	cfg := globalServerConfig.Clone()
	var cfgInfos []madmin.IDPCfgInfo
	var err error
	switch idpCfgType {
	case madmin.OpenidIDPCfg:
		cfgInfos, err = globalIAMSys.OpenIDConfig.GetConfigInfo(cfg, cfgName)
	case madmin.LDAPIDPCfg:
		cfgInfos, err = globalIAMSys.LDAPConfig.GetConfigInfo(cfg, cfgName)
	}
	if err != nil {
		if errors.Is(err, openid.ErrProviderConfigNotFound) || errors.Is(err, cfgldap.ErrProviderConfigNotFound) {
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

// DeleteIdentityProviderCfg:
//
// DELETE <admin-prefix>/idp-cfg/openid/dex_test
func (a adminAPIHandlers) DeleteIdentityProviderCfg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
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
		cfgInfos, err := globalIAMSys.OpenIDConfig.GetConfigInfo(cfgCopy, cfgName)
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
		cfgInfos, err := globalIAMSys.LDAPConfig.GetConfigInfo(cfgCopy, cfgName)
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

	cfg, err := readServerConfig(ctx, objectAPI, nil)
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
	if err = validateConfig(ctx, cfg, subSys); err != nil {
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
	if err = saveServerConfig(ctx, objectAPI, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	dynamic := config.SubSystemsDynamic.Contains(subSys)
	if dynamic {
		applyDynamic(ctx, objectAPI, cfg, subSys, r, w)
	}
}
