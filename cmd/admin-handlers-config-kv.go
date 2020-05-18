/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
 */

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
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/etcd"
	xldap "github.com/minio/minio/cmd/config/identity/ldap"
	"github.com/minio/minio/cmd/config/identity/openid"
	"github.com/minio/minio/cmd/config/policy/opa"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

func validateAdminReqConfigKV(ctx context.Context, w http.ResponseWriter, r *http.Request) (auth.Credentials, ObjectLayer) {
	// Get current object layer instance.
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return auth.Credentials{}, nil
	}

	// Validate request signature.
	cred, adminAPIErr := checkAdminRequestAuthType(ctx, r, iampolicy.ConfigUpdateAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return cred, nil
	}

	return cred, objectAPI
}

// DelConfigKVHandler - DELETE /minio/admin/v3/del-config-kv
func (a adminAPIHandlers) DelConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteConfigKV")

	defer logger.AuditLog(w, r, "DeleteConfigKV", mustGetClaimsFromToken(r))

	cred, objectAPI := validateAdminReqConfigKV(ctx, w, r)
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

	if err = cfg.DelFrom(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = saveServerConfig(ctx, objectAPI, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SetConfigKVHandler - PUT /minio/admin/v3/set-config-kv
func (a adminAPIHandlers) SetConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetConfigKV")

	defer logger.AuditLog(w, r, "SetConfigKV", mustGetClaimsFromToken(r))

	cred, objectAPI := validateAdminReqConfigKV(ctx, w, r)
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

	if _, err = cfg.ReadFrom(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg); err != nil {
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

	// Make sure to write backend is encrypted
	if globalConfigEncrypted {
		saveConfig(GlobalContext, objectAPI, backendEncryptedFile, backendEncryptedMigrationComplete)
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetConfigKVHandler - GET /minio/admin/v3/get-config-kv?key={key}
func (a adminAPIHandlers) GetConfigKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetConfigKV")

	defer logger.AuditLog(w, r, "GetConfigKV", mustGetClaimsFromToken(r))

	cred, objectAPI := validateAdminReqConfigKV(ctx, w, r)
	if objectAPI == nil {
		return
	}

	cfg := globalServerConfig
	if globalSafeMode {
		var err error
		cfg, err = getValidConfig(objectAPI)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	vars := mux.Vars(r)
	var buf = &bytes.Buffer{}
	cw := config.NewConfigWriteTo(cfg, vars["key"])
	if _, err := cw.WriteTo(buf); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	password := cred.SecretKey
	econfigData, err := madmin.EncryptData(password, buf.Bytes())
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

func (a adminAPIHandlers) ClearConfigHistoryKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ClearConfigHistoryKV")

	defer logger.AuditLog(w, r, "ClearConfigHistoryKV", mustGetClaimsFromToken(r))

	_, objectAPI := validateAdminReqConfigKV(ctx, w, r)
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
	} else {
		if err := delServerConfigHistory(ctx, objectAPI, restoreID); err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
}

// RestoreConfigHistoryKVHandler - restores a config with KV settings for the given KV id.
func (a adminAPIHandlers) RestoreConfigHistoryKVHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RestoreConfigHistoryKV")

	defer logger.AuditLog(w, r, "RestoreConfigHistoryKV", mustGetClaimsFromToken(r))

	_, objectAPI := validateAdminReqConfigKV(ctx, w, r)
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

	if _, err = cfg.ReadFrom(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg); err != nil {
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

	defer logger.AuditLog(w, r, "ListConfigHistoryKV", mustGetClaimsFromToken(r))

	cred, objectAPI := validateAdminReqConfigKV(ctx, w, r)
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

	defer logger.AuditLog(w, r, "HelpHistoryKV", mustGetClaimsFromToken(r))

	_, objectAPI := validateAdminReqConfigKV(ctx, w, r)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)

	subSys := vars["subSys"]
	key := vars["key"]

	_, envOnly := r.URL.Query()["env"]

	rd, err := GetHelp(subSys, key, envOnly)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	json.NewEncoder(w).Encode(rd)
	w.(http.Flusher).Flush()
}

// SetConfigHandler - PUT /minio/admin/v3/config
func (a adminAPIHandlers) SetConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetConfig")

	defer logger.AuditLog(w, r, "SetConfig", mustGetClaimsFromToken(r))

	cred, objectAPI := validateAdminReqConfigKV(ctx, w, r)
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
	if _, err = cfg.ReadFrom(bytes.NewReader(kvBytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = validateConfig(cfg); err != nil {
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

	// Make sure to write backend is encrypted
	if globalConfigEncrypted {
		saveConfig(GlobalContext, objectAPI, backendEncryptedFile, backendEncryptedMigrationComplete)
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetConfigHandler - GET /minio/admin/v3/config
// Get config.json of this minio setup.
func (a adminAPIHandlers) GetConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetConfig")

	defer logger.AuditLog(w, r, "GetConfig", mustGetClaimsFromToken(r))

	cred, objectAPI := validateAdminReqConfigKV(ctx, w, r)
	if objectAPI == nil {
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var s strings.Builder
	hkvs := config.HelpSubSysMap[""]
	var count int
	for _, hkv := range hkvs {
		count += len(cfg[hkv.Key])
	}
	for _, hkv := range hkvs {
		v := cfg[hkv.Key]
		for target, kv := range v {
			off := kv.Get(config.Enable) == config.EnableOff
			switch hkv.Key {
			case config.EtcdSubSys:
				off = !etcd.Enabled(kv)
			case config.CacheSubSys:
				off = !cache.Enabled(kv)
			case config.StorageClassSubSys:
				off = !storageclass.Enabled(kv)
			case config.KmsVaultSubSys:
				off = !crypto.EnabledVault(kv)
			case config.KmsKesSubSys:
				off = !crypto.EnabledKes(kv)
			case config.PolicyOPASubSys:
				off = !opa.Enabled(kv)
			case config.IdentityOpenIDSubSys:
				off = !openid.Enabled(kv)
			case config.IdentityLDAPSubSys:
				off = !xldap.Enabled(kv)
			}
			if off {
				s.WriteString(config.KvComment)
				s.WriteString(config.KvSpaceSeparator)
			}
			s.WriteString(hkv.Key)
			if target != config.Default {
				s.WriteString(config.SubSystemSeparator)
				s.WriteString(target)
			}
			s.WriteString(config.KvSpaceSeparator)
			s.WriteString(kv.String())
			count--
			if count > 0 {
				s.WriteString(config.KvNewline)
			}
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
