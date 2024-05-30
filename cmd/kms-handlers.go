// Copyright (c) 2015-2023 MinIO, Inc.
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
	"crypto/subtle"
	"encoding/json"
	"net/http"

	"github.com/minio/kms-go/kes"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
)

// KMSStatusHandler - GET /minio/kms/v1/status
func (a kmsAPIHandlers) KMSStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSStatus")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSStatusAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	stat, err := GlobalKMS.Status(ctx)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	resp, err := json.Marshal(stat)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, resp)
}

// KMSMetricsHandler - POST /minio/kms/v1/metrics
func (a kmsAPIHandlers) KMSMetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSMetrics")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSMetricsAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	metrics, err := GlobalKMS.Metrics(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if res, err := json.Marshal(metrics); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, res)
	}
}

// KMSAPIsHandler - POST /minio/kms/v1/apis
func (a kmsAPIHandlers) KMSAPIsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSAPIs")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSAPIAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	apis, err := GlobalKMS.APIs(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if res, err := json.Marshal(apis); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, res)
	}
}

type versionResponse struct {
	Version string `json:"version"`
}

// KMSVersionHandler - POST /minio/kms/v1/version
func (a kmsAPIHandlers) KMSVersionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSVersion")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSVersionAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	version, err := GlobalKMS.Version(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	res := &versionResponse{Version: version}
	v, err := json.Marshal(res)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, v)
}

// KMSCreateKeyHandler - POST /minio/kms/v1/key/create?key-id=<master-key-id>
func (a kmsAPIHandlers) KMSCreateKeyHandler(w http.ResponseWriter, r *http.Request) {
	// If env variable MINIO_KMS_SECRET_KEY is populated, prevent creation of new keys
	ctx := newContext(r, w, "KMSCreateKey")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSCreateKeyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	if err := GlobalKMS.CreateKey(ctx, &kms.CreateKeyRequest{Name: r.Form.Get("key-id")}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSListKeysHandler - GET /minio/kms/v1/key/list?pattern=<pattern>
func (a kmsAPIHandlers) KMSListKeysHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSListKeys")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSListKeysAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	names, _, err := GlobalKMS.ListKeyNames(ctx, &kms.ListRequest{
		Prefix: r.Form.Get("pattern"),
	})
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	values := make([]kes.KeyInfo, 0, len(names))
	for _, name := range names {
		values = append(values, kes.KeyInfo{
			Name: name,
		})
	}
	if res, err := json.Marshal(values); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, res)
	}
}

// KMSKeyStatusHandler - GET /minio/kms/v1/key/status?key-id=<master-key-id>
func (a kmsAPIHandlers) KMSKeyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSKeyStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSKeyStatusAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	keyID := r.Form.Get("key-id")
	if keyID == "" {
		keyID = GlobalKMS.DefaultKey
	}
	response := madmin.KMSKeyStatus{
		KeyID: keyID,
	}

	kmsContext := kms.Context{"MinIO admin API": "KMSKeyStatusHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{Name: keyID, AssociatedData: kmsContext})
	if err != nil {
		response.EncryptionErr = err.Error()
		resp, err := json.Marshal(response)
		if err != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
			return
		}
		writeSuccessResponseJSON(w, resp)
		return
	}

	// 2. Verify that we can indeed decrypt the (encrypted) key
	decryptedKey, err := GlobalKMS.Decrypt(ctx, &kms.DecryptRequest{
		Name:           key.KeyID,
		Ciphertext:     key.Ciphertext,
		AssociatedData: kmsContext,
	})
	if err != nil {
		response.DecryptionErr = err.Error()
		resp, err := json.Marshal(response)
		if err != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
			return
		}
		writeSuccessResponseJSON(w, resp)
		return
	}

	// 3. Compare generated key with decrypted key
	if subtle.ConstantTimeCompare(key.Plaintext, decryptedKey) != 1 {
		response.DecryptionErr = "The generated and the decrypted data key do not match"
		resp, err := json.Marshal(response)
		if err != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
			return
		}
		writeSuccessResponseJSON(w, resp)
		return
	}

	resp, err := json.Marshal(response)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, resp)
}
