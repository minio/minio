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
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"

	"github.com/minio/kes"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
)

// KMSStatusHandler - GET /minio/kms/v1/status
func (a kmsAPIHandlers) KMSStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSStatus")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSStatusAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	stat, err := GlobalKMS.Stat(ctx)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}

	status := madmin.KMSStatus{
		Name:         stat.Name,
		DefaultKeyID: stat.DefaultKey,
		Endpoints:    make(map[string]madmin.ItemState, len(stat.Endpoints)),
	}
	for _, endpoint := range stat.Endpoints {
		status.Endpoints[endpoint] = madmin.ItemOnline // TODO(aead): Implement an online check for mTLS
	}

	resp, err := json.Marshal(status)
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

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSMetricsAction)
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

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSAPIAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	manager, ok := GlobalKMS.(kms.StatusManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	apis, err := manager.APIs(ctx)
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

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSVersionAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	manager, ok := GlobalKMS.(kms.StatusManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	version, err := manager.Version(ctx)
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
	ctx := newContext(r, w, "KMSCreateKey")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSCreateKeyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	manager, ok := GlobalKMS.(kms.KeyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if err := manager.CreateKey(ctx, r.Form.Get("key-id")); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSDeleteKeyHandler - DELETE /minio/kms/v1/key/delete?key-id=<master-key-id>
func (a kmsAPIHandlers) KMSDeleteKeyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSDeleteKey")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSDeleteKeyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.KeyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	if err := manager.DeleteKey(ctx, r.Form.Get("key-id")); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSListKeysHandler - GET /minio/kms/v1/key/list?pattern=<pattern>
func (a kmsAPIHandlers) KMSListKeysHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSListKeys")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSListKeysAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.KeyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	keys, err := manager.ListKeys(ctx, r.Form.Get("pattern"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	values, err := keys.Values(0)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if res, err := json.Marshal(values); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, res)
	}
}

type importKeyRequest struct {
	Bytes string
}

// KMSImportKeyHandler - POST /minio/kms/v1/key/import?key-id=<master-key-id>
func (a kmsAPIHandlers) KMSImportKeyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSImportKey")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSImportKeyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.KeyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	var request importKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if err := manager.ImportKey(ctx, r.Form.Get("key-id"), []byte(request.Bytes)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSKeyStatusHandler - GET /minio/kms/v1/key/status?key-id=<master-key-id>
func (a kmsAPIHandlers) KMSKeyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSKeyStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSKeyStatusAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	stat, err := GlobalKMS.Stat(ctx)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}

	keyID := r.Form.Get("key-id")
	if keyID == "" {
		keyID = stat.DefaultKey
	}
	response := madmin.KMSKeyStatus{
		KeyID: keyID,
	}

	kmsContext := kms.Context{"MinIO admin API": "KMSKeyStatusHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, err := GlobalKMS.GenerateKey(ctx, keyID, kmsContext)
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
	decryptedKey, err := GlobalKMS.DecryptKey(key.KeyID, key.Ciphertext, kmsContext)
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

// KMSDescribePolicyHandler - GET /minio/kms/v1/policy/describe?policy=<policy>
func (a kmsAPIHandlers) KMSDescribePolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSDescribePolicy")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSDescribePolicyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.PolicyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	policy, err := manager.DescribePolicy(ctx, r.Form.Get("policy"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	p, err := json.Marshal(policy)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, p)
}

type assignPolicyRequest struct {
	Identity string
}

// KMSAssignPolicyHandler - POST /minio/kms/v1/policy/assign?policy=<policy>
func (a kmsAPIHandlers) KMSAssignPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSAssignPolicy")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSAssignPolicyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.PolicyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	var request assignPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	err := manager.AssignPolicy(ctx, r.Form.Get("policy"), request.Identity)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSSetPolicyHandler - POST /minio/kms/v1/policy/policy?policy=<policy>
func (a kmsAPIHandlers) KMSSetPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSSetPolicy")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSSetPolicyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.PolicyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	var policy kes.Policy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if err := manager.SetPolicy(ctx, r.Form.Get("policy"), &policy); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSDeletePolicyHandler - DELETE /minio/kms/v1/policy/delete?policy=<policy>
func (a kmsAPIHandlers) KMSDeletePolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSDeletePolicy")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSDeletePolicyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	manager, ok := GlobalKMS.(kms.PolicyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	if err := manager.DeletePolicy(ctx, r.Form.Get("policy")); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSListPoliciesHandler - GET /minio/kms/v1/policy/list?pattern=<pattern>
func (a kmsAPIHandlers) KMSListPoliciesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSListPolicies")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSListPoliciesAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.PolicyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	policies, err := manager.ListPolicies(ctx, r.Form.Get("pattern"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	values, err := policies.Values(0)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if res, err := json.Marshal(values); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, res)
	}
}

// KMSGetPolicyHandler - GET /minio/kms/v1/policy/get?policy=<policy>
func (a kmsAPIHandlers) KMSGetPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSGetPolicy")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSGetPolicyAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.PolicyManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	policy, err := manager.GetPolicy(ctx, r.Form.Get("policy"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if p, err := json.Marshal(policy); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, p)
	}
}

// KMSDescribeIdentityHandler - GET /minio/kms/v1/identity/describe?identity=<identity>
func (a kmsAPIHandlers) KMSDescribeIdentityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSDescribeIdentity")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSDescribeIdentityAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.IdentityManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	identity, err := manager.DescribeIdentity(ctx, r.Form.Get("identity"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	i, err := json.Marshal(identity)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, i)
}

type describeSelfIdentityResponse struct {
	Policy     *kes.Policy `json:"policy"`
	PolicyName string      `json:"policyName"`
	Identity   string      `json:"identity"`
	IsAdmin    bool        `json:"isAdmin"`
	CreatedAt  time.Time   `json:"createdAt"`
	CreatedBy  string      `json:"createdBy"`
}

// KMSDescribeSelfIdentityHandler - GET /minio/kms/v1/identity/describe-self
func (a kmsAPIHandlers) KMSDescribeSelfIdentityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSDescribeSelfIdentity")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSDescribeSelfIdentityAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.IdentityManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	identity, policy, err := manager.DescribeSelfIdentity(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	res := &describeSelfIdentityResponse{
		Policy:     policy,
		PolicyName: identity.Policy,
		Identity:   identity.Identity.String(),
		IsAdmin:    identity.IsAdmin,
		CreatedAt:  identity.CreatedAt,
		CreatedBy:  identity.CreatedBy.String(),
	}
	i, err := json.Marshal(res)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, i)
}

// KMSDeleteIdentityHandler - DELETE /minio/kms/v1/identity/delete?identity=<identity>
func (a kmsAPIHandlers) KMSDeleteIdentityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSDeleteIdentity")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSDeleteIdentityAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.IdentityManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if err := manager.DeleteIdentity(ctx, r.Form.Get("policy")); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSListIdentitiesHandler - GET /minio/kms/v1/identity/list?pattern=<pattern>
func (a kmsAPIHandlers) KMSListIdentitiesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSListIdentities")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSListIdentitiesAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}
	manager, ok := GlobalKMS.(kms.IdentityManager)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	identities, err := manager.ListIdentities(ctx, r.Form.Get("pattern"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	values, err := identities.Values(0)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if res, err := json.Marshal(values); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
	} else {
		writeSuccessResponseJSON(w, res)
	}
}
