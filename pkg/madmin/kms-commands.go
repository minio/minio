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

package madmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
)

// GetKeyStatus requests status information about the key referenced by keyID
// from the KMS connected to a MinIO by performing a Admin-API request.
// It basically hits the `/minio/admin/v3/kms/key/status` API endpoint.
func (adm *AdminClient) GetKeyStatus(ctx context.Context, keyID string) (*KMSKeyStatus, error) {
	// GET /minio/admin/v3/kms/key/status?key-id=<keyID>
	qv := url.Values{}
	qv.Set("key-id", keyID)
	reqData := requestData{
		relPath:     adminAPIPrefix + "/kms/key/status",
		queryValues: qv,
	}

	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	if err != nil {
		return nil, err
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}
	var keyInfo KMSKeyStatus
	if err = json.NewDecoder(resp.Body).Decode(&keyInfo); err != nil {
		return nil, err
	}
	return &keyInfo, nil
}

// KMSKeyStatus contains some status information about a KMS master key.
// The MinIO server tries to access the KMS and perform encryption and
// decryption operations. If the MinIO server can access the KMS and
// all master key operations succeed it returns a status containing only
// the master key ID but no error.
type KMSKeyStatus struct {
	KeyID         string `json:"key-id"`
	EncryptionErr string `json:"encryption-error,omitempty"` // An empty error == success
	DecryptionErr string `json:"decryption-error,omitempty"` // An empty error == success
}
