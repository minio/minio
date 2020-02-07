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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// GetKeyStatus requests status information about the key referenced by keyID
// from the KMS connected to a MinIO by performing a Admin-API request.
// It basically hits the `/minio/admin/v2/kms/key/status` API endpoint.
func (adm *AdminClient) GetKeyStatus(keyID string) (*KMSKeyStatus, error) {
	// GET /minio/admin/v2/kms/key/status?key-id=<keyID>
	qv := url.Values{}
	qv.Set("key-id", keyID)
	reqData := requestData{
		relPath:     adminAPIPrefix + "/kms/key/status",
		queryValues: qv,
	}

	resp, err := adm.executeMethod("GET", reqData)
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

// RotateKeys performs a key rotation of the data encryption keys (DEK) derived by
// the KMS. The MinIO server will inspect all objects specified by req and requests
// a new data encryption key from the KMS for this object.
//
// RotateKeys returns a stream of newline-delimited JSON objects. Each object represents
// one successful key rotation.
func (adm *AdminClient) RotateKeys(req *KMSRotateKeyRequest) (io.ReadCloser, error) {
	content, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	reqData := requestData{
		relPath: adminAPIPrefix + "/kms/key/rotate",
		content: content,
	}

	resp, err := adm.executeMethod(http.MethodPost, reqData)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}
	return resp.Body, nil
}

// KMSRotateKeyRequest specifies how and for which objects the
// MinIO server should perform the key rotation.
type KMSRotateKeyRequest struct {
	Bucket string `json:"bucket"` // If not specified MinIO will use all buckets
	Prefix string `json:"prefix"` // The object prefix. If empty MinIO will rotate every object.

	KeyMapping map[string]string `json:"key-mapping"` // The mapping old-key -> new-key
	Recursive  bool              `json:"recursive"`   // Specifies whether the rotation should be recursive
	DryRun     bool              `json:"dry-run"`     // Specifies whether the rotation should be persistet
}

// KMSRotateKeyResponse represents one successful key rotation operation.
type KMSRotateKeyResponse struct {
	Bucket   string `json:"bucket"`
	Object   string `json:"object"`
	OldKeyID string `json:"old-key-id"`
	NewKeyID string `json:"new-key-id"`
}

// JSON returns the JSON representation of a KMSRotateKeyResponse as string.
func (r *KMSRotateKeyResponse) JSON() string {
	const format = `{"bucket":"%s","object":"%s","old-key-id":"%s","new-key-id":"%s"}`
	return fmt.Sprintf(format, r.Bucket, r.Object, r.OldKeyID, r.NewKeyID)
}
