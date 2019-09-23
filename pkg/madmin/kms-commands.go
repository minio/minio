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
// It basically hits the `/minio/admin/v1/kms/key/status` API endpoint.
func (adm *AdminClient) GetKeyStatus(keyID string) (*KMSKeyStatus, error) {
	// GET /minio/admin/v1/kms/key/status?key-id=<keyID>
	qv := url.Values{}
	qv.Set("key-id", keyID)
	reqData := requestData{
		relPath:     "/v1/kms/key/status",
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
	UpdateErr     string `json:"update-error,omitempty"`     // An empty error == success
	DecryptionErr string `json:"decryption-error,omitempty"` // An empty error == success
}

// RotateKeys performs a KMS key rotation operation.
// On success the server returns a list of KMSRotateResponses
// encoded as JSON values.
func (adm *AdminClient) RotateKeys(bucket, prefix string, recursive bool, oldKeyID, newKeyID string, dryRun bool) (io.ReadCloser, error) {
	reqBody, err := json.Marshal(KMSRotateKeyRequest{
		Bucket:    bucket,
		Prefix:    prefix,
		Recursive: recursive,
		OldKeyID:  oldKeyID,
		NewKeyID:  newKeyID,
		DryRun:    dryRun,
	})
	if err != nil {
		return nil, err
	}
	reqData := requestData{
		relPath: "/v1/kms/key/rotate",
		content: reqBody,
	}

	resp, err := adm.executeMethod(http.MethodPost, reqData)
	if err != nil {
		closeResponse(resp)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}
	return resp.Body, nil
}

// KMSRotateKeyRequest represents the request arguments for
// rotating the encryption keys of objects encrypted with a
// particular master key ID.
// If the KeyID is empty the server will use the currently configured
// default master key. If the bucket is empty the server will lookup
// all buckets (ListBucket API), first. An non-empty prefix causes the
// list operation to only show entries that match this prefix. The same
// is true for the KeyID.
type KMSRotateKeyRequest struct {
	Bucket    string `json:"bucket,omitempty"`     // If empty and recursive == true, operration applies to all buckets
	Prefix    string `json:"prefix,omitempty"`     // The object prefix
	OldKeyID  string `json:"old-key-id,omitempty"` // Only apply rotation on an object if its master key == OldKeyID
	NewKeyID  string `json:"new-key-id,omitempty"` // The new master key ID
	Recursive bool   `json:"recursive,omitempty"`  // Perform key rotation recursively
	DryRun    bool   `json:"dry-run,omitempty"`    // If true, perform key rotation but do not persist any changes
}

// KMSRotateKeyResponse represents the key rotation for one object
// at the S3 server. The server will answer a KMSRotateKeysReqeuest with a list
// (separated by new-line) of KMSRotateKeyResponses (encoded as JSON).
type KMSRotateKeyResponse struct {
	Bucket   string `json:"bucket"`
	Object   string `json:"object"`
	OldKeyID string `json:"old-key-id"`
	NewKeyID string `json:"new-key-id"`
}

// JSON returns the JSON representation of a KMSRotateKeyResponse
// as string.
func (r *KMSRotateKeyResponse) JSON() string {
	const fmtStr = `{"bucket":"%s","object":"%s","old-key-id":"%s","new-key-id":"%s"}`
	return fmt.Sprintf(fmtStr, r.Bucket, r.Object, r.OldKeyID, r.NewKeyID)
}
