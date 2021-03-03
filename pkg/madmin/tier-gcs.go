/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
 *
 */

package madmin

import (
	"encoding/base64"
)

//go:generate msgp -file $GOFILE

// TierGCS represents the remote tier configuration for Google Cloud Storage
type TierGCS struct {
	Endpoint     string `json:",omitempty"` // custom endpoint is not supported for GCS
	Creds        string `json:",omitempty"` // base64 encoding of credentials.json
	Bucket       string `json:",omitempty"`
	Prefix       string `json:",omitempty"`
	Region       string `json:",omitempty"`
	StorageClass string `json:",omitempty"`
}

// GCSOptions supports NewTierGCS to take variadic options
type GCSOptions func(*TierGCS) error

// GCSPrefix helper to supply optional object prefix to NewTierGCS
func GCSPrefix(prefix string) func(*TierGCS) error {
	return func(gcs *TierGCS) error {
		gcs.Prefix = prefix
		return nil
	}
}

// GCSRegion helper to supply optional region to NewTierGCS
func GCSRegion(region string) func(*TierGCS) error {
	return func(gcs *TierGCS) error {
		gcs.Region = region
		return nil
	}
}

// GCSStorageClass helper to supply optional storage class to NewTierGCS
func GCSStorageClass(sc string) func(*TierGCS) error {
	return func(gcs *TierGCS) error {
		gcs.StorageClass = sc
		return nil
	}
}

// GetCredentialJSON method returns the credentials JSON bytes.
func (gcs *TierGCS) GetCredentialJSON() ([]byte, error) {
	return base64.URLEncoding.DecodeString(gcs.Creds)
}

// NewTierGCS returns a TierConfig of GCS type. Returns error if the given
// parameters are invalid like name is empty etc.
func NewTierGCS(name string, credsJSON []byte, bucket string, options ...GCSOptions) (*TierConfig, error) {
	if name == "" {
		return nil, ErrTierNameEmpty
	}
	creds := base64.URLEncoding.EncodeToString(credsJSON)
	gcs := &TierGCS{
		Creds:  creds,
		Bucket: bucket,
		// Defaults
		// endpoint is meant only for client-side display purposes
		Endpoint:     "https://storage.googleapis.com/",
		Prefix:       "",
		Region:       "",
		StorageClass: "",
	}

	for _, option := range options {
		err := option(gcs)
		if err != nil {
			return nil, err
		}
	}

	return &TierConfig{
		Version: TierConfigV1,
		Type:    GCS,
		Name:    name,
		GCS:     gcs,
	}, nil
}
