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

type TierGCS struct {
	Name         string
	Endpoint     string // custom endpoint is not supported for GCS
	Creds        string // base64 encoding of credentials.json
	Bucket       string
	Prefix       string
	Region       string
	StorageClass string
}

type GCSOptions func(*TierGCS) error

func GCSPrefix(prefix string) func(*TierGCS) error {
	return func(gcs *TierGCS) error {
		gcs.Prefix = prefix
		return nil
	}
}

func GCSRegion(region string) func(*TierGCS) error {
	return func(gcs *TierGCS) error {
		gcs.Region = region
		return nil
	}
}

func GCSStorageClass(sc string) func(*TierGCS) error {
	return func(gcs *TierGCS) error {
		gcs.StorageClass = sc
		return nil
	}
}

func (gcs *TierGCS) GetCredentialJSON() ([]byte, error) {
	return base64.URLEncoding.DecodeString(gcs.Creds)
}

func NewTierGCS(name string, credsJSON []byte, bucket string, options ...GCSOptions) (*TierConfig, error) {
	creds := base64.URLEncoding.EncodeToString(credsJSON)
	gcs := &TierGCS{
		Name:   name,
		Creds:  creds,
		Bucket: bucket,
		// Defaults
		// endpoint is meant only for client-side display purposes
		Endpoint:     "https://storage.googleapis.com/storage/v1/",
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
		Type: GCS,
		GCS:  gcs,
	}, nil
}
