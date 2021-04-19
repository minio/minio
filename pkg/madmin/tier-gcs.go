// Copyright (c) 2015-2021 MinIO, Inc.
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
