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
	"encoding/json"
	"errors"
	"log"
)

//go:generate msgp -file $GOFILE

// TierConfigV1 is the supported tier config version
const TierConfigV1 = "v1"

// TierType enumerates different remote tier backends.
type TierType int

const (
	// Unsupported refers to remote tier backend that is not supported in this version
	Unsupported TierType = iota
	// S3 refers to AWS S3 compatible backend
	S3
	// Azure refers to Azure Blob Storage
	Azure
	// GCS refers to Google Cloud Storage
	GCS
)

// String returns the name of tt's remote tier backend.
func (tt TierType) String() string {
	switch tt {
	case S3:
		return "s3"
	case Azure:
		return "azure"
	case GCS:
		return "gcs"
	}
	return "unsupported"
}

// MarshalJSON returns the canonical json representation of tt.
func (tt TierType) MarshalJSON() ([]byte, error) {
	typ := tt.String()
	return json.Marshal(typ)
}

// UnmarshalJSON parses the provided tier type string, failing unmarshal
// if data contains invalid tier type.
func (tt *TierType) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	newtt, err := NewTierType(s)
	if err != nil {
		return err
	}
	*tt = newtt
	return nil
}

// NewTierType creates TierType if scType is a valid tier type string, otherwise
// returns an error.
func NewTierType(scType string) (TierType, error) {
	switch scType {
	case S3.String():
		return S3, nil
	case Azure.String():
		return Azure, nil
	case GCS.String():
		return GCS, nil
	}

	return Unsupported, ErrTierTypeUnsupported
}

// TierConfig represents the different remote tier backend configurations
// supported. The specific backend is identified by the Type field. It has a
// Version field to allow for backwards-compatible extension in the future.
type TierConfig struct {
	Version string
	Type    TierType   `json:",omitempty"`
	Name    string     `json:",omitempty"`
	S3      *TierS3    `json:",omitempty"`
	Azure   *TierAzure `json:",omitempty"`
	GCS     *TierGCS   `json:",omitempty"`
}

var (
	// ErrTierNameEmpty "remote tier name empty"
	ErrTierNameEmpty = errors.New("remote tier name empty")
	// ErrTierInvalidConfig "invalid tier config"
	ErrTierInvalidConfig = errors.New("invalid tier config")
	// ErrTierInvalidConfigVersion "invalid tier config version"
	ErrTierInvalidConfigVersion = errors.New("invalid tier config version")
	// ErrTierTypeUnsupported "unsupported tier type"
	ErrTierTypeUnsupported = errors.New("unsupported tier type")
)

// Clone returns a copy of TierConfig with secret key/credentials redacted.
func (cfg *TierConfig) Clone() TierConfig {
	var (
		s3  TierS3
		az  TierAzure
		gcs TierGCS
	)
	switch cfg.Type {
	case S3:
		s3 = *cfg.S3
		s3.SecretKey = "REDACTED"
	case Azure:
		az = *cfg.Azure
		az.AccountKey = "REDACTED"
	case GCS:
		gcs = *cfg.GCS
		gcs.Creds = "REDACTED"
	}
	return TierConfig{
		Version: cfg.Version,
		Type:    cfg.Type,
		Name:    cfg.Name,
		S3:      &s3,
		Azure:   &az,
		GCS:     &gcs,
	}
}

// UnmarshalJSON unmarshals json value to ensure that Type field is filled in
// correspondence with the tier config supplied.
// See TestUnmarshalTierConfig for an example json.
func (cfg *TierConfig) UnmarshalJSON(b []byte) error {
	type tierConfig TierConfig
	var m tierConfig
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	if m.Version != TierConfigV1 {
		return ErrTierInvalidConfigVersion
	}

	switch m.Type {
	case S3:
		if m.S3 == nil {
			return ErrTierInvalidConfig
		}
	case Azure:
		if m.Azure == nil {
			return ErrTierInvalidConfig
		}
	case GCS:
		if m.GCS == nil {
			return ErrTierInvalidConfig
		}
	}

	if m.Name == "" {
		return ErrTierNameEmpty
	}

	*cfg = TierConfig(m)
	return nil
}

// Endpoint returns the remote tier backend endpoint.
func (cfg *TierConfig) Endpoint() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Endpoint
	case Azure:
		return cfg.Azure.Endpoint
	case GCS:
		return cfg.GCS.Endpoint
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

// Bucket returns the remote tier backend bucket.
func (cfg *TierConfig) Bucket() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Bucket
	case Azure:
		return cfg.Azure.Bucket
	case GCS:
		return cfg.GCS.Bucket
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

// Prefix returns the remote tier backend prefix.
func (cfg *TierConfig) Prefix() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Prefix
	case Azure:
		return cfg.Azure.Prefix
	case GCS:
		return cfg.GCS.Prefix
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

// Region returns the remote tier backend region.
func (cfg *TierConfig) Region() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Region
	case Azure:
		return cfg.Azure.Region
	case GCS:
		return cfg.GCS.Region
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}
