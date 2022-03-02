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

import "github.com/minio/madmin-go"

//go:generate msgp -file $GOFILE -unexported

type tierTypeV1 int

const (
	// Unsupported refers to remote tier backend that is not supported in this version
	unsupportedV1 = tierTypeV1(madmin.Unsupported)
	// S3 refers to AWS S3 compatible backend
	s3V1 = tierTypeV1(madmin.S3)
	// Azure refers to Azure Blob Storage
	azureV1 = tierTypeV1(madmin.Azure)
	// GCS refers to Google Cloud Storage
	gcsV1 = tierTypeV1(madmin.GCS)
)

// String returns the name of tt's remote tier backend.
func (tt tierTypeV1) String() string {
	return madmin.TierType(tt).String()
}

type tierConfigV1 struct {
	Version string
	Type    tierTypeV1        `json:",omitempty"`
	Name    string            `json:",omitempty"`
	S3      *madmin.TierS3    `json:",omitempty"`
	Azure   *madmin.TierAzure `json:",omitempty"`
	GCS     *madmin.TierGCS   `json:",omitempty"`
}

type tierConfigMgrV1 struct {
	Tiers map[string]tierConfigV1
}

//msgp:encode ignore tierTypeV1 tierConfigV1 tierConfigMgrV1
//msgp:marshal ignore tierTypeV1 tierConfigV1 tierConfigMgrV1

func (t *tierConfigV1) migrate() madmin.TierConfig {
	cfg := madmin.TierConfig{
		Version: madmin.TierConfigVer,
		Type:    madmin.TierType(t.Type),
		Name:    t.Name,
	}
	switch t.Type {
	case s3V1:
		cfg.S3 = t.S3
	case azureV1:
		cfg.Azure = t.Azure
	case gcsV1:
		cfg.GCS = t.GCS
	}
	return cfg
}
