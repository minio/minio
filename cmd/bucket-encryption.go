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

package cmd

import (
	"errors"
	"io"

	sse "github.com/minio/minio/internal/bucket/encryption"
)

// BucketSSEConfigSys - in-memory cache of bucket encryption config
type BucketSSEConfigSys struct{}

// NewBucketSSEConfigSys - Creates an empty in-memory bucket encryption configuration cache
func NewBucketSSEConfigSys() *BucketSSEConfigSys {
	return &BucketSSEConfigSys{}
}

// Get - gets bucket encryption config for the given bucket.
func (sys *BucketSSEConfigSys) Get(bucket string) (*sse.BucketSSEConfig, error) {
	sseCfg, _, err := globalBucketMetadataSys.GetSSEConfig(bucket)
	return sseCfg, err
}

// validateBucketSSEConfig parses bucket encryption configuration and validates if it is supported by MinIO.
func validateBucketSSEConfig(r io.Reader) (*sse.BucketSSEConfig, error) {
	encConfig, err := sse.ParseBucketSSEConfig(r)
	if err != nil {
		return nil, err
	}

	if len(encConfig.Rules) == 1 {
		return encConfig, nil
	}

	return nil, errors.New("Unsupported bucket encryption configuration")
}
