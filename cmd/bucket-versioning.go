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
	"strings"

	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/logger"
)

// BucketVersioningSys - policy subsystem.
type BucketVersioningSys struct{}

// Enabled enabled versioning?
func (sys *BucketVersioningSys) Enabled(bucket string) bool {
	vc, err := sys.Get(bucket)
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	return vc.Enabled()
}

// PrefixEnabled returns true is versioning is enabled at bucket level and if
// the given prefix doesn't match any excluded prefixes pattern. This is
// part of a MinIO versioning configuration extension.
func (sys *BucketVersioningSys) PrefixEnabled(bucket, prefix string) bool {
	vc, err := sys.Get(bucket)
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	return vc.PrefixEnabled(prefix)
}

// Suspended suspended versioning?
func (sys *BucketVersioningSys) Suspended(bucket string) bool {
	vc, err := sys.Get(bucket)
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	return vc.Suspended()
}

// PrefixSuspended returns true if the given prefix matches an excluded prefix
// pattern. This is part of a MinIO versioning configuration extension.
func (sys *BucketVersioningSys) PrefixSuspended(bucket, prefix string) bool {
	vc, err := sys.Get(bucket)
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}

	return vc.PrefixSuspended(prefix)
}

// Get returns stored bucket policy
func (sys *BucketVersioningSys) Get(bucket string) (*versioning.Versioning, error) {
	if bucket == minioMetaBucket || strings.HasPrefix(bucket, minioMetaBucket) {
		return &versioning.Versioning{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/"}, nil
	}

	vcfg, _, err := globalBucketMetadataSys.GetVersioningConfig(bucket)
	return vcfg, err
}

// NewBucketVersioningSys - creates new versioning system.
func NewBucketVersioningSys() *BucketVersioningSys {
	return &BucketVersioningSys{}
}
