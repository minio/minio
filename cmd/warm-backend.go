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
	"context"
	"io"

	"github.com/minio/minio/pkg/madmin"
)

type warmBackendGetOpts struct {
	startOffset int64
	length      int64
}

// WarmBackend provides interface to be implemented by remote backends
type WarmBackend interface {
	Put(ctx context.Context, object string, r io.Reader, length int64) error
	Get(ctx context.Context, object string, opts warmBackendGetOpts) (io.ReadCloser, error)
	Remove(ctx context.Context, object string) error
	InUse(ctx context.Context) (bool, error)
}

// checkWarmBackend checks if tier config credentials have sufficient privileges
// to perform all operations definde in the WarmBackend interface.
// FIXME: currently, we check only for Get.
func checkWarmBackend(ctx context.Context, w WarmBackend) error {
	// TODO: requires additional checks to ensure that WarmBackend
	// configuration has sufficient privileges to Put/Remove objects as well.
	_, err := w.Get(ctx, "probeobject", warmBackendGetOpts{})
	switch {
	case isErrObjectNotFound(err):
		return nil
	case isErrBucketNotFound(err):
		return errTierBucketNotFound
	case isErrSignatureDoesNotMatch(err):
		return errTierInvalidCredentials
	}
	return err
}

// newWarmBackend instantiates the tier type specific WarmBackend, runs
// checkWarmBackend on it.
func newWarmBackend(ctx context.Context, tier madmin.TierConfig) (d WarmBackend, err error) {
	switch tier.Type {
	case madmin.S3:
		d, err = newWarmBackendS3(*tier.S3)
	case madmin.Azure:
		d, err = newWarmBackendAzure(*tier.Azure)
	case madmin.GCS:
		d, err = newWarmBackendGCS(*tier.GCS)
	default:
		return nil, errTierTypeUnsupported
	}
	if err != nil {
		return nil, errTierTypeUnsupported
	}

	err = checkWarmBackend(ctx, d)
	if err != nil {
		return nil, err
	}
	return d, nil
}
