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
 */

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
