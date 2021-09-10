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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/madmin-go"
)

// WarmBackendGetOpts is used to express byte ranges within an object. The zero
// value represents the entire byte range of an object.
type WarmBackendGetOpts struct {
	startOffset int64
	length      int64
}

// WarmBackend provides interface to be implemented by remote tier backends
type WarmBackend interface {
	Put(ctx context.Context, object string, r io.Reader, length int64) (remoteVersionID, error)
	Get(ctx context.Context, object string, rv remoteVersionID, opts WarmBackendGetOpts) (io.ReadCloser, error)
	Remove(ctx context.Context, object string, rv remoteVersionID) error
	InUse(ctx context.Context) (bool, error)
}

const probeObject = "probeobject"

// checkWarmBackend checks if tier config credentials have sufficient privileges
// to perform all operations defined in the WarmBackend interface.
func checkWarmBackend(ctx context.Context, w WarmBackend) error {
	var empty bytes.Reader
	rv, err := w.Put(ctx, probeObject, &empty, 0)
	if err != nil {
		return tierPermErr{
			Op:  tierPut,
			Err: err,
		}
	}

	_, err = w.Get(ctx, probeObject, rv, WarmBackendGetOpts{})
	if err != nil {
		switch {
		case isErrBucketNotFound(err):
			return errTierBucketNotFound
		case isErrSignatureDoesNotMatch(err):
			return errTierInvalidCredentials
		default:
			return tierPermErr{
				Op:  tierGet,
				Err: err,
			}
		}
	}

	if err = w.Remove(ctx, probeObject, rv); err != nil {
		return tierPermErr{
			Op:  tierDelete,
			Err: err,
		}
	}
	return err
}

type tierOp uint8

const (
	_ tierOp = iota
	tierGet
	tierPut
	tierDelete
)

func (op tierOp) String() string {
	switch op {
	case tierGet:
		return "GET"
	case tierPut:
		return "PUT"
	case tierDelete:
		return "DELETE"
	}
	return "UNKNOWN"
}

type tierPermErr struct {
	Op  tierOp
	Err error
}

func (te tierPermErr) Error() string {
	return fmt.Sprintf("failed to perform %s %v", te.Op, te.Err)
}

func errIsTierPermError(err error) bool {
	var tpErr tierPermErr
	return errors.As(err, &tpErr)
}

// remoteVersionID represents the version id of an object in the remote tier.
// Its usage is remote tier cloud implementation specific.
type remoteVersionID string

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
