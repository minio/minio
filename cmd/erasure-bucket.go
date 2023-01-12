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
	"errors"

	"github.com/minio/minio/internal/sync/errgroup"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

// markDelete creates a vol entry in .minio.sys/buckets/.deleted until site replication
// syncs the delete to peers
func (er erasureObjects) markDelete(ctx context.Context, bucket, prefix string) error {
	storageDisks := er.getDisks()
	g := errgroup.WithNErrs(len(storageDisks))
	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		if storageDisks[index] == nil {
			continue
		}
		g.Go(func() error {
			if err := storageDisks[index].MakeVol(ctx, pathJoin(bucket, prefix)); err != nil {
				if errors.Is(err, errVolumeExists) {
					return nil
				}
				return err
			}
			return nil
		}, index)
	}
	err := reduceWriteQuorumErrs(ctx, g.Wait(), bucketOpIgnoredErrs, er.defaultWQuorum())
	return toObjectErr(err, bucket)
}

// purgeDelete deletes vol entry in .minio.sys/buckets/.deleted after site replication
// syncs the delete to peers OR on a new MakeBucket call.
func (er erasureObjects) purgeDelete(ctx context.Context, bucket, prefix string) error {
	storageDisks := er.getDisks()
	g := errgroup.WithNErrs(len(storageDisks))
	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				return storageDisks[index].DeleteVol(ctx, pathJoin(bucket, prefix), true)
			}
			return errDiskNotFound
		}, index)
	}
	err := reduceWriteQuorumErrs(ctx, g.Wait(), bucketOpIgnoredErrs, er.defaultWQuorum())
	return toObjectErr(err, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (er erasureObjects) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (er erasureObjects) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (er erasureObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (er erasureObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported indicates whether erasureObjects implements tagging support.
func (er erasureObjects) IsTaggingSupported() bool {
	return true
}
