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
	"sync"

	"github.com/dustin/go-humanize"
)

const (
	// Block size used for all internal operations version 1.

	// TLDR..
	// Not used anymore xl.meta captures the right blockSize
	// so blockSizeV2 should be used for all future purposes.
	// this value is kept here to calculate the max API
	// requests based on RAM size for existing content.
	blockSizeV1 = 10 * humanize.MiByte

	// Block size used in erasure coding version 2.
	blockSizeV2 = 1 * humanize.MiByte

	// Buckets meta prefix.
	bucketMetaPrefix = "buckets"

	// Deleted Buckets prefix.
	deletedBucketsPrefix = ".deleted"

	// ETag (hex encoded md5sum) of empty string.
	emptyETag = "d41d8cd98f00b204e9800998ecf8427e"
)

// Global object layer mutex, used for safely updating object layer.
var globalObjLayerMutex sync.RWMutex

// Global object layer, only accessed by globalObjectAPI.
var globalObjectAPI ObjectLayer

type storageOpts struct {
	cleanUp     bool
	healthCheck bool
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(endpoint Endpoint, opts storageOpts) (storage StorageAPI, err error) {
	if endpoint.IsLocal {
		storage, err := newXLStorage(endpoint, opts.cleanUp)
		if err != nil {
			return nil, err
		}
		return newXLStorageDiskIDCheck(storage, opts.healthCheck), nil
	}

	return newStorageRESTClient(endpoint, opts.healthCheck, globalGrid.Load())
}
