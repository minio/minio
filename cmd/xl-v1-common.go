/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"path"

	"github.com/minio/minio/pkg/sync/errgroup"
)

// getLoadBalancedDisks - fetches load balanced (sufficiently randomized) disk slice.
func (xl xlObjects) getLoadBalancedDisks() (newDisks []StorageAPI) {
	disks := xl.getDisks()
	// Based on the random shuffling return back randomized disks.
	for _, i := range hashOrder(UTCNow().String(), len(disks)) {
		newDisks = append(newDisks, disks[i-1])
	}
	return newDisks
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." || p == SlashSeparator {
			return false
		}
		if xl.isObject(bucket, p) {
			// If there is already a file at prefix "p", return true.
			return true
		}
		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

// isObject - returns `true` if the prefix is an object i.e if
// `xl.json` exists at the leaf, false otherwise.
func (xl xlObjects) isObject(bucket, prefix string) (ok bool) {
	storageDisks := xl.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				return errDiskNotFound
			}
			// Check if 'prefix' is an object on this 'disk', else continue the check the next disk
			fi, err := storageDisks[index].StatFile(bucket, pathJoin(prefix, xlMetaJSONFile))
			if err != nil {
				return err
			}
			if fi.Size == 0 {
				return errCorruptedFormat
			}
			return nil
		}, index)
	}

	// NOTE: Observe we are not trying to read `xl.json` and figure out the actual
	// quorum intentionally, but rely on the default case scenario. Actual quorum
	// verification will happen by top layer by using getObjectInfo() and will be
	// ignored if necessary.
	readQuorum := getReadQuorum(len(storageDisks))

	return reduceReadQuorumErrs(GlobalContext, g.Wait(), objectOpIgnoredErrs, readQuorum) == nil
}
