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
	"sync"
)

// getLoadBalancedDisks - fetches load balanced (sufficiently randomized) disk slice.
func (xl xlObjects) getLoadBalancedDisks() (disks []StorageAPI) {
	// Based on the random shuffling return back randomized disks.
	for _, i := range hashOrder(UTCNow().String(), len(xl.getDisks())) {
		disks = append(disks, xl.getDisks()[i-1])
	}
	return disks
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." || p == "/" {
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
	var errs = make([]error, len(xl.getDisks()))
	var wg sync.WaitGroup
	for index, disk := range xl.getDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Check if 'prefix' is an object on this 'disk', else continue the check the next disk
			fi, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
			if err != nil {
				errs[index] = err
				return
			}
			if fi.Size == 0 {
				errs[index] = errCorruptedFormat
				return
			}
		}(index, disk)
	}

	wg.Wait()

	// NOTE: Observe we are not trying to read `xl.json` and figure out the actual
	// quorum intentionally, but rely on the default case scenario. Actual quorum
	// verification will happen by top layer by using getObjectInfo() and will be
	// ignored if necessary.
	readQuorum := len(xl.getDisks()) / 2

	return reduceReadQuorumErrs(context.Background(), errs, objectOpIgnoredErrs, readQuorum) == nil
}
