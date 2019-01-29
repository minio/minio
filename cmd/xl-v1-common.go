/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

	"github.com/minio/minio/cmd/logger"
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

var xlTreeWalkIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound)

// isObject - returns `true` if the prefix is an object i.e if
// `xl.json` exists at the leaf, false otherwise.
func (xl xlObjects) isObject(bucket, prefix string) (ok bool) {
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		// Check if 'prefix' is an object on this 'disk', else continue the check the next disk
		_, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
		if err == nil {
			return true
		}
		// Ignore for file not found,  disk not found or faulty disk.
		if IsErrIgnored(err, xlTreeWalkIgnoredErrs...) {
			continue
		}
	} // Exhausted all disks - return false.
	return false
}

// isObjectDir returns if the specified path represents an empty directory.
func (xl xlObjects) isObjectDir(bucket, prefix string) (ok bool) {
	var objectDirs = make([]bool, len(xl.getDisks()))
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
			entries, err := disk.ListDir(bucket, prefix, 1)
			if err != nil {
				errs[index] = err
				return
			}
			objectDirs[index] = len(entries) == 0
		}(index, disk)
	}

	wg.Wait()

	readQuorum := len(xl.getDisks()) / 2

	err := reduceReadQuorumErrs(context.Background(), errs, xlTreeWalkIgnoredErrs, readQuorum)
	if err != nil {
		reqInfo := &logger.ReqInfo{BucketName: bucket}
		reqInfo.AppendTags("prefix", prefix)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return false
	}

	for _, ok := range objectDirs {
		if !ok {
			// We perhaps found some entries in a prefix.
			return false
		}
	}

	// Return true if and only if the all directories are empty.
	return true
}
