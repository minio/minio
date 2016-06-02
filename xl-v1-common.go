/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import "path"

// getLoadBalancedQuorumDisks - fetches load balanced sufficiently
// randomized quorum disk slice.
func (xl xlObjects) getLoadBalancedQuorumDisks() (disks []StorageAPI) {
	// It is okay to have readQuorum disks.
	return xl.getLoadBalancedDisks()[:xl.readQuorum-1]
}

// getLoadBalancedDisks - fetches load balanced (sufficiently
// randomized) disk slice.
func (xl xlObjects) getLoadBalancedDisks() (disks []StorageAPI) {
	// Based on the random shuffling return back randomized disks.
	for _, i := range randInts(len(xl.storageDisks)) {
		disks = append(disks, xl.storageDisks[i-1])
	}
	return disks
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." {
			return false
		}
		if xl.isObject(bucket, p) {
			// If there is already a file at prefix "p" return error.
			return true
		}
		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

// isObject - returns `true` if the prefix is an object i.e if
// `xl.json` exists at the leaf, false otherwise.
func (xl xlObjects) isObject(bucket, prefix string) bool {
	for _, disk := range xl.getLoadBalancedQuorumDisks() {
		if disk == nil {
			continue
		}
		_, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
		if err != nil {
			return false
		}
		break
	}
	return true
}

// statPart - returns fileInfo structure for a successful stat on part file.
func (xl xlObjects) statPart(bucket, objectPart string) (fileInfo FileInfo, err error) {
	for _, disk := range xl.getLoadBalancedQuorumDisks() {
		if disk == nil {
			continue
		}
		fileInfo, err = disk.StatFile(bucket, objectPart)
		if err != nil {
			return FileInfo{}, err
		}
		break
	}
	return fileInfo, nil
}
