/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package disk

import "runtime"

// IsRootDisk returns if diskPath belongs to root-disk, i.e the disk mounted at "/"
func IsRootDisk(diskPath string, rootDisk string) (bool, error) {
	if runtime.GOOS == "windows" {
		// On windows this function is not implemented.
		return false, nil
	}
	info, err := GetInfo(diskPath)
	if err != nil {
		return false, err
	}
	rootInfo, err := GetInfo(rootDisk)
	if err != nil {
		return false, err
	}
	return SameDisk(info, rootInfo), nil
}
