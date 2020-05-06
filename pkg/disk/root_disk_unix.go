// +build !windows

/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

import (
	"os"
	"syscall"
)

// IsRootDisk returns if diskPath belongs to root-disk, i.e the disk mounted at "/"
func IsRootDisk(diskPath string) (bool, error) {
	rootDisk := false
	diskInfo, err := os.Stat(diskPath)
	if err != nil {
		return false, err
	}
	rootInfo, err := os.Stat("/")
	if err != nil {
		return false, err
	}
	diskStat, diskStatOK := diskInfo.Sys().(*syscall.Stat_t)
	rootStat, rootStatOK := rootInfo.Sys().(*syscall.Stat_t)
	if diskStatOK && rootStatOK {
		if diskStat.Dev == rootStat.Dev {
			// Indicate if the disk path is on root disk. This is used to indicate the healing
			// process not to format the drive and end up healing it.
			rootDisk = true
		}
	}
	return rootDisk, nil
}
