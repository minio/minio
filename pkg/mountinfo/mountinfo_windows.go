// +build windows

/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package mountinfo

import (
	"os"
)

// CheckCrossDevice - check if any input path has multiple sub-mounts.
// this is a dummy function and returns nil for now.
func CheckCrossDevice(paths []string) error {
	return nil
}

// fileExists checks if specified file exists.
func fileExists(filename string) (bool, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// IsLikelyMountPoint determines if a directory is a mountpoint.
func IsLikelyMountPoint(file string) bool {
	stat, err := os.Lstat(file)
	if err != nil {
		return false
	}

	// If current file is a symlink, then it is a mountpoint.
	if stat.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(file)
		if err != nil {
			return false
		}
		exists, _ := fileExists(target)
		return exists
	}

	return false
}
