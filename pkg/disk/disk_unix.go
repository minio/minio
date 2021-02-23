// +build !windows

/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

package disk

import (
	"syscall"
)

// SameDisk reports whether di1 and di2 describe the same disk.
func SameDisk(disk1, disk2 string) (bool, error) {
	st1 := syscall.Stat_t{}
	st2 := syscall.Stat_t{}

	if err := syscall.Stat(disk1, &st1); err != nil {
		return false, err
	}

	if err := syscall.Stat(disk2, &st2); err != nil {
		return false, err
	}

	return st1.Dev == st2.Dev, nil
}
