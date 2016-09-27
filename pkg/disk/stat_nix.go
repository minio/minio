// +build darwin dragonfly freebsd linux netbsd openbsd

/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

// GetInfo returns total and free bytes available in a directory, e.g. `/`.
func GetInfo(path string) (info Info, err error) {
	s := syscall.Statfs_t{}
	err = syscall.Statfs(path, &s)
	if err != nil {
		return Info{}, err
	}
	info = Info{}
	info.Total = int64(s.Bsize) * int64(s.Blocks)
	info.Free = int64(s.Bsize) * int64(s.Bavail)
	info.Files = int64(s.Files)
	info.Ffree = int64(s.Ffree)
	info.FSType, err = getFSType(path)
	if err != nil {
		return Info{}, err
	}
	return info, nil
}
