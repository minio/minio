// +build !windows

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

package os

import (
	"os"
	"syscall"
)

const (
	ownerWritable  = 0700 // 111 000 000
	groupWritable  = 0070 // 000 111 000
	othersWritable = 0007 // 000 000 111
)

func isDirWritable(name string, info os.FileInfo) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(name, &stat); err != nil {
		return err
	}

	if uint32(os.Geteuid()) == stat.Uid {
		if info.Mode().Perm()&ownerWritable != ownerWritable {
			return os.ErrPermission
		}

		return nil
	}

	if uint32(os.Getegid()) == stat.Gid {
		if info.Mode().Perm()&groupWritable != groupWritable {
			return os.ErrPermission
		}

		return nil
	}

	if info.Mode().Perm()&othersWritable != othersWritable {
		return os.ErrPermission
	}

	return nil
}
