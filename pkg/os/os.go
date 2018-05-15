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

// Mkdir creates a new directory as similar as os.Mkdir() except it ignores
// already exists error.
func Mkdir(name string, perm os.FileMode) error {
	info, err := os.Stat(name)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		// As directory does not exist, create it.
		return os.Mkdir(name, perm)
	}

	if !info.IsDir() {
		return &os.PathError{Op: "mkdir", Path: name, Err: syscall.ENOTDIR}
	}

	return isDirWritable(name, info)
}
