// +build !linux,!netbsd,!freebsd,!darwin

/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"os"
)

// OpenBSD and Windows not supported.
// On OpenBSD O_DIRECT is not supported
// On Windows there is no documentation on disabling O_DIRECT

func OpenFileDirectIO(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filePath, flag, perm)
}

func DisableDirectIO(f *os.File) error {
	return nil
}
