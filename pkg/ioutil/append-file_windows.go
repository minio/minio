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

package ioutil

import (
	"io"
	"os"

	"github.com/minio/minio/pkg/lock"
)

// AppendFile - appends the file "src" to the file "dst"
func AppendFile(dst string, src string) error {
	appendFile, err := lock.Open(dst, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer appendFile.Close()

	srcFile, err := lock.Open(src, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	// Allocate staging buffer.
	var buf = make([]byte, defaultAppendBufferSize)
	_, err = io.CopyBuffer(appendFile, srcFile, buf)
	return err
}
