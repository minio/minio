// +build !windows

/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
)

// AppendFile - appends the file "src" to the file "dst"
func AppendFile(dst string, src string, osync bool) error {
	flags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	if osync {
		flags = flags | os.O_SYNC
	}
	appendFile, err := os.OpenFile(dst, flags, 0666)
	if err != nil {
		return err
	}
	defer appendFile.Close()

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	// Allocate staging buffer.
	var buf = make([]byte, defaultAppendBufferSize)
	_, err = io.CopyBuffer(appendFile, srcFile, buf)
	return err
}
