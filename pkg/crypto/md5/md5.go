/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

package md5

import (
	"crypto/md5"
	"io"
)

// Sum - low memory footprint io.Reader based md5sum helper
func Sum(reader io.Reader) ([]byte, error) {
	hash := md5.New()
	var err error
	var length int
	for err == nil {
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		// While hash.Write() wouldn't mind a Nil byteBuffer
		// It is necessary for us to verify this and break
		if length == 0 {
			break
		}
		byteBuffer = byteBuffer[0:length]
		hash.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return hash.Sum(nil), nil
}
