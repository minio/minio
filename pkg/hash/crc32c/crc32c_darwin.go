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

package crc32c

import (
	"hash/crc32"
	"io"
)

var castanagoliTable = crc32.MakeTable(crc32.Castagnoli)

/// Convenience functions

// Sum32 - single caller crc helper
func Sum32(buffer []byte) uint32 {
	crc := crc32.New(castanagoliTable)
	crc.Reset()
	crc.Write(buffer)
	return crc.Sum32()
}

// Sum - io.Reader based crc helper
func Sum(reader io.Reader) (uint32, error) {
	h := crc32.New(castanagoliTable)
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		h.Write(byteBuffer)
	}
	if err != io.EOF {
		return 0, err
	}
	return h.Sum32(), nil
}
