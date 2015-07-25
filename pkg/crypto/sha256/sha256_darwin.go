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

package sha256

import (
	"hash"
	"io"

	"crypto/sha256"
)

// Sum256 - single caller sha256 helper
func Sum256(data []byte) []byte {
	d := sha256.New()
	d.Write(data)
	return d.Sum(nil)
}

// Sum - io.Reader based streaming sha256 helper
func Sum(reader io.Reader) ([]byte, error) {
	d := sha256.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		d.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return d.Sum(nil), nil
}

// New returns a new hash.Hash computing SHA256.
func New() hash.Hash {
	return sha256.New()
}
