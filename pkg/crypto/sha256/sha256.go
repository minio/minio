// +build freebsd darwin windows 386 arm !cgo

/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
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

	"crypto/sha256"
)

// Size - The size of a SHA256 checksum in bytes.
const Size = 32

// BlockSize - The blocksize of SHA256 in bytes.
const BlockSize = 64

// New returns a new hash.Hash computing SHA256.
func New() hash.Hash {
	return sha256.New()
}

// Sum256 - single caller sha256 helper
func Sum256(data []byte) [Size]byte {
	return sha256.Sum256(data)
}
