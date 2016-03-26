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

package sha512

import (
	"hash"

	"crypto/sha512"
)

// Size - The size of a SHA512 checksum in bytes.
const Size = 64

// BlockSize - The blocksize of SHA512 in bytes.
const BlockSize = 128

// New returns a new hash.Hash computing SHA512.
func New() hash.Hash {
	return sha512.New()
}

// Sum512 - single caller sha512 helper
func Sum512(data []byte) [Size]byte {
	return sha512.Sum512(data)
}
