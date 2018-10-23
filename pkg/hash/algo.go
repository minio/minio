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

package hash

import (
	"crypto/sha256"
	"errors"
	"hash"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/sha3"
)

// Algorithm specifies a algorithm used for  protection.
type Algorithm uint

const (
	// SHA256 represents the SHA-256 hash function
	SHA256 Algorithm = 1 + iota

	// SHA3256 represents the SHA-3 256 bits hash function
	SHA3256

	// SHA3512 represents the SHA-3 512 bits hash function
	SHA3512

	// BLAKE2b512 represents the BLAKE2b-512 hash function
	BLAKE2b512
)

var algorithms = map[Algorithm]string{
	SHA256:     "sha256",
	SHA3256:    "sha3-256",
	SHA3512:    "sha3-512",
	BLAKE2b512: "blake2b",
}

var errUnsupportedAlgo = errors.New("Unsupported algorithm")

// New returns a new hash.Hash calculating the given  algorithm.
func (a Algorithm) New() hash.Hash {
	switch a {
	case SHA256:
		return sha256.New()
	case SHA3256:
		return sha3.New256()
	case SHA3512:
		return sha3.New512()
	case BLAKE2b512:
		b2, _ := blake2b.New512(nil) // New512 never returns an error if the key is nil
		return b2
	default:
		panic(errUnsupportedAlgo)
	}
}

// Available reports whether the given algorithm is available.
func (a Algorithm) Available() bool {
	_, ok := algorithms[a]
	return ok
}

// String returns the string identifier for a given  algorithm.
// If the algorithm is not supported String panics.
func (a Algorithm) String() string {
	name, ok := algorithms[a]
	if !ok {
		panic(errUnsupportedAlgo)
	}
	return name
}

// AlgorithmFromString returns a  algorithm from the given string representation.
// It returns 0 if the string representation does not match any supported algorithm.
// The zero value of a  algorithm is never supported.
func AlgorithmFromString(s string) (a Algorithm) {
	for alg, name := range algorithms {
		if name == s {
			return alg
		}
	}
	return
}
