/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

// Package bitrot provides algorithms for detecting data corruption.
package bitrot

import (
	"fmt"
	"io"
	"strconv"
)

// Mode specifies how the Hash will be used - either for protection or for verification.
type Mode bool

const (
	// Protect indicates that the Hash is used for computing a checksum.
	Protect Mode = true
	// Verify indicates that the Hash is used for verifying a previously computed checksum.
	Verify Mode = !Protect
)

// Algorithm identifies a bitrot detection algorithm that is implemented in another
// package.
type Algorithm uint

const (
	// Poly1305 specifies the poly1305 polynomial authenticator
	Poly1305 Algorithm = iota

	// GHASH specifies the GHASH (AES-GCM) polynomial authenticator
	GHASH

	// BLAKE2b512 specifies the BLAKE2b-512 hash function
	BLAKE2b512

	// SHA256 specifies the SHA256 hash function
	SHA256

	// AESGCM specifies the AES-GCM AEAD cipher construction (AES-256)
	AESGCM

	// ChaCha20Poly1305 specifies the ChaCha20Poly1305 AEAD cipher construction
	ChaCha20Poly1305

	// add algorithms here

	// UnknownAlgorithm indicates an unknown/invalid algorithm
	UnknownAlgorithm
)

var keysizes = []int{
	Poly1305:         32,
	GHASH:            32,
	BLAKE2b512:       0,
	SHA256:           0,
	AESGCM:           32,
	ChaCha20Poly1305: 32,
}

var names = []string{
	Poly1305:         "poly1305",
	GHASH:            "ghash",
	BLAKE2b512:       "blake2b",
	SHA256:           "sha256",
	AESGCM:           "aes-gcm",
	ChaCha20Poly1305: "chacha20poly1305",
}

var hashes = make([]func([]byte, Mode) Hash, UnknownAlgorithm)

// New returns a new Hash implementing the given algorithm.
// It returns an error if the given key cannot be used by the algorithm.
// New panics if the algorithm is unknown.
func (a Algorithm) New(key []byte, mode Mode) (Hash, error) {
	if a.KeySize() != len(key) {
		return nil, fmt.Errorf("bitrot: bad keysize %d for algorithm #%s", len(key), a)
	}
	f := hashes[a]
	if f != nil {
		return f(key, mode), nil
	}
	panic("bitrot: requested algorithm  #" + strconv.Itoa(int(a)) + " is not available")
}

// KeySize returns the size of valid keys for the given algorithm in bytes.
// It panics if the algorithm is unknown.
func (a Algorithm) KeySize() int {
	if a < UnknownAlgorithm && int(a) < len(keysizes) {
		return keysizes[a]
	}
	panic("bitrot: requested algorithm  #" + strconv.Itoa(int(a)) + " is not available")
}

// Available reports whether the given algorithm is registered.
func (a Algorithm) Available() bool {
	return a < UnknownAlgorithm && hashes[a] != nil
}

// String returns a string representation of the algorithm.
// It panics if the algorithm is unknown.
func (a Algorithm) String() string {
	if a < UnknownAlgorithm && int(a) < len(names) {
		return names[a]
	}
	panic("bitrot: requested algorithm  #" + strconv.Itoa(int(a)) + " is not available")
}

// GenerateKey generates a valid key for the given algorithm. Therefore
// the given reader should return random data. It returns an error if the
// reader fails - in this case the key must not be used.
func (a Algorithm) GenerateKey(reader io.Reader) (key []byte, err error) {
	size := a.KeySize()
	if size > 0 {
		key = make([]byte, size)
		_, err = io.ReadFull(reader, key)
	}
	return
}

// RegisterAlgorithm registers a function that returns a new instance of the given
// hash function. This is intended to be called from the init function in
// packages that implement the hash function.
func RegisterAlgorithm(alg Algorithm, f func(key []byte, mode Mode) Hash) {
	if alg >= UnknownAlgorithm {
		panic("bitrot: cannot register unknown algorithm")
	}
	hashes[alg] = f
}

// AlgorithmFromString returns an algorithm based on the provided string representation.
// It returns an error if the given string does not match a known algorithm.
func AlgorithmFromString(s string) (Algorithm, error) {
	for i, v := range names {
		if s == v {
			return Algorithm(i), nil
		}
	}
	return UnknownAlgorithm, fmt.Errorf("bitrot: algorithm %s is unknown", s)
}

// Hash is the common interface for bitrot protection.
// A bitrot protection algorithm implementation must implement this interface.
type Hash interface {
	io.Writer

	Sum(b []byte) []byte
}
