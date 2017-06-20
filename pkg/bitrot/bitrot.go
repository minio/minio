package bitrot

import (
	"fmt"
	"io"
	"strconv"
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

	// add algorithms here
	unknownAlgorithm
)

var keysizes = []int{
	Poly1305:   32,
	GHASH:      32,
	BLAKE2b512: 0,
	SHA256:     0,
}

var names = []string{
	Poly1305:   "poly1305",
	GHASH:      "ghash",
	BLAKE2b512: "blake2b",
	SHA256:     "sha256",
}

var hashes = make([]func([]byte) Hash, unknownAlgorithm)

// New returns a new Hash implementing the given algorithm.
// It returns an error if the given key cannot be used by the algorithm.
// New panics if the algorithm is unknown.
func (a Algorithm) New(key []byte) (Hash, error) {
	if keysize := a.KeySize(); keysize != len(key) {
		return nil, fmt.Errorf("bitrot: bad keysize %d for algorithm #%s", len(key), a)
	}
	f := hashes[a]
	if f != nil {
		return f(key), nil
	}
	panic("bitrot: requested algorithm  #" + strconv.Itoa(int(a)) + " is not available")
}

// KeySize returns the size of valid keys for the given algorithm in bytes.
// It panics if the algorithm is unknown.
func (a Algorithm) KeySize() int {
	if a >= 0 && a < unknownAlgorithm {
		return keysizes[a]
	}
	panic("bitrot: requested algorithm  #" + strconv.Itoa(int(a)) + " is not available")
}

// Available reports whether the given algorithm is registered.
func (a Algorithm) Available() bool {
	return a < unknownAlgorithm && hashes[a] != nil
}

// String returns a string representation of the algorithm.
// It panics if the algorithm is unknown.
func (a Algorithm) String() string {
	if a >= 0 && a < unknownAlgorithm {
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
func RegisterAlgorithm(alg Algorithm, f func([]byte) Hash) {
	if alg >= unknownAlgorithm {
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
	return unknownAlgorithm, fmt.Errorf("bitrot: algorithm #%s is unknown", s)
}

// Hash is the common interface for bitrot protection.
// A bitrot protection algorithm implementation must implement this interface.
type Hash interface {
	io.Writer

	Sum(b []byte) []byte
}
