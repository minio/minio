package poly

import (
	"io"

	"github.com/aead/poly1305"
)

// Hash defines a common interface for polynomial authenticators.
type Hash interface {
	// Write adds more data to the running authenticator.
	// This function should return a non-nil error if a call
	// to Write happens after a call to Sum. So it is not possible
	// to compute the checksum and than add more data.
	io.Writer

	// Size returns the number of bytes Sum will append.
	Size() int

	// Sum computes and appends the hash to b and returns the resulting slice.
	// It is safe to call this function multiple times.
	Sum(b []byte) []byte
}

// NewPoly1305 returns a new Hash computing a poly1305 authenticator.
func NewPoly1305(key [32]byte) Hash {
	return poly1305.New(key)
}
