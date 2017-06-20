// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build amd64, !gccgo, !appengine

package poly1305

var useBMI2 = supportsBMI2()

//go:noescape
func supportsBMI2() bool

//go:noescape
func initialize(state *[7]uint64, key *[32]byte)

//go:noescape
func update(state *[7]uint64, msg []byte)

//go:noescape
func finalize(tag *[TagSize]byte, state *[7]uint64)

// Sum generates an authenticator for msg using a one-time key and returns the
// 16-byte result. Authenticating two different messages with the same key allows
// an attacker to forge messages at will.
func Sum(msg []byte, key [32]byte) [TagSize]byte {
	if len(msg) == 0 {
		msg = []byte{}
	}
	var out [TagSize]byte
	var state [7]uint64 // := uint64{ h0, h1, h2, r0, r1, pad0, pad1 }
	initialize(&state, &key)
	update(&state, msg)
	finalize(&out, &state)
	return out
}

// New returns a Hash computing the poly1305 sum.
// Notice that Poly1305 is insecure if one key is used twice.
func New(key [32]byte) *Hash {
	p := new(Hash)
	initialize(&(p.state), &key)
	return p
}

// Hash implements the poly1305 authenticator.
// Poly1305 cannot be used like common hash.Hash implementations,
// because of using a poly1305 key twice breaks its security.
type Hash struct {
	state [7]uint64 // := uint64{ h0, h1, h2, r0, r1, pad0, pad1 }

	buf  [TagSize]byte
	off  int
	done bool
}

// Size returns the number of bytes Sum will append.
func (p *Hash) Size() int { return TagSize }

// Write adds more data to the running Poly1305 hash.
// This function should return a non-nil error if a call
// to Write happens after a call to Sum. So it is not possible
// to compute the checksum and than add more data.
func (p *Hash) Write(msg []byte) (int, error) {
	if p.done {
		return 0, errWriteAfterSum
	}
	n := len(msg)

	if p.off > 0 {
		dif := TagSize - p.off
		if n <= dif {
			p.off += copy(p.buf[p.off:], msg)
			return n, nil
		}
		copy(p.buf[p.off:], msg[:dif])
		update(&(p.state), p.buf[:])
		msg = msg[dif:]
		p.off = 0
	}

	if nn := len(msg) & (^(TagSize - 1)); nn > 0 {
		update(&(p.state), msg[:nn])
		msg = msg[nn:]
	}

	if len(msg) > 0 {
		p.off += copy(p.buf[p.off:], msg)
	}

	return n, nil
}

// Sum appends the Pol1305 hash of the previously
// processed data to b and returns the resulting slice.
// It is safe to call this function multiple times.
func (p *Hash) Sum(b []byte) []byte {
	var out [TagSize]byte
	state := p.state

	if p.off > 0 {
		update(&state, p.buf[:p.off])
	}

	finalize(&out, &state)
	p.done = true
	return append(b, out[:]...)
}
