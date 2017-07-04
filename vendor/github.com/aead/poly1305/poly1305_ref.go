// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build !amd64 gccgo appengine nacl

package poly1305

import "encoding/binary"

const (
	msgBlock   = uint32(1 << 24)
	finalBlock = uint32(0)
)

// Sum generates an authenticator for msg using a one-time key and returns the
// 16-byte result. Authenticating two different messages with the same key allows
// an attacker to forge messages at will.
func Sum(msg []byte, key [32]byte) [TagSize]byte {
	var (
		h, r [5]uint32
		s    [4]uint32
	)
	var out [TagSize]byte

	initialize(&r, &s, &key)

	// process full 16-byte blocks
	n := len(msg) & (^(TagSize - 1))
	if n > 0 {
		update(msg[:n], msgBlock, &h, &r)
		msg = msg[n:]
	}
	if len(msg) > 0 {
		var block [TagSize]byte
		off := copy(block[:], msg)
		block[off] = 1
		update(block[:], finalBlock, &h, &r)
	}
	finalize(&out, &h, &s)
	return out
}

// New returns a hash.Hash computing the poly1305 sum.
// Notice that Poly1305 is insecure if one key is used twice.
func New(key [32]byte) *Hash {
	p := new(Hash)
	initialize(&(p.r), &(p.s), &key)
	return p
}

// Hash implements a Poly1305 writer interface.
// Poly1305 cannot be used like common hash.Hash implementations,
// because using a poly1305 key twice breaks its security.
// So poly1305.Hash does not support some kind of reset.
type Hash struct {
	h, r [5]uint32
	s    [4]uint32

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
		msg = msg[dif:]
		update(p.buf[:], msgBlock, &(p.h), &(p.r))
		p.off = 0
	}

	// process full 16-byte blocks
	if nn := len(msg) & (^(TagSize - 1)); nn > 0 {
		update(msg[:nn], msgBlock, &(p.h), &(p.r))
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
	h := p.h

	if p.off > 0 {
		var buf [TagSize]byte
		copy(buf[:], p.buf[:p.off])
		buf[p.off] = 1 // invariant: p.off < TagSize

		update(buf[:], finalBlock, &h, &(p.r))
	}

	finalize(&out, &h, &(p.s))
	p.done = true
	return append(b, out[:]...)
}

func initialize(r *[5]uint32, s *[4]uint32, key *[32]byte) {
	r[0] = binary.LittleEndian.Uint32(key[0:]) & 0x3ffffff
	r[1] = (binary.LittleEndian.Uint32(key[3:]) >> 2) & 0x3ffff03
	r[2] = (binary.LittleEndian.Uint32(key[6:]) >> 4) & 0x3ffc0ff
	r[3] = (binary.LittleEndian.Uint32(key[9:]) >> 6) & 0x3f03fff
	r[4] = (binary.LittleEndian.Uint32(key[12:]) >> 8) & 0x00fffff

	s[0] = binary.LittleEndian.Uint32(key[16:])
	s[1] = binary.LittleEndian.Uint32(key[20:])
	s[2] = binary.LittleEndian.Uint32(key[24:])
	s[3] = binary.LittleEndian.Uint32(key[28:])
}

func update(msg []byte, flag uint32, h, r *[5]uint32) {
	h0, h1, h2, h3, h4 := h[0], h[1], h[2], h[3], h[4]
	r0, r1, r2, r3, r4 := uint64(r[0]), uint64(r[1]), uint64(r[2]), uint64(r[3]), uint64(r[4])
	R1, R2, R3, R4 := r1*5, r2*5, r3*5, r4*5

	for len(msg) > 0 {
		// h += msg
		h0 += binary.LittleEndian.Uint32(msg[0:]) & 0x3ffffff
		h1 += (binary.LittleEndian.Uint32(msg[3:]) >> 2) & 0x3ffffff
		h2 += (binary.LittleEndian.Uint32(msg[6:]) >> 4) & 0x3ffffff
		h3 += (binary.LittleEndian.Uint32(msg[9:]) >> 6) & 0x3ffffff
		h4 += (binary.LittleEndian.Uint32(msg[12:]) >> 8) | flag

		// h *= r
		d0 := (uint64(h0) * r0) + (uint64(h1) * R4) + (uint64(h2) * R3) + (uint64(h3) * R2) + (uint64(h4) * R1)
		d1 := (d0 >> 26) + (uint64(h0) * r1) + (uint64(h1) * r0) + (uint64(h2) * R4) + (uint64(h3) * R3) + (uint64(h4) * R2)
		d2 := (d1 >> 26) + (uint64(h0) * r2) + (uint64(h1) * r1) + (uint64(h2) * r0) + (uint64(h3) * R4) + (uint64(h4) * R3)
		d3 := (d2 >> 26) + (uint64(h0) * r3) + (uint64(h1) * r2) + (uint64(h2) * r1) + (uint64(h3) * r0) + (uint64(h4) * R4)
		d4 := (d3 >> 26) + (uint64(h0) * r4) + (uint64(h1) * r3) + (uint64(h2) * r2) + (uint64(h3) * r1) + (uint64(h4) * r0)

		// h %= p
		h0 = uint32(d0) & 0x3ffffff
		h1 = uint32(d1) & 0x3ffffff
		h2 = uint32(d2) & 0x3ffffff
		h3 = uint32(d3) & 0x3ffffff
		h4 = uint32(d4) & 0x3ffffff

		h0 += uint32(d4>>26) * 5
		h1 += h0 >> 26
		h0 = h0 & 0x3ffffff

		msg = msg[TagSize:]
	}

	h[0], h[1], h[2], h[3], h[4] = h0, h1, h2, h3, h4
}

func finalize(out *[TagSize]byte, h *[5]uint32, s *[4]uint32) {
	h0, h1, h2, h3, h4 := h[0], h[1], h[2], h[3], h[4]

	// h %= p reduction
	h2 += h1 >> 26
	h1 &= 0x3ffffff
	h3 += h2 >> 26
	h2 &= 0x3ffffff
	h4 += h3 >> 26
	h3 &= 0x3ffffff
	h0 += 5 * (h4 >> 26)
	h4 &= 0x3ffffff
	h1 += h0 >> 26
	h0 &= 0x3ffffff

	// h - p
	t0 := h0 + 5
	t1 := h1 + (t0 >> 26)
	t2 := h2 + (t1 >> 26)
	t3 := h3 + (t2 >> 26)
	t4 := h4 + (t3 >> 26) - (1 << 26)
	t0 &= 0x3ffffff
	t1 &= 0x3ffffff
	t2 &= 0x3ffffff
	t3 &= 0x3ffffff

	// select h if h < p else h - p
	t_mask := (t4 >> 31) - 1
	h_mask := ^t_mask
	h0 = (h0 & h_mask) | (t0 & t_mask)
	h1 = (h1 & h_mask) | (t1 & t_mask)
	h2 = (h2 & h_mask) | (t2 & t_mask)
	h3 = (h3 & h_mask) | (t3 & t_mask)
	h4 = (h4 & h_mask) | (t4 & t_mask)

	// h %= 2^128
	h0 |= h1 << 26
	h1 = ((h1 >> 6) | (h2 << 20))
	h2 = ((h2 >> 12) | (h3 << 14))
	h3 = ((h3 >> 18) | (h4 << 8))

	// s: the s part of the key
	// tag = (h + s) % (2^128)
	t := uint64(h0) + uint64(s[0])
	h0 = uint32(t)
	t = uint64(h1) + uint64(s[1]) + (t >> 32)
	h1 = uint32(t)
	t = uint64(h2) + uint64(s[2]) + (t >> 32)
	h2 = uint32(t)
	t = uint64(h3) + uint64(s[3]) + (t >> 32)
	h3 = uint32(t)

	binary.LittleEndian.PutUint32(out[0:], h0)
	binary.LittleEndian.PutUint32(out[4:], h1)
	binary.LittleEndian.PutUint32(out[8:], h2)
	binary.LittleEndian.PutUint32(out[12:], h3)
}
