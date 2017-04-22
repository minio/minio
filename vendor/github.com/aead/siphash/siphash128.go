// Copyright (c) 2017 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

package siphash

import (
	"encoding/binary"
	"hash"
)

// Sum128 returns the 128 bit authenticator for msg using a 128 bit secret key.
func Sum128(msg []byte, key *[KeySize]byte) [16]byte {
	k0 := binary.LittleEndian.Uint64(key[0:])
	k1 := binary.LittleEndian.Uint64(key[8:])

	var hVal [4]uint64
	hVal[0] = k0 ^ c0
	hVal[1] = k1 ^ c1 ^ 0xee
	hVal[2] = k0 ^ c2
	hVal[3] = k1 ^ c3

	n := len(msg)
	ctr := byte(n)

	if n >= BlockSize {
		n &= (^(BlockSize - 1))
		core(&hVal, msg[:n])
		msg = msg[n:]
	}

	var block [BlockSize]byte
	copy(block[:], msg)
	block[7] = ctr

	var out [16]byte
	finalize128(&out, &hVal, &block)
	return out
}

// New128 returns a hash.Hash computing the SipHash-128 checksum.
// This function returns a non-nil error if len(key) != 16.
func New128(key []byte) (hash.Hash, error) {
	if k := len(key); k != KeySize {
		return nil, KeySizeError(k)
	}
	h := new(digest128)
	h.key[0] = binary.LittleEndian.Uint64(key)
	h.key[1] = binary.LittleEndian.Uint64(key[8:])
	h.Reset()
	return h, nil
}

type digest128 digest64

func (d *digest128) BlockSize() int { return BlockSize }

func (d *digest128) Size() int { return 16 }

func (d *digest128) Reset() {
	d.hVal[0] = d.key[0] ^ c0
	d.hVal[1] = d.key[1] ^ c1 ^ 0xee
	d.hVal[2] = d.key[0] ^ c2
	d.hVal[3] = d.key[1] ^ c3

	d.off = 0
	d.ctr = 0
}

func (d *digest128) Write(p []byte) (n int, err error) {
	n = len(p)
	d.ctr += byte(n)

	if d.off > 0 {
		dif := BlockSize - d.off
		if n < dif {
			d.off += copy(d.block[d.off:], p)
			return
		}
		copy(d.block[d.off:], p[:dif])
		core(&(d.hVal), d.block[:])
		p = p[dif:]
		d.off = 0
	}
	if nn := len(p) &^ (BlockSize - 1); nn >= BlockSize {
		core(&(d.hVal), p[:nn])
		p = p[nn:]
	}
	if len(p) > 0 {
		d.off = copy(d.block[:], p)
	}
	return n, nil
}

func (d *digest128) Sum(sum []byte) []byte {
	hVal := d.hVal
	block := d.block
	for i := d.off; i < BlockSize-1; i++ {
		block[i] = 0
	}
	block[7] = d.ctr

	var out [16]byte
	finalize128(&out, &hVal, &block)
	return append(sum, out[:]...)
}
