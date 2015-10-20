// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file of
// Golang project:
//    https://github.com/golang/go/blob/master/LICENSE

// Using this part of Minio codebase under the license
// Apache License Version 2.0 with modifications

// Package sha512 implements the SHA512 hash algorithms as defined
// in FIPS 180-2.
package sha512

import (
	"hash"
	"io"

	"github.com/minio/minio-xl/pkg/cpu"
)

// Size - The size of a SHA512 checksum in bytes.
const Size = 64

// BlockSize - The blocksize of SHA512 in bytes.
const BlockSize = 128

const (
	chunk = 128
	init0 = 0x6a09e667f3bcc908
	init1 = 0xbb67ae8584caa73b
	init2 = 0x3c6ef372fe94f82b
	init3 = 0xa54ff53a5f1d36f1
	init4 = 0x510e527fade682d1
	init5 = 0x9b05688c2b3e6c1f
	init6 = 0x1f83d9abfb41bd6b
	init7 = 0x5be0cd19137e2179
)

// digest represents the partial evaluation of a checksum.
type digest struct {
	h   [8]uint64
	x   [chunk]byte
	nx  int
	len uint64
}

func block(dig *digest, p []byte) {
	switch true {
	case cpu.HasAVX2() == true:
		blockAVX2(dig, p)
	case cpu.HasAVX() == true:
		blockAVX(dig, p)
	case cpu.HasSSE41() == true:
		blockSSE(dig, p)
	default:
		blockGeneric(dig, p)
	}
}

// Reset digest to its default value
func (d *digest) Reset() {
	d.h[0] = init0
	d.h[1] = init1
	d.h[2] = init2
	d.h[3] = init3
	d.h[4] = init4
	d.h[5] = init5
	d.h[6] = init6
	d.h[7] = init7
	d.nx = 0
	d.len = 0
}

// New returns a new hash.Hash computing the SHA512 checksum.
func New() hash.Hash {
	d := new(digest)
	d.Reset()
	return d
}

// Return output array byte size
func (d *digest) Size() int { return Size }

// Return blockSize
func (d *digest) BlockSize() int { return BlockSize }

// Write blocks
func (d *digest) Write(p []byte) (nn int, err error) {
	nn = len(p)
	d.len += uint64(nn)
	if d.nx > 0 {
		n := copy(d.x[d.nx:], p)
		d.nx += n
		if d.nx == chunk {
			block(d, d.x[:])
			d.nx = 0
		}
		p = p[n:]
	}
	if len(p) >= chunk {
		n := len(p) &^ (chunk - 1)
		block(d, p[:n])
		p = p[n:]
	}
	if len(p) > 0 {
		d.nx = copy(d.x[:], p)
	}
	return
}

// Calculate sha512
func (d *digest) Sum(in []byte) []byte {
	// Make a copy of d0 so that caller can keep writing and summing.
	d0 := *d
	hash := d0.checkSum()
	return append(in, hash[:]...)
}

// internal checksum calculation, returns [Size]byte
func (d *digest) checkSum() [Size]byte {
	// Padding.  Add a 1 bit and 0 bits until 112 bytes mod 128.
	len := d.len
	var tmp [128]byte
	tmp[0] = 0x80
	if len%128 < 112 {
		d.Write(tmp[0 : 112-len%128])
	} else {
		d.Write(tmp[0 : 128+112-len%128])
	}

	// Length in bits.
	len <<= 3
	for i := uint(0); i < 16; i++ {
		tmp[i] = byte(len >> (120 - 8*i))
	}
	d.Write(tmp[0:16])

	if d.nx != 0 {
		panic("d.nx != 0")
	}

	h := d.h[:]

	var digest [Size]byte
	for i, s := range h {
		digest[i*8] = byte(s >> 56)
		digest[i*8+1] = byte(s >> 48)
		digest[i*8+2] = byte(s >> 40)
		digest[i*8+3] = byte(s >> 32)
		digest[i*8+4] = byte(s >> 24)
		digest[i*8+5] = byte(s >> 16)
		digest[i*8+6] = byte(s >> 8)
		digest[i*8+7] = byte(s)
	}

	return digest
}

/// Convenience functions

// Sum512 - single caller sha512 helper
func Sum512(data []byte) []byte {
	var d digest
	d.Reset()
	d.Write(data)
	return d.Sum(nil)
}

// Sum - io.Reader based streaming sha512 helper
func Sum(reader io.Reader) ([]byte, error) {
	h := New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		h.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return h.Sum(nil), nil
}
