// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package bpool

import (
	"github.com/klauspost/reedsolomon"
)

// BytePoolCap implements a leaky pool of []byte in the form of a bounded channel.
type BytePoolCap struct {
	c    chan []byte
	w    int
	wcap int
}

// NewBytePoolCap creates a new BytePool bounded to the given maxSize, with new
// byte arrays sized based on width.
func NewBytePoolCap(maxSize uint64, width int, capwidth int) (bp *BytePoolCap) {
	if capwidth <= 0 {
		panic("total buffer capacity must be provided")
	}
	if capwidth < 64 {
		panic("buffer capped with smaller than 64 bytes is not supported")
	}
	if width > capwidth {
		panic("minimum buffer length cannot be > capacity of the buffer")
	}
	return &BytePoolCap{
		c:    make(chan []byte, maxSize),
		w:    width,
		wcap: capwidth,
	}
}

// Populate - populates and pre-warms the byte pool, this function is non-blocking.
func (bp *BytePoolCap) Populate() {
	for _, buf := range reedsolomon.AllocAligned(cap(bp.c), bp.wcap) {
		bp.Put(buf[:bp.w])
	}
}

// Get gets a []byte from the BytePool, or creates a new one if none are
// available in the pool.
func (bp *BytePoolCap) Get() (b []byte) {
	if bp == nil {
		return nil
	}
	select {
	case b = <-bp.c:
		// reuse existing buffer
	default:
		// create new aligned buffer
		b = reedsolomon.AllocAligned(1, bp.wcap)[0][:bp.w]
	}
	return b
}

// Put returns the given Buffer to the BytePool.
func (bp *BytePoolCap) Put(b []byte) {
	if bp == nil {
		return
	}

	if cap(b) != bp.wcap {
		// someone tried to put back buffer which is not part of this buffer pool
		// we simply don't put this back into pool, a modified buffer provided
		// by this package is no more usable, callers make sure to not modify
		// the capacity of the buffer.
		return
	}

	select {
	case bp.c <- b[:bp.w]:
		// buffer went back into pool
	default:
		// buffer didn't go back into pool, just discard
	}
}

// Width returns the width of the byte arrays in this pool.
func (bp *BytePoolCap) Width() (n int) {
	if bp == nil {
		return 0
	}
	return bp.w
}

// WidthCap returns the cap width of the byte arrays in this pool.
func (bp *BytePoolCap) WidthCap() (n int) {
	if bp == nil {
		return 0
	}
	return bp.wcap
}

// CurrentSize returns current size of buffer pool
func (bp *BytePoolCap) CurrentSize() int {
	if bp == nil {
		return 0
	}
	return len(bp.c) * bp.w
}
