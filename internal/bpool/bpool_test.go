// Copyright (c) 2015-2021 MinIO, Inc.
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
	"testing"
)

// Tests - bytePool functionality.
func TestBytePool(t *testing.T) {
	size := uint64(4)
	width := 1024
	capWidth := 2048

	bp := NewBytePoolCap(size, width, capWidth)

	// Check the width
	if bp.Width() != width {
		t.Fatalf("bytepool width invalid: got %v want %v", bp.Width(), width)
	}

	// Check with width cap
	if bp.WidthCap() != capWidth {
		t.Fatalf("bytepool capWidth invalid: got %v want %v", bp.WidthCap(), capWidth)
	}

	// Check that retrieved buffer are of the expected width
	b := bp.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}
	if cap(b) != capWidth {
		t.Fatalf("bytepool cap invalid: got %v want %v", cap(b), capWidth)
	}

	bp.Put(b)

	// Fill the pool beyond the capped pool size.
	for i := uint64(0); i < size*2; i++ {
		bp.Put(make([]byte, bp.w, bp.wcap))
	}

	b = bp.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}
	if cap(b) != capWidth {
		t.Fatalf("bytepool length invalid: got %v want %v", cap(b), capWidth)
	}

	bp.Put(b)

	// Check the size of the pool.
	if uint64(len(bp.c)) != size {
		t.Fatalf("bytepool size invalid: got %v want %v", len(bp.c), size)
	}

	// lets drain the buf channel first before we validate invalid buffers.
	for range size {
		bp.Get() // discard
	}

	// Try putting some invalid buffers into pool
	bp.Put(make([]byte, bp.w, bp.wcap-1)) // wrong capacity is rejected (less)
	bp.Put(make([]byte, bp.w, bp.wcap+1)) // wrong capacity is rejected (more)
	bp.Put(make([]byte, width))           // wrong capacity is rejected (very less)
	if len(bp.c) > 0 {
		t.Fatal("bytepool should have rejected invalid packets")
	}

	// Try putting a short slice into pool
	bp.Put(make([]byte, bp.w, bp.wcap)[:2])
	if len(bp.c) != 1 {
		t.Fatal("bytepool should have accepted short slice with sufficient capacity")
	}

	b = bp.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}

	// Close the channel.
	close(bp.c)
}
