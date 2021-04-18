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

import "testing"

// Tests - bytePool functionality.
func TestBytePool(t *testing.T) {
	var size = 4
	var width = 10
	var capWidth = 16

	bufPool := NewBytePoolCap(size, width, capWidth)

	// Check the width
	if bufPool.Width() != width {
		t.Fatalf("bytepool width invalid: got %v want %v", bufPool.Width(), width)
	}

	// Check with width cap
	if bufPool.WidthCap() != capWidth {
		t.Fatalf("bytepool capWidth invalid: got %v want %v", bufPool.WidthCap(), capWidth)
	}

	// Check that retrieved buffer are of the expected width
	b := bufPool.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}
	if cap(b) != capWidth {
		t.Fatalf("bytepool length invalid: got %v want %v", cap(b), capWidth)
	}

	bufPool.Put(b)

	// Fill the pool beyond the capped pool size.
	for i := 0; i < size*2; i++ {
		bufPool.Put(make([]byte, bufPool.w))
	}

	b = bufPool.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}
	if cap(b) != capWidth {
		t.Fatalf("bytepool length invalid: got %v want %v", cap(b), capWidth)
	}

	bufPool.Put(b)

	// Close the channel so we can iterate over it.
	close(bufPool.c)

	// Check the size of the pool.
	if len(bufPool.c) != size {
		t.Fatalf("bytepool size invalid: got %v want %v", len(bufPool.c), size)
	}

	bufPoolNoCap := NewBytePoolCap(size, width, 0)
	// Check the width
	if bufPoolNoCap.Width() != width {
		t.Fatalf("bytepool width invalid: got %v want %v", bufPool.Width(), width)
	}

	// Check with width cap
	if bufPoolNoCap.WidthCap() != 0 {
		t.Fatalf("bytepool capWidth invalid: got %v want %v", bufPool.WidthCap(), 0)
	}
	b = bufPoolNoCap.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}
	if cap(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", cap(b), width)
	}
}
