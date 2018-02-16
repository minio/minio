// Original work https://github.com/oxtoacart/bpool borrowed
// only bpool.go licensed under Apache 2.0.

// This file modifies original bpool.go to add one more option
// to provide []byte capacity for better GC management.

/*
 * Minio Cloud Storage (C) 2018 Minio, Inc.
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
