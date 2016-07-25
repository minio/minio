/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

func TestBytePool(t *testing.T) {
	size := int64(10)
	n := 16
	pool := NewBytePool(size, n)
	enBlocks := make([][]byte, n)

	// Allocates all the 16 byte slices in the pool.
	alloc := func() {
		for i := range enBlocks {
			var err error
			enBlocks[i], err = pool.Get()
			if err != nil {
				t.Fatal("expected nil, got", err)
			}
			// Make sure the slice length is as expected.
			if len(enBlocks[i]) != int(size) {
				t.Fatalf("expected size %d, got %d", len(enBlocks[i]), size)
			}
		}
	}

	// Allocate everything in the pool.
	alloc()
	// Any Get() will fail when the pool does not have any free buffer.
	_, err := pool.Get()
	if err == nil {
		t.Fatalf("expected %s, got nil", err)
	}
	// Reset - so that all the buffers are marked as unused.
	pool.Reset()
	// Allocation of all the buffers in the pool should succeed now.
	alloc()
}
