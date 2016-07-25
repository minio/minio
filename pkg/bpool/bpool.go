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

// Package bpool implements a fixed size pool of byte slices.
package bpool

import (
	"errors"
	"sync"
)

// ErrBpoolNoFree - Normally this error should never be returned, this error
// indicates a bug in the package consumer.
var ErrBpoolNoFree = errors.New("no free byte slice in pool")

// BytePool - temporary pool of byte slices.
type BytePool struct {
	buf  [][]byte // array of byte slices
	used []bool   // indicates if a buf[i] is in use
	size int64    // size of buf[i]
	mu   sync.Mutex
}

// Get - Returns an unused byte slice.
func (b *BytePool) Get() (buf []byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < len(b.used); i++ {
		if !b.used[i] {
			b.used[i] = true
			if b.buf[i] == nil {
				b.buf[i] = make([]byte, b.size)
			}
			return b.buf[i], nil
		}
	}
	return nil, ErrBpoolNoFree
}

// Reset - Marks all slices as unused.
func (b *BytePool) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < len(b.used); i++ {
		b.used[i] = false
	}
}

// NewBytePool - Returns new pool.
// size - length of each slice.
// n - number of slices in the pool.
func NewBytePool(size int64, n int) *BytePool {
	used := make([]bool, n)
	buf := make([][]byte, n)
	return &BytePool{buf, used, size, sync.Mutex{}}
}
