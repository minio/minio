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

// BytePoolCap implements a leaky pool of []byte in the form of a bounded channel.
type BytePoolCap struct {
	c    chan []byte
	w    int
	wcap int
}

// NewBytePoolCap creates a new BytePool bounded to the given maxSize, with new
// byte arrays sized based on width.
func NewBytePoolCap(maxSize int, width int, capwidth int) (bp *BytePoolCap) {
	return &BytePoolCap{
		c:    make(chan []byte, maxSize),
		w:    width,
		wcap: capwidth,
	}
}

// Get gets a []byte from the BytePool, or creates a new one if none are
// available in the pool.
func (bp *BytePoolCap) Get() (b []byte) {
	select {
	case b = <-bp.c:
	// reuse existing buffer
	default:
		// create new buffer
		if bp.wcap > 0 {
			b = make([]byte, bp.w, bp.wcap)
		} else {
			b = make([]byte, bp.w)
		}
	}
	return
}

// Put returns the given Buffer to the BytePool.
func (bp *BytePoolCap) Put(b []byte) {
	select {
	case bp.c <- b:
		// buffer went back into pool
	default:
		// buffer didn't go back into pool, just discard
	}
}

// Width returns the width of the byte arrays in this pool.
func (bp *BytePoolCap) Width() (n int) {
	return bp.w
}

// WidthCap returns the cap width of the byte arrays in this pool.
func (bp *BytePoolCap) WidthCap() (n int) {
	return bp.wcap
}
