/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package ecc

import "io"

// The Buffer implementation below is tightly coupled to
// the JoinedReaders implementation in this package.

// Buffer contains a set of data and parity shards.
// It translates between a flat memory and multi-dimensional
// slices (shards).
type Buffer struct {
	shards [][]byte
	data   [][]byte // view of shards containing actual data
	parity [][]byte // view of shards containing parity data

	offset int
}

// NewBuffer creates a new buffer from the given shards.
// It treats the first len(shards)-parity shards as
// actual data shards and the remaining as parity
// shards.
//
// NewBuffer assumes that each shards[i] has the
// same length and capcacity. In particular:
//  len(shards[i]) == cap(shards[i]) == len(shards[j]) == cap(shards[j])
func NewBuffer(shards [][]byte, parity int) *Buffer {
	if len(shards) > 0 {
		for _, shard := range shards {
			if len(shard) != cap(shard) {
				panic("ecc: Buffer requires that each shard has equal len and cap")
			}
			if len(shards[0]) != len(shard) {
				panic("ecc: Buffer requires that each shard has the same len")
			}
		}
	}
	return &Buffer{
		shards: shards,
		data:   shards[:len(shards)-parity],
		parity: shards[len(shards)-parity:],
	}
}

// IsDataMissing returns true if at least one
// data shard is marked as missing. A data shard
// is considered as not present if its length
// len(shard) == 0.
func (b *Buffer) IsDataMissing() bool {
	dataShards := len(b.shards) - len(b.parity)
	for _, shard := range b.shards[:dataShards] {
		if len(shard) == 0 {
			return true
		}
	}
	return false
}

// Empty marks the buffer as empty such that
// IsEmpty() returns true.
func (b *Buffer) Empty() { b.data = b.data[:0] }

// Remaining returns the number of bytes
// of the remaining (actual data). Therefore,
// it returns how many bytes can be copied
// from the buffer.
func (b *Buffer) Remaining() int {
	var s int
	offset := b.offset
	for i := range b.data {
		if offset > 0 {
			s += len(b.data[i]) - offset
			offset = 0
		} else {
			s += len(b.data[i])
		}
	}
	return s
}

// IsEmpty returns true if the buffer does not
// hold any actual data. In particular, copying
// an empty buffer to a slice (using CopyTo)
// copies no data.
func (b *Buffer) IsEmpty() bool { return len(b.data) == 0 }

// Reset resets the buffer to its initial state.
// In particular, a reseted buffer is not empty.
func (b *Buffer) Reset() {
	for i := range b.shards {
		b.shards[i] = b.shards[i][:cap(b.shards[i])]
	}
	b.data = b.shards[:len(b.shards)-len(b.parity)]
	b.parity = b.shards[len(b.data):]

	b.offset = 0
}

// Skip skips the next n (actual data) bytes hold
// by the buffer and returns the number of skipped
// bytes.
//
// If n is greater than the number of remaining
// data bytes hold by the buffer, Skip will only
// skip as many (actual data) bytes as available.
// In particular, trying to skip n > 0 bytes on
// an empty buffer is a NOP.
func (b *Buffer) Skip(n int) int {
	if n <= 0 || b.IsEmpty() {
		return 0
	}

	nn := n
	if b.offset > 0 {
		remaining := len(b.data[0]) - b.offset
		if n < remaining {
			b.offset += n
			return n
		}

		b.offset = 0
		n -= remaining
		if len(b.data) > 0 {
			b.data = b.data[1:]
		}
	}

	for n > 0 && len(b.data) > 0 {
		if n < len(b.data[0]) {
			b.offset += n
			return nn
		}
		n -= len(b.data[0])
		b.data = b.data[1:]
	}
	return nn - n
}

// Read tries to copy len(p) (actual data) bytes
// into p and behaves as specified by the io.Reader
// interface. If it returns io.EOF the buffer is
// empty.
func (b *Buffer) Read(p []byte) (n int, err error) {
	if b.offset > 0 {
		n = copy(p, b.data[0][b.offset:])
		if n == len(p) {
			b.offset += n
			return n, nil
		}
		b.offset = 0
		p = p[n:]

		if len(b.data) > 0 {
			b.data = b.data[1:]
		}
	}

	for len(p) > 0 && len(b.data) > 0 {
		nn := copy(p, b.data[0])
		n += nn
		if nn < len(b.data[0]) {
			b.offset = nn
			return n, nil
		}
		b.data = b.data[1:]
		p = p[nn:]
	}

	if len(b.data) == 0 {
		err = io.EOF
	}
	return n, err
}
