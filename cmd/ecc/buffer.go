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

// The Buffer implementation below is tightly coupled to
// the JoinedReaders implementation in this package.

// Buffer contains a set of data and parity shards.
// It translates between a flat memory and multi-dimensional
// slices (shards).
type Buffer struct {
	shards [][]byte
	data   [][]byte // view of shards containing actual data
	parity [][]byte // view of shards containing parity data

	buffer []byte
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
func (b *Buffer) Empty() *Buffer {
	b.offset = 0
	b.data = b.data[:0]
	return b
}

// IsEmpty returns true if the buffer does not
// hold any actual data. In particular, copying
// an empty buffer to a slice (using CopyTo)
// copies no data.
func (b *Buffer) IsEmpty() bool { return b.offset == 0 && len(b.data) == 0 }

// Reset resets the buffer to its initial state.
// In particular, a reseted buffer is not empty.
func (b *Buffer) Reset() *Buffer {
	for i := range b.shards {
		b.shards[i] = b.shards[i][:cap(b.shards[i])]
	}
	b.data = b.shards[:len(b.shards)-len(b.parity)]
	b.parity = b.shards[len(b.data):]

	b.buffer = nil
	b.offset = 0
	return b
}

// Skip skips the next n (actual data) bytes hold
// by the buffer. If n is greater than the number
// of remaining data bytes hold by the buffer, Skip
// will only skip as many (actual data) bytes as
// available. In particular, trying to skip n > 0
// bytes on an empty buffer is a NOP.
func (b *Buffer) Skip(n int) *Buffer {
	if n <= 0 || b.IsEmpty() {
		return b
	}
	if b.offset > 0 {
		remaining := len(b.buffer) - b.offset
		if n < remaining {
			b.offset += n
			return b
		}
		n -= remaining
		b.offset = 0
	}

	for len(b.data) > 0 {
		b.buffer = b.data[0]
		b.data = b.data[1:]
		if n < len(b.buffer) {
			b.offset += n
			return b
		}
		n -= len(b.buffer)
	}
	return b
}

// CopyTo tries to copy len(p) (actual data)
// bytes into p. It returns the number of bytes
// copied to p. If the buffer holds less than
// len(p) (actual data) bytes it copies as
// many bytes as it holds until the buffer is
// empty.
func (b *Buffer) CopyTo(p []byte) int {
	var n int
	if b.offset > 0 {
		n = copy(p, b.buffer[b.offset:])
		if n == len(p) {
			b.offset += n
			return n
		}
		b.offset = 0
		p = p[n:]
	}

	for len(b.data) > 0 {
		b.buffer = b.data[0]
		b.data = b.data[1:]

		nn := copy(p, b.buffer)
		n += nn
		if nn == len(p) {
			if nn != len(b.buffer) {
				b.offset = nn
			}
			return n
		}
		p = p[nn:]
	}
	return n
}
