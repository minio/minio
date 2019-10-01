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

import (
	"github.com/klauspost/reedsolomon"
)

// Decoder implements reedsolomon erasure decoding of
// a continuous data stream. Whenever some content data
// is missing, it tries to reconstruct it on the fly.
type Decoder struct {
	src    *JoinedReaders
	ecc    reedsolomon.Encoder
	buffer *Buffer
	err    error

	firstRead bool
}

// NewDecoder returns a new Decoder that reads from a set
// of data sources combined to JoinedReaders. Whenever,
// the JoinedReaders report that some data shards are missing
// it reconstructs the data using the reedsolomon encoder.
//
// The blockSize is the size of one erasure-coded block. The
// reedsolomon encoder en/decodes data in blocks (e.g 10 MB).
// Therefore, it splits a blockSize large buffer into N shards
// where N is the number of data disks. For more information
// refer to: https://godoc.org/github.com/klauspost/reedsolomon
//
// The parity is the number of parity shards. The total number
// of shards = the number of data sources = data shards + parity
// shards. The Decoder can recover data only if at most n = parity
// shards of all shards are missing.
func NewDecoder(src *JoinedReaders, enc reedsolomon.Encoder, blockSize, parity int) (*Decoder, error) {
	// Split the buffer into data and parity shards.
	// Using a buffer with a capacity of 2*blockSize
	// is a small optimization to avoid reallocations
	// during spliting.
	// TODO(aead): measure impact
	shards, err := enc.Split(make([]byte, blockSize, 2*blockSize))
	if err != nil {
		return nil, err
	}

	return &Decoder{
		src:    src,
		ecc:    enc,
		buffer: NewBuffer(shards, parity).Empty(),
	}, nil
}

// Read behaves as specified by the io.Reader
// interface and reads up to len(p) into p.
func (d *Decoder) Read(p []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}

	// This is only valid since we created an empty
	// buffer during NewDecoder. On the first read
	// the buffer must be "marked" as empty to
	// avoid copying before reading anything.
	// Alternatively, we could use a first-read flag
	// and only copy if we have read at least once from
	// src.
	if !d.buffer.IsEmpty() {
		return d.buffer.CopyTo(p), nil
	}

	// The JoinedReaders will reset the buffer before
	// reading data into it.
	if err := d.src.Read(d.buffer); err != nil {
		d.err = err
		return 0, err
	}

	// Only reconstruct (data, no parity shards) if one or more
	// data shards are missing. If a parity shard is missing we
	// don't care.
	if d.buffer.IsDataMissing() {
		if err := d.ecc.ReconstructData(d.buffer.shards); err != nil {
			d.err = err
			return 0, err
		}
	}
	return d.buffer.CopyTo(p), nil
}

// Close closes all underlying data sources and behaves
// as specified by the io.Closer interface.
func (d *Decoder) Close() error { return d.src.Close() }
