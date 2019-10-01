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

// ReedSolomon implements reed-solomon erasure
// en/decoding.
type ReedSolomon struct {
	reedsolomon.Encoder

	nData, nParity, blockSize int
}

// NewReedSolomon returns a new reed-solomon erasure
// en/decoder for the given number of data and parity
// shards. It also expects the size of the data blocks
// which must be en/decoded.
func NewReedSolomon(nData, nParity, blockSize int) (*ReedSolomon, error) {
	reedSolomon := &ReedSolomon{
		nData:     nData,
		nParity:   nParity,
		blockSize: blockSize,
	}

	shardSize := reedSolomon.ShardSize()
	enc, err := reedsolomon.New(nData, nParity, reedsolomon.WithAutoGoroutines(shardSize))
	if err != nil {
		return nil, err
	}
	reedSolomon.Encoder = enc
	return reedSolomon, nil
}

// DataShards returns the number of data shards.
func (r *ReedSolomon) DataShards() int { return r.nData }

// ParityShards returns the number of parity shards.
func (r *ReedSolomon) ParityShards() int { return r.nParity }

// BlockSize returns how large a continious (actual) data
// block must be.
func (r *ReedSolomon) BlockSize() int { return r.blockSize }

// ShardSize returns the size of each shard for the
// given block size.
func (r *ReedSolomon) ShardSize() int { return (r.BlockSize() + r.DataShards() - 1) / r.DataShards() }

// Decoder implements reedsolomon erasure decoding of
// a continuous data stream. Whenever some content data
// is missing, it tries to reconstruct it on the fly.
type Decoder struct {
	src    *JoinedReaders
	enc    *ReedSolomon
	buffer *Buffer

	skip int
	err  error
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
func NewDecoder(src *JoinedReaders, enc *ReedSolomon) (*Decoder, error) {
	// Split the buffer into data and parity shards.
	// Using a buffer with a capacity of 2*blockSize
	// is a small optimization to avoid reallocations
	// during spliting.
	// TODO(aead): measure impact
	shards, err := enc.Split(make([]byte, enc.BlockSize(), 2*enc.BlockSize()))
	if err != nil {
		return nil, err
	}

	buffer := NewBuffer(shards, enc.ParityShards())
	buffer.Empty()
	return &Decoder{
		src:    src,
		enc:    enc,
		buffer: buffer,
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
		n, _ := d.buffer.Read(p)
		return n, nil
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
		if err := d.enc.ReconstructData(d.buffer.shards); err != nil {
			d.err = err
			return 0, err
		}
	}

	if d.skip > 0 {
		n := d.buffer.Skip(d.skip)
		d.skip -= n
	}
	n, _ := d.buffer.Read(p)
	return n, nil
}

// Close closes all underlying data sources and behaves
// as specified by the io.Closer interface.
func (d *Decoder) Close() error { return d.src.Close() }

// Skip marks the next n (actual data) bytes as
// to be skipped and not returned during reading.
func (d *Decoder) Skip(n int) { d.skip += n }
