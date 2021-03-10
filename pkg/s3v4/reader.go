// MinIO Cloud Storage, (C) 2021 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3v4

import (
	"bufio"
	"errors"
	"io"
	"time"
)

// ChunkReader is an io.Reader that reads and verifies
// S3 streaming-signature V4 encoded data.
type ChunkReader struct {
	r io.Reader

	signingKey    []byte
	scope         string
	date          time.Time
	prevSignature []byte

	chunk  Chunk
	offset int
	err    error
}

// NewChunkReader returns a new ChunkReader that verifies
// everything it reads from the given io.Reader with the
// signingKey.
func NewChunkReader(r io.Reader, signingKey []byte, scope string, date time.Time, seedSignature []byte) *ChunkReader {
	return &ChunkReader{
		r:             bufio.NewReader(r),
		signingKey:    signingKey,
		scope:         scope,
		date:          date,
		prevSignature: seedSignature,
	}
}

// Reads returns up to len(p) data from the underlying
// io.Reader as specified by the io.Reader interface.
func (r *ChunkReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.offset > 0 {
		n = copy(p, r.chunk.payload[r.offset:])
		if len(p) == n {
			r.offset += n
			return n, nil
		}
		r.offset = 0
		p = p[n:]
	}
	if _, err = r.chunk.ReadFrom(r.r); err != nil {
		r.err = err
		return n, err
	}
	if !r.chunk.verify(r.signingKey, r.date, r.scope, r.prevSignature) {
		return n, errors.New("s3v4: invalid signature")
	}
	r.prevSignature = r.chunk.signature

	r.offset = copy(p, r.chunk.payload)
	n += r.offset
	return n, nil
}

// Close closes the underlying io.Reader
func (r *ChunkReader) Close() error { return nil }
