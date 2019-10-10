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
	"bytes"
	"hash"
	"io"
)

// ErrContentMismatch is returned by Verify when
// the content hash checksum does not match the
// expected checksum computed when the data was
// created.
//
// ErrContentMismatch indicates that some bits
// of the content data got flipped.
const ErrContentMismatch errorType = "ecc: bitrot: content checksum does not match"

// Verify writes everything it reads from r to hash using the buffer and
// compares the computed hash value to the provided checksum.
// If the computed hash differs from checksum it returns ErrContentMismatch.
func Verify(r io.Reader, buffer []byte, hash hash.Hash, checksum []byte) error {
	if _, err := io.CopyBuffer(hash, r, buffer); err != nil {
		return err
	}
	if computed := hash.Sum(nil); !bytes.Equal(computed, checksum) {
		return ErrContentMismatch
	}
	return nil
}

// A Verifier verifies everything it reads from an underlying
// data stream. It returns ErrContentMismatch if it detects that
// the underlying data stream is invalid.
type Verifier interface {
	io.Reader
	io.WriterTo
	io.Closer
}

type nopVerifier struct{ io.ReadCloser }

// NOPVerifier is a verifier that does not actually
// verify whatever it reads. The verification step is
// implemented as a NOP.
//
// A NOPVerifier should be used when the content has
// been verified before but an API requires a Verifier.
func NOPVerifier(src io.ReadCloser) Verifier { return &nopVerifier{src} }

// WriteTo behaves as specified by the io.WriterTo interface.
// In particular, it writes everything it reads from an
// underlying data source to w.
func (v *nopVerifier) WriteTo(w io.Writer) (int64, error) { return io.Copy(w, v.ReadCloser) }
