/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package hash

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"

	sha256 "github.com/minio/sha256-simd"
)

var errNestedReader = errors.New("Nesting of Reader detected, not allowed")

// Reader writes what it reads from an io.Reader to an MD5 and SHA256 hash.Hash.
// Reader verifies that the content of the io.Reader matches the expected checksums.
type Reader struct {
	src  io.Reader
	size int64

	md5sum, sha256sum   []byte // Byte values of md5sum, sha256sum of client sent values.
	md5Hash, sha256Hash hash.Hash
}

// NewReader returns a new hash Reader which computes the MD5 sum and
// SHA256 sum (if set) of the provided io.Reader at EOF.
func NewReader(src io.Reader, size int64, md5Hex, sha256Hex string) (*Reader, error) {
	if _, ok := src.(*Reader); ok {
		return nil, errNestedReader
	}

	sha256sum, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return nil, SHA256Mismatch{}
	}

	md5sum, err := hex.DecodeString(md5Hex)
	if err != nil {
		return nil, BadDigest{}
	}

	var sha256Hash hash.Hash
	if len(sha256sum) != 0 {
		sha256Hash = sha256.New()
	}

	return &Reader{
		md5sum:     md5sum,
		sha256sum:  sha256sum,
		src:        io.LimitReader(src, size),
		size:       size,
		md5Hash:    md5.New(),
		sha256Hash: sha256Hash,
	}, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	if n > 0 {
		r.md5Hash.Write(p[:n])
		if r.sha256Hash != nil {
			r.sha256Hash.Write(p[:n])
		}
	}

	// At io.EOF verify if the checksums are right.
	if err == io.EOF {
		if cerr := r.Verify(); cerr != nil {
			return 0, cerr
		}
	}

	return
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (r *Reader) Size() int64 { return r.size }

// MD5 - returns byte md5 value
func (r *Reader) MD5() []byte {
	return r.md5sum
}

// MD5Current - returns byte md5 value of the current state
// of the md5 hash after reading the incoming content.
// NOTE: Calling this function multiple times might yield
// different results if they are intermixed with Reader.
func (r *Reader) MD5Current() []byte {
	return r.md5Hash.Sum(nil)
}

// SHA256 - returns byte sha256 value
func (r *Reader) SHA256() []byte {
	return r.sha256sum
}

// MD5HexString returns hex md5 value.
func (r *Reader) MD5HexString() string {
	return hex.EncodeToString(r.md5sum)
}

// MD5Base64String returns base64 encoded MD5sum value.
func (r *Reader) MD5Base64String() string {
	return base64.StdEncoding.EncodeToString(r.md5sum)
}

// SHA256HexString returns hex sha256 value.
func (r *Reader) SHA256HexString() string {
	return hex.EncodeToString(r.sha256sum)
}

// Verify verifies if the computed MD5 sum and SHA256 sum are
// equal to the ones specified when creating the Reader.
func (r *Reader) Verify() error {
	if r.sha256Hash != nil && len(r.sha256sum) > 0 {
		if sum := r.sha256Hash.Sum(nil); !bytes.Equal(r.sha256sum, sum) {
			return SHA256Mismatch{hex.EncodeToString(r.sha256sum), hex.EncodeToString(sum)}
		}
	}
	if len(r.md5sum) > 0 {
		if sum := r.md5Hash.Sum(nil); !bytes.Equal(r.md5sum, sum) {
			return BadDigest{hex.EncodeToString(r.md5sum), hex.EncodeToString(sum)}
		}
	}
	return nil
}
