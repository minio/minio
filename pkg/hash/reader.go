/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"io"

	md5simd "github.com/minio/md5-simd"
	sha256 "github.com/minio/sha256-simd"
)

// Reader writes what it reads from an io.Reader to an MD5 and SHA256 hash.Hash.
// Reader verifies that the content of the io.Reader matches the expected checksums.
type Reader struct {
	src        io.Reader
	size       int64
	actualSize int64

	md5sum, sha256sum []byte // Byte values of md5sum, sha256sum of client sent values.
	sha256Hash        hash.Hash
	// If md5Buffer is non-nil write to this instead of md5Hash
	md5Buffer *bufio.Writer
	md5Hash   md5simd.Hasher

	// md5Got is the sum at the end of the stream
	md5Got []byte
}

// md5Server is a singleton md5 processor.
var md5Server = md5simd.NewServer()

func newMd5(size int64) (md5simd.Hasher, *bufio.Writer) {
	if true && (size < 0 || size > 32<<10) {
		h := md5Server.NewHash()
		return h, bufio.NewWriterSize(h, 32<<10)
		//return h, nil
	}
	return &md5stdWrapper{Hash: md5.New()}, nil
}

type md5stdWrapper struct {
	hash.Hash
}

func (*md5stdWrapper) Close() {}

// NewReader returns a new hash Reader which computes the MD5 sum and
// SHA256 sum (if set) of the provided io.Reader at EOF.
func NewReader(src io.Reader, size int64, md5Hex, sha256Hex string, actualSize int64, strictCompat bool) (*Reader, error) {
	if r, ok := src.(*Reader); ok {
		return r.merge(size, md5Hex, sha256Hex, actualSize, strictCompat)
	}

	sha256sum, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return nil, SHA256Mismatch{}
	}

	md5sum, err := hex.DecodeString(md5Hex)
	if err != nil {
		return nil, BadDigest{}
	}
	if size >= 0 {
		src = io.LimitReader(src, size)
	}
	r := Reader{
		md5sum:     md5sum,
		sha256sum:  sha256sum,
		src:        src,
		size:       size,
		actualSize: actualSize,
	}

	if len(sha256sum) != 0 {
		r.sha256Hash = sha256.New()
	}

	if strictCompat || len(md5sum) != 0 {
		// Strict compatibility is set then we should
		// calculate md5sum always.
		r.md5Hash, r.md5Buffer = newMd5(size)
	}

	return &r, nil
}

// merge another hash into this one.
// There cannot be conflicting information given.
func (r *Reader) merge(size int64, md5Hex, sha256Hex string, actualSize int64, strictCompat bool) (*Reader, error) {
	// Merge sizes.
	if r.size < 0 && size >= 0 {
		r.size = size
		if size > 0 {
			r.src = io.LimitReader(r.src, size)
		}
	}
	if r.actualSize < 0 && actualSize >= 0 {
		r.actualSize = actualSize
	}

	// Merge SHA256.
	// If both are set, they must expect the same.
	sha256sum, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return nil, SHA256Mismatch{}
	}

	if r.sha256Hash != nil && len(sha256sum) > 0 {
		if !bytes.Equal(r.sha256sum, sha256sum) {
			return nil, SHA256Mismatch{hex.EncodeToString(r.sha256sum), sha256Hex}
		}
	} else if len(sha256sum) > 0 {
		r.sha256Hash = sha256.New()
		r.sha256sum = sha256sum
	}

	// Merge MD5 Sum.
	// If both are set, they must expect the same.
	md5sum, err := hex.DecodeString(md5Hex)
	if err != nil {
		return nil, BadDigest{}
	}
	if r.md5Hash != nil && len(md5sum) > 0 {
		if !bytes.Equal(r.md5sum, md5sum) {
			return nil, BadDigest{hex.EncodeToString(r.md5sum), md5Hex}
		}
	} else if len(md5sum) > 0 || (r.md5Hash == nil && strictCompat) {
		r.md5Hash, r.md5Buffer = newMd5(size)
		r.md5sum = md5sum
	}
	return r, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	if n > 0 {
		if r.md5Buffer != nil {
			r.md5Buffer.Write(p[:n])
		} else if r.md5Hash != nil {
			r.md5Hash.Write(p[:n])
		}
		if r.sha256Hash != nil {
			r.sha256Hash.Write(p[:n])
		}
	}

	// At io.EOF verify if the checksums are right.
	if err == io.EOF {
		if cerr := r.verify(); cerr != nil {
			return 0, cerr
		}
	}

	return
}

// Close and release resources.
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	if r.md5Hash != nil {
		r.md5Hash.Close()
		r.md5Hash = nil
	}
	return nil
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (r *Reader) Size() int64 { return r.size }

// ActualSize returns the pre-modified size of the object.
// DecompressedSize - For compressed objects.
func (r *Reader) ActualSize() int64 { return r.actualSize }

// MD5Current - returns byte md5 value of the current state
// of the md5 hash after reading the incoming content.
// NOTE: Calling this function multiple times might yield
// different results if they are intermixed with Reader.
func (r *Reader) MD5Current() []byte {
	if len(r.md5Got) > 0 {
		return r.md5Got
	}
	if r.md5Hash != nil {
		if r.md5Buffer != nil {
			r.md5Buffer.Flush()
		}
		return r.md5Hash.Sum(nil)
	}
	return nil
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

// verify verifies if the computed MD5 sum and SHA256 sum are
// equal to the ones specified when creating the Reader.
func (r *Reader) verify() error {
	if r.sha256Hash != nil && len(r.sha256sum) > 0 {
		if sum := r.sha256Hash.Sum(nil); !bytes.Equal(r.sha256sum, sum) {
			return SHA256Mismatch{hex.EncodeToString(r.sha256sum), hex.EncodeToString(sum)}
		}
	}

	if r.md5Hash != nil || len(r.md5Got) > 0 {
		if len(r.md5Got) == 0 {
			if r.md5Buffer != nil {
				r.md5Buffer.Flush()
			}
			r.md5Got = r.md5Hash.Sum(nil)
			r.md5Hash.Close()
			r.md5Hash = nil
		}
		if len(r.md5sum) > 0 {
			if !bytes.Equal(r.md5sum, r.md5Got) {
				return BadDigest{hex.EncodeToString(r.md5sum), hex.EncodeToString(r.md5Got)}
			}
		}
	}
	return nil
}
