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
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"

	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

var errNestedReader = errors.New("Nesting of Reader detected, not allowed")

// Hex returns the hexadecimal encoding of b.
func Hex(b []byte) string { return hex.EncodeToString(b) }

// Base64 returns the base64 encoding of b.
func Base64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// HexAsBase64 returns the base64 encoding of the hexadecimal encoded string s.
func HexAsBase64(s string) string {
	b, _ := hex.DecodeString(s)
	return Base64(b)
}

type Reader interface {
	io.Reader

	// Size returns the absolute number of bytes the Reader
	// will return during reading and the actual size of the
	// underlying object.
	//
	// The actual size may differ from the available number of
	// bytes of the stream - e.g. if the stream is compressed.
	// It returns -1 for size and actualSize for unlimited data.
	Size() (size, actualSize int64)

	// Checksums returns the MD5 and the SHA256 hash values as hex
	// string provided during the Reader initialization. They may
	// not be equal to the ETag or SHA256 computed during reading.
	Checksums() (md5, sha256 string)

	// ETag returns the etag hash value of currently read data.
	// Calling this function multiple times may return different
	// results if intermixed with Read calls.
	ETag() []byte

	// SHA256 returns the SHA256 hash value of currently read data.
	// Calling this function multiple times may return different
	// results if intermixed with Read calls.
	SHA256() []byte
}

// NewReader returns a new hash Reader which computes the MD5 sum and
// SHA256 sum (if set) of the provided io.Reader at EOF.
func NewReader(src io.Reader, size int64, md5Hex, sha256Hex string, actualSize int64) (Reader, error) {
	if _, ok := src.(Reader); ok {
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
	if size >= 0 {
		src = io.LimitReader(src, size)
	}
	return &hashReader{
		reader{
			md5sum:     md5sum,
			sha256sum:  sha256sum,
			src:        src,
			size:       size,
			md5Hash:    md5.New(),
			sha256Hash: sha256Hash,
			actualSize: actualSize,
		},
	}, nil
}

func NewEncryptedReader(ciphertext io.Reader, plaintext Reader, size int64, md5Hex, sha256Hex string, actualSize int64, sealKey []byte) (Reader, error) {
	if _, ok := ciphertext.(Reader); ok {
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
	if size >= 0 {
		ciphertext = io.LimitReader(ciphertext, size)
	}
	if len(sealKey) != 32 {
		return nil, errors.New("hash: key for sealing the etag must be 32 bytes long")
	}

	return &cryptoReader{
		reader: reader{
			md5sum:     md5sum,
			sha256sum:  sha256sum,
			src:        ciphertext,
			size:       size,
			md5Hash:    md5.New(),
			sha256Hash: sha256Hash,
			actualSize: actualSize,
		},
		plaintext: plaintext,
		key:       sealKey,
	}, nil
}

type hashReader struct{ reader }

func (r *hashReader) ETag() []byte { return r.md5Hash.Sum(nil) }

type cryptoReader struct {
	reader
	plaintext Reader
	key       []byte
}

func (r *cryptoReader) ETag() []byte {
	var buffer bytes.Buffer
	if _, err := sio.Encrypt(&buffer, bytes.NewReader(r.plaintext.ETag()), sio.Config{Key: r.key}); err != nil {
		logger.CriticalIf(context.Background(), errors.New("Failed to seal the ETag")) // Fail if encryption fails - don't return a corrupt etag
	}
	return buffer.Bytes()
}

type reader struct {
	src        io.Reader
	size       int64
	actualSize int64

	md5sum, sha256sum   []byte // Byte values of md5sum, sha256sum of client sent values.
	md5Hash, sha256Hash hash.Hash
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	if n > 0 {
		r.md5Hash.Write(p[:n])
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

func (r *reader) Size() (int64, int64) { return r.size, r.actualSize }

func (r *reader) Checksums() (string, string) { return Hex(r.md5sum), Hex(r.sha256sum) }

func (r *reader) SHA256() []byte {
	if r.sha256Hash != nil {
		return r.sha256Hash.Sum(nil)
	}
	return nil
}

func (r *reader) verify() error {
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
