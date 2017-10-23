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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
)

// NewReader returns a new hash.Reader which computes the MD5 sum and
// SHA256 sum (if set) of the provided io.Reader at EOF. The returned Reader
// uses the computed MD5 sum as etag.
func NewReader(src io.Reader, size int64, md5Hex, sha256Hex string) (Reader, error) {
	if _, ok := src.(Reader); ok {
		return Reader{}, errors.New("Nesting of Reader detected, not allowed")
	}

	sha256sum, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return Reader{}, err
	}
	md5sum, err := hex.DecodeString(md5Hex)
	if err != nil {
		return Reader{}, err
	}

	var sha256Hash hash.Hash
	if len(sha256sum) != 0 {
		sha256Hash = sha256.New()
	}
	if size >= 0 {
		src = io.LimitReader(src, size)
	} else {
		size = -1
	}
	return Reader{
		tagger: &reader{
			src:        src,
			md5sum:     md5sum,
			sha256sum:  sha256sum,
			md5Hash:    md5.New(),
			sha256Hash: sha256Hash,
		},
		size:   size,
		MD5:    md5sum,
		SHA256: sha256sum,
	}, nil
}

// NewGatewayReader returns a new hash.Reader implementation. It will not compute any
// etag and can only be used to pass the provided hash values to the backend.
func NewGatewayReader(src io.Reader, size int64, md5Hex, sha256Hex string) (Reader, error) {
	sha256sum, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return Reader{}, err
	}
	md5sum, err := hex.DecodeString(md5Hex)
	if err != nil {
		return Reader{}, err
	}
	if size >= 0 {
		src = io.LimitReader(src, size)
	} else {
		size = -1
	}
	return Reader{
		tagger: gatewayReader{src, md5sum},
		size:   size,
		MD5:    md5sum,
		SHA256: sha256sum,
	}, nil
}

// Reader computes an etag value of the data it reads.
type Reader struct {
	tagger
	size        int64
	MD5, SHA256 []byte
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (r Reader) Size() int64 { return r.size }

type tagger interface {
	io.Reader
	Etag() []byte
}

// gatewayReader implements tagger and returns the client provided
// content-md5 as etag.
type gatewayReader struct {
	io.Reader
	etag []byte
}

// Etag returns client provided content-md5 as etag.
func (r gatewayReader) Etag() []byte { return r.etag }

// reader writes what it reads from an io.Reader to an MD5 and (if specified) SHA256 hash.Hash.
// reader verifies that the content of the io.Reader matches the expected checksums.
//
// reader is the hash.Reader implementation for simple PUT operations.
type reader struct {
	src                 io.Reader
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

// verify verifies if the computed MD5 sum and SHA256 sum are
// equal to the ones specified when creating the Reader.
func (r *reader) verify() error {
	if r.sha256Hash != nil {
		if sum := r.sha256Hash.Sum(nil); !bytes.Equal(r.sha256sum, sum) {
			return SHA256Mismatch{hex.EncodeToString(r.sha256sum), hex.EncodeToString(sum)}
		}
	}
	if sum := r.Etag(); len(r.md5sum) > 0 && !bytes.Equal(r.md5sum, sum) {
		return BadDigest{hex.EncodeToString(r.md5sum), hex.EncodeToString(sum)}
	}
	return nil
}

// Etag returns md5 sum of the processed content as etag.
func (r *reader) Etag() []byte { return r.md5Hash.Sum(nil) }
