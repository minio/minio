// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package hash

import (
	"crypto/sha1"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"net/http"
	"strings"

	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
)

type ChecksumType uint32

const (
	checksumTrailing ChecksumType = 1 << iota
	checksumSHA256
	checksumSHA1
	checksumCRC32
	checksumCRC32C
	checksumInvalid

	checksumNone ChecksumType = 0
)

type Checksum struct {
	Type    ChecksumType
	Encoded string
}

// Is returns if c is all of t.
func (c ChecksumType) Is(t ChecksumType) bool {
	return c&t == t
}

// Key returns the header key.
// returns empty string if invalid or none.
func (c ChecksumType) Key() string {
	switch {
	case c.Is(checksumCRC32):
		return xhttp.AmzChecksumCRC32
	case c.Is(checksumCRC32C):
		return xhttp.AmzChecksumCRC32C
	case c.Is(checksumSHA1):
		return xhttp.AmzChecksumSHA1
	case c.Is(checksumSHA256):
		return xhttp.AmzChecksumSHA256
	}
	return ""
}

func (c ChecksumType) Set() bool {
	return !c.Is(checksumInvalid) && !c.Is(checksumNone)
}

func NewChecksumType(alg string) ChecksumType {
	switch strings.ToUpper(alg) {
	case "CRC32":
		return checksumCRC32
	case "CRC32C":
		return checksumCRC32C
	case "SHA1":
		return checksumSHA1
	case "SHA256":
		return checksumSHA256
	case "":
		return checksumNone
	}
	return checksumInvalid
}

func (c ChecksumType) String() string {
	switch {
	case c.Is(checksumCRC32):
		return "CRC32"
	case c.Is(checksumCRC32C):
		return "CRC32C"
	case c.Is(checksumSHA1):
		return "SHA1"
	case c.Is(checksumSHA256):
		return "SHA256"
	case c.Is(checksumNone):
		return ""
	}
	return "invalid"
}

func (c Checksum) Trailing() bool {
	return c.Type.Is(checksumTrailing)
}

// Valid returns whether checksum is valid.
func (c Checksum) Valid() bool {
	if c.Type == checksumInvalid {
		return false
	}
	if len(c.Encoded) == 0 || c.Type.Is(checksumTrailing) {
		return c.Type.Is(checksumNone) || c.Type.Is(checksumTrailing)
	}
	h := c.Type.Hasher()
	raw := c.Raw()
	return h.Size() == len(raw)
}

// Raw returns the Raw checksum.
func (c Checksum) Raw() []byte {
	if len(c.Encoded) == 0 {
		return nil
	}
	v, _ := base64.StdEncoding.DecodeString(c.Encoded)
	return v
}

func TransferChecksumHeader(w http.ResponseWriter, r *http.Request) {
	t, s := getContentChecksum(r)
	if t.Is(checksumTrailing) || t.Is(checksumNone) || t.Is(checksumInvalid) {
		// TODO: Add trailing when we can read it.
		return
	}
	w.Header().Set(t.Key(), s)
}

// Hasher returns a hasher corresponding to the checksum type.
// Returns nil if no checksum.
func (c ChecksumType) Hasher() hash.Hash {
	switch {
	case c.Is(checksumCRC32):
		return crc32.NewIEEE()
	case c.Is(checksumCRC32C):
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case c.Is(checksumSHA1):
		return sha1.New()
	case c.Is(checksumSHA256):
		return sha256.New()
	}
	return nil
}

// GetContentChecksum returns content checksum.
// Returns ErrInvalidChecksum if so.
// Returns nil, nil if no checksum.
func GetContentChecksum(r *http.Request) (*Checksum, error) {
	t, s := getContentChecksum(r)
	if t == checksumNone {
		if s == "" {
			return nil, nil
		}
		return nil, ErrInvalidChecksum
	}
	c := Checksum{Type: t, Encoded: s}
	if !c.Valid() {
		return nil, ErrInvalidChecksum
	}

	return &c, nil
}

// getContentChecksum returns content checksum type and value.
// Returns checksumInvalid if so.
func getContentChecksum(r *http.Request) (t ChecksumType, s string) {
	t = checksumNone
	alg := r.Header.Get(xhttp.AmzChecksumAlgo)
	if alg != "" {
		t |= NewChecksumType(alg)
		if t.Set() {
			hdr := t.Key()
			if s = r.Header.Get(hdr); s == "" {
				if strings.EqualFold(r.Header.Get(xhttp.AmzTrailer), hdr) {
					t |= checksumTrailing
				} else {
					t = checksumInvalid
				}
			}
		}
		return t, s
	}

	checkType := func(c ChecksumType) {
		if s = r.Header.Get(c.Key()); s != "" {
			// If already set, invalid
			if t != checksumNone {
				t = checksumInvalid
				s = ""
			} else {
				t = c
			}
		}
	}
	checkType(checksumCRC32)
	checkType(checksumCRC32C)
	checkType(checksumSHA1)
	checkType(checksumSHA256)
	return t, s
}
