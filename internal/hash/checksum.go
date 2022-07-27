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

// MinIOMultipartChecksum is as metadata on multiparts to indicate checksum.
const MinIOMultipartChecksum = "x-minio-multipart-checksum"

// ChecksumType contains information about the checksum type.
type ChecksumType uint32

const (
	ChecksumTrailing ChecksumType = 1 << iota
	ChecksumSHA256
	ChecksumSHA1
	ChecksumCRC32
	ChecksumCRC32C
	ChecksumInvalid

	ChecksumNone ChecksumType = 0
)

// Checksum is a type and base 64 encoded value.
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
	case c.Is(ChecksumCRC32):
		return xhttp.AmzChecksumCRC32
	case c.Is(ChecksumCRC32C):
		return xhttp.AmzChecksumCRC32C
	case c.Is(ChecksumSHA1):
		return xhttp.AmzChecksumSHA1
	case c.Is(ChecksumSHA256):
		return xhttp.AmzChecksumSHA256
	}
	return ""
}

// Set returns whether the type is valid and known.
func (c ChecksumType) Set() bool {
	return !c.Is(ChecksumInvalid) && !c.Is(ChecksumNone)
}

// NewChecksumType returns a checksum type based on the algorithm string.
func NewChecksumType(alg string) ChecksumType {
	switch strings.ToUpper(alg) {
	case "CRC32":
		return ChecksumCRC32
	case "CRC32C":
		return ChecksumCRC32C
	case "SHA1":
		return ChecksumSHA1
	case "SHA256":
		return ChecksumSHA256
	case "":
		return ChecksumNone
	}
	return ChecksumInvalid
}

// String returns the type as a string.
func (c ChecksumType) String() string {
	switch {
	case c.Is(ChecksumCRC32):
		return "CRC32"
	case c.Is(ChecksumCRC32C):
		return "CRC32C"
	case c.Is(ChecksumSHA1):
		return "SHA1"
	case c.Is(ChecksumSHA256):
		return "SHA256"
	case c.Is(ChecksumNone):
		return ""
	}
	return "invalid"
}

// NewChecksumString returns a new checksum from specified algorithm and base64 encoded value.
func NewChecksumString(alg, value string) *Checksum {
	t := NewChecksumType(alg)
	if !t.Set() {
		return nil
	}
	c := Checksum{Type: t, Encoded: value}
	if !c.Valid() {
		return nil
	}
	return &c
}

func (c Checksum) Trailing() bool {
	return c.Type.Is(ChecksumTrailing)
}

// Valid returns whether checksum is valid.
func (c Checksum) Valid() bool {
	if c.Type == ChecksumInvalid {
		return false
	}
	if len(c.Encoded) == 0 || c.Type.Is(ChecksumTrailing) {
		return c.Type.Is(ChecksumNone) || c.Type.Is(ChecksumTrailing)
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

// TransferChecksumHeader will transfer any checksum value that has been checked.
func TransferChecksumHeader(w http.ResponseWriter, r *http.Request) {
	t, s := getContentChecksum(r)
	if t.Is(ChecksumTrailing) || t.Is(ChecksumNone) || t.Is(ChecksumInvalid) {
		// TODO: Add trailing when we can read it.
		return
	}
	w.Header().Set(t.Key(), s)
}

// Hasher returns a hasher corresponding to the checksum type.
// Returns nil if no checksum.
func (c ChecksumType) Hasher() hash.Hash {
	switch {
	case c.Is(ChecksumCRC32):
		return crc32.NewIEEE()
	case c.Is(ChecksumCRC32C):
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case c.Is(ChecksumSHA1):
		return sha1.New()
	case c.Is(ChecksumSHA256):
		return sha256.New()
	}
	return nil
}

// GetContentChecksum returns content checksum.
// Returns ErrInvalidChecksum if so.
// Returns nil, nil if no checksum.
func GetContentChecksum(r *http.Request) (*Checksum, error) {
	t, s := getContentChecksum(r)
	if t == ChecksumNone {
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
// Returns ChecksumInvalid if so.
func getContentChecksum(r *http.Request) (t ChecksumType, s string) {
	t = ChecksumNone
	alg := r.Header.Get(xhttp.AmzChecksumAlgo)
	if alg != "" {
		t |= NewChecksumType(alg)
		if t.Set() {
			hdr := t.Key()
			if s = r.Header.Get(hdr); s == "" {
				if strings.EqualFold(r.Header.Get(xhttp.AmzTrailer), hdr) {
					t |= ChecksumTrailing
				} else {
					t = ChecksumInvalid
				}
			}
		}
		return t, s
	}

	checkType := func(c ChecksumType) {
		if s = r.Header.Get(c.Key()); s != "" {
			// If already set, invalid
			if t != ChecksumNone {
				t = ChecksumInvalid
				s = ""
			} else {
				t = c
			}
		}
	}
	checkType(ChecksumCRC32)
	checkType(ChecksumCRC32C)
	checkType(ChecksumSHA1)
	checkType(ChecksumSHA256)
	return t, s
}
