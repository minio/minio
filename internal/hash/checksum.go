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
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"net/http"
	"strings"

	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
)

// MinIOMultipartChecksum is as metadata on multipart uploads to indicate checksum type.
const MinIOMultipartChecksum = "x-minio-multipart-checksum"

// ChecksumType contains information about the checksum type.
type ChecksumType uint32

const (

	// ChecksumTrailing indicates the checksum will be sent in the trailing header.
	// Another checksum type will be set.
	ChecksumTrailing ChecksumType = 1 << iota

	// ChecksumSHA256 indicates a SHA256 checksum.
	ChecksumSHA256
	// ChecksumSHA1 indicates a SHA-1 checksum.
	ChecksumSHA1
	// ChecksumCRC32 indicates a CRC32 checksum with IEEE table.
	ChecksumCRC32
	// ChecksumCRC32C indicates a CRC32 checksum with Castagnoli table.
	ChecksumCRC32C
	// ChecksumInvalid indicates an invalid checksum.
	ChecksumInvalid

	// ChecksumNone indicates no checksum.
	ChecksumNone ChecksumType = 0
)

// Checksum is a type and base 64 encoded value.
type Checksum struct {
	Type    ChecksumType
	Encoded string
	Raw     []byte
}

// Is returns if c is all of t.
func (c ChecksumType) Is(t ChecksumType) bool {
	if t == ChecksumNone {
		return c == ChecksumNone
	}
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

// RawByteLen returns the size of the un-encoded checksum.
func (c ChecksumType) RawByteLen() int {
	switch {
	case c.Is(ChecksumCRC32):
		return 4
	case c.Is(ChecksumCRC32C):
		return 4
	case c.Is(ChecksumSHA1):
		return sha1.Size
	case c.Is(ChecksumSHA256):
		return sha256.Size
	}
	return 0
}

// IsSet returns whether the type is valid and known.
func (c ChecksumType) IsSet() bool {
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

// Trailing return whether the checksum is traling.
func (c ChecksumType) Trailing() bool {
	return c.Is(ChecksumTrailing)
}

// NewChecksumFromData returns a new checksum from specified algorithm and base64 encoded value.
func NewChecksumFromData(t ChecksumType, data []byte) *Checksum {
	if !t.IsSet() {
		return nil
	}
	h := t.Hasher()
	h.Write(data)
	raw := h.Sum(nil)
	c := Checksum{Type: t, Encoded: base64.StdEncoding.EncodeToString(raw), Raw: raw}
	if !c.Valid() {
		return nil
	}
	return &c
}

// ReadCheckSums will read checksums from b and return them.
func ReadCheckSums(b []byte) map[string]string {
	res := make(map[string]string, 1)
	for len(b) > 0 {
		t, n := binary.Uvarint(b)
		if n < 0 {
			break
		}
		b = b[n:]

		typ := ChecksumType(t)
		length := typ.RawByteLen()
		if length == 0 || len(b) < length {
			break
		}
		res[typ.String()] = base64.StdEncoding.EncodeToString(b[:length])
		b = b[length:]
	}
	if len(res) == 0 {
		res = nil
	}
	return res
}

// NewChecksumWithType is similar to NewChecksumString but expects input algo of ChecksumType.
func NewChecksumWithType(alg ChecksumType, value string) *Checksum {
	if !alg.IsSet() {
		return nil
	}
	bvalue, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil
	}
	c := Checksum{Type: alg, Encoded: value, Raw: bvalue}
	if !c.Valid() {
		return nil
	}
	return &c
}

// NewChecksumString returns a new checksum from specified algorithm and base64 encoded value.
func NewChecksumString(alg, value string) *Checksum {
	return NewChecksumWithType(NewChecksumType(alg), value)
}

// AppendTo will append the checksum to b.
// ReadCheckSums reads the values back.
func (c *Checksum) AppendTo(b []byte) []byte {
	if c == nil {
		return nil
	}
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(tmp[:], uint64(c.Type))
	crc := c.Raw
	if len(crc) != c.Type.RawByteLen() {
		return b
	}
	b = append(b, tmp[:n]...)
	b = append(b, crc...)
	return b
}

// Valid returns whether checksum is valid.
func (c Checksum) Valid() bool {
	if c.Type == ChecksumInvalid {
		return false
	}
	if len(c.Encoded) == 0 || c.Type.Is(ChecksumTrailing) {
		return c.Type.Is(ChecksumNone) || c.Type.Is(ChecksumTrailing)
	}
	raw := c.Raw
	return c.Type.RawByteLen() == len(raw)
}

// Matches returns whether given content matches c.
func (c Checksum) Matches(content []byte) error {
	if len(c.Encoded) == 0 {
		return nil
	}
	hasher := c.Type.Hasher()
	_, err := hasher.Write(content)
	if err != nil {
		return err
	}
	sum := hasher.Sum(nil)
	if !bytes.Equal(sum, c.Raw) {
		return ChecksumMismatch{
			Want: c.Encoded,
			Got:  base64.StdEncoding.EncodeToString(sum),
		}
	}
	return nil
}

// AsMap returns the
func (c *Checksum) AsMap() map[string]string {
	if c == nil || !c.Valid() {
		return nil
	}
	return map[string]string{c.Type.String(): c.Encoded}
}

// TransferChecksumHeader will transfer any checksum value that has been checked.
func TransferChecksumHeader(w http.ResponseWriter, r *http.Request) {
	t, s := getContentChecksum(r)
	if !t.IsSet() || t.Is(ChecksumTrailing) {
		// TODO: Add trailing when we can read it.
		return
	}
	w.Header().Set(t.Key(), s)
}

// AddChecksumHeader will transfer any checksum value that has been checked.
func AddChecksumHeader(w http.ResponseWriter, c map[string]string) {
	for k, v := range c {
		fmt.Println(c, v)
		cksum := NewChecksumString(k, v)
		if cksum == nil {
			continue
		}
		if cksum.Valid() {
			w.Header().Set(cksum.Type.Key(), v)
		}
	}
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
	cksum := NewChecksumWithType(t, s)
	if cksum == nil {
		return nil, ErrInvalidChecksum
	}
	return cksum, nil
}

// getContentChecksum returns content checksum type and value.
// Returns ChecksumInvalid if so.
func getContentChecksum(r *http.Request) (t ChecksumType, s string) {
	t = ChecksumNone
	alg := r.Header.Get(xhttp.AmzChecksumAlgo)
	if alg != "" {
		t |= NewChecksumType(alg)
		if t.IsSet() {
			hdr := t.Key()
			if s = r.Header.Get(hdr); s == "" {
				if strings.EqualFold(r.Header.Get(xhttp.AmzTrailer), hdr) {
					t |= ChecksumTrailing
				} else {
					t = ChecksumInvalid
				}
				return ChecksumNone, ""
			}
		}
		return t, s
	}
	checkType := func(c ChecksumType) {
		if got := r.Header.Get(c.Key()); got != "" {
			// If already set, invalid
			if t != ChecksumNone {
				t = ChecksumInvalid
				s = ""
			} else {
				t = c
				s = got
			}
		}
	}
	checkType(ChecksumCRC32)
	checkType(ChecksumCRC32C)
	checkType(ChecksumSHA1)
	checkType(ChecksumSHA256)
	return t, s
}
