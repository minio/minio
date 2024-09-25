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
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

func hashLogIf(ctx context.Context, err error) {
	logger.LogIf(ctx, "hash", err)
}

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
	// ChecksumMultipart indicates the checksum is from a multipart upload.
	ChecksumMultipart
	// ChecksumIncludesMultipart indicates the checksum also contains part checksums.
	ChecksumIncludesMultipart

	// ChecksumNone indicates no checksum.
	ChecksumNone ChecksumType = 0
)

// Checksum is a type and base 64 encoded value.
type Checksum struct {
	Type      ChecksumType
	Encoded   string
	Raw       []byte
	WantParts int
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

// Trailing return whether the checksum is trailing.
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
func ReadCheckSums(b []byte, part int) map[string]string {
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
		cs := base64.StdEncoding.EncodeToString(b[:length])
		b = b[length:]
		if typ.Is(ChecksumMultipart) {
			t, n := binary.Uvarint(b)
			if n < 0 {
				break
			}
			cs = fmt.Sprintf("%s-%d", cs, t)
			b = b[n:]
			if part > 0 {
				cs = ""
			}
			if typ.Is(ChecksumIncludesMultipart) {
				wantLen := int(t) * length
				if len(b) < wantLen {
					break
				}
				// Read part checksum
				if part > 0 && uint64(part) <= t {
					offset := (part - 1) * length
					partCs := b[offset:]
					cs = base64.StdEncoding.EncodeToString(partCs[:length])
				}
				b = b[wantLen:]
			}
		} else if part > 1 {
			// For non-multipart, checksum is part 1.
			cs = ""
		}
		if cs != "" {
			res[typ.String()] = cs
		}
	}
	if len(res) == 0 {
		res = nil
	}
	return res
}

// ReadPartCheckSums will read all part checksums from b and return them.
func ReadPartCheckSums(b []byte) (res []map[string]string) {
	for len(b) > 0 {
		t, n := binary.Uvarint(b)
		if n <= 0 {
			break
		}
		b = b[n:]

		typ := ChecksumType(t)
		length := typ.RawByteLen()
		if length == 0 || len(b) < length {
			break
		}
		// Skip main checksum
		b = b[length:]
		parts, n := binary.Uvarint(b)
		if n <= 0 {
			break
		}
		if !typ.Is(ChecksumIncludesMultipart) {
			continue
		}

		if len(res) == 0 {
			res = make([]map[string]string, parts)
		}
		b = b[n:]
		for part := 0; part < int(parts); part++ {
			if len(b) < length {
				break
			}
			// Read part checksum
			cs := base64.StdEncoding.EncodeToString(b[:length])
			b = b[length:]
			if res[part] == nil {
				res[part] = make(map[string]string, 1)
			}
			res[part][typ.String()] = cs
		}
	}
	return res
}

// NewChecksumWithType is similar to NewChecksumString but expects input algo of ChecksumType.
func NewChecksumWithType(alg ChecksumType, value string) *Checksum {
	if !alg.IsSet() {
		return nil
	}
	wantParts := 0
	if strings.ContainsRune(value, '-') {
		valSplit := strings.Split(value, "-")
		if len(valSplit) != 2 {
			return nil
		}
		value = valSplit[0]
		nParts, err := strconv.Atoi(valSplit[1])
		if err != nil {
			return nil
		}
		alg |= ChecksumMultipart
		wantParts = nParts
	}
	bvalue, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil
	}
	c := Checksum{Type: alg, Encoded: value, Raw: bvalue, WantParts: wantParts}
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
// 'parts' is used when checksum has ChecksumMultipart set.
// ReadCheckSums reads the values back.
func (c *Checksum) AppendTo(b []byte, parts []byte) []byte {
	if c == nil {
		return nil
	}
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(tmp[:], uint64(c.Type))
	crc := c.Raw
	if c.Type.Trailing() {
		// When we serialize we don't care if it was trailing.
		c.Type ^= ChecksumTrailing
	}
	if len(crc) != c.Type.RawByteLen() {
		return b
	}
	b = append(b, tmp[:n]...)
	b = append(b, crc...)
	if c.Type.Is(ChecksumMultipart) {
		var checksums int
		if c.WantParts > 0 && !c.Type.Is(ChecksumIncludesMultipart) {
			checksums = c.WantParts
		}
		// Ensure we don't divide by 0:
		if c.Type.RawByteLen() == 0 || len(parts)%c.Type.RawByteLen() != 0 {
			hashLogIf(context.Background(), fmt.Errorf("internal error: Unexpected checksum length: %d, each checksum %d", len(parts), c.Type.RawByteLen()))
			checksums = 0
			parts = nil
		} else if len(parts) > 0 {
			checksums = len(parts) / c.Type.RawByteLen()
		}
		if !c.Type.Is(ChecksumIncludesMultipart) {
			parts = nil
		}
		n := binary.PutUvarint(tmp[:], uint64(checksums))
		b = append(b, tmp[:n]...)
		if len(parts) > 0 {
			b = append(b, parts...)
		}
	}
	return b
}

// Valid returns whether checksum is valid.
func (c Checksum) Valid() bool {
	if c.Type == ChecksumInvalid {
		return false
	}
	if len(c.Encoded) == 0 || c.Type.Trailing() {
		return c.Type.Is(ChecksumNone) || c.Type.Trailing()
	}
	raw := c.Raw
	return c.Type.RawByteLen() == len(raw)
}

// Matches returns whether given content matches c.
func (c Checksum) Matches(content []byte, parts int) error {
	if len(c.Encoded) == 0 {
		return nil
	}
	hasher := c.Type.Hasher()
	_, err := hasher.Write(content)
	if err != nil {
		return err
	}
	sum := hasher.Sum(nil)
	if c.WantParts > 0 && c.WantParts != parts {
		return ChecksumMismatch{
			Want: fmt.Sprintf("%s-%d", c.Encoded, c.WantParts),
			Got:  fmt.Sprintf("%s-%d", base64.StdEncoding.EncodeToString(sum), parts),
		}
	}

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
// If checksum was trailing, they must have been added to r.Trailer.
func TransferChecksumHeader(w http.ResponseWriter, r *http.Request) {
	c, err := GetContentChecksum(r.Header)
	if err != nil || c == nil {
		return
	}
	t, s := c.Type, c.Encoded
	if !c.Type.IsSet() {
		return
	}
	if c.Type.Is(ChecksumTrailing) {
		val := r.Trailer.Get(t.Key())
		if val != "" {
			w.Header().Set(t.Key(), val)
		}
		return
	}
	w.Header().Set(t.Key(), s)
}

// AddChecksumHeader will transfer any checksum value that has been checked.
func AddChecksumHeader(w http.ResponseWriter, c map[string]string) {
	for k, v := range c {
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
func GetContentChecksum(h http.Header) (*Checksum, error) {
	if trailing := h.Values(xhttp.AmzTrailer); len(trailing) > 0 {
		var res *Checksum
		for _, header := range trailing {
			var duplicates bool
			switch {
			case strings.EqualFold(header, ChecksumCRC32C.Key()):
				duplicates = res != nil
				res = NewChecksumWithType(ChecksumCRC32C|ChecksumTrailing, "")
			case strings.EqualFold(header, ChecksumCRC32.Key()):
				duplicates = res != nil
				res = NewChecksumWithType(ChecksumCRC32|ChecksumTrailing, "")
			case strings.EqualFold(header, ChecksumSHA256.Key()):
				duplicates = res != nil
				res = NewChecksumWithType(ChecksumSHA256|ChecksumTrailing, "")
			case strings.EqualFold(header, ChecksumSHA1.Key()):
				duplicates = res != nil
				res = NewChecksumWithType(ChecksumSHA1|ChecksumTrailing, "")
			}
			if duplicates {
				return nil, ErrInvalidChecksum
			}
		}
		if res != nil {
			return res, nil
		}
	}
	t, s := getContentChecksum(h)
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
func getContentChecksum(h http.Header) (t ChecksumType, s string) {
	t = ChecksumNone
	alg := h.Get(xhttp.AmzChecksumAlgo)
	if alg != "" {
		t |= NewChecksumType(alg)
		if t.IsSet() {
			hdr := t.Key()
			if s = h.Get(hdr); s == "" {
				return ChecksumNone, ""
			}
		}
		return t, s
	}
	checkType := func(c ChecksumType) {
		if got := h.Get(c.Key()); got != "" {
			// If already set, invalid
			if t != ChecksumNone {
				t = ChecksumInvalid
				s = ""
			} else {
				t = c
				s = got
			}
			return
		}
	}
	checkType(ChecksumCRC32)
	checkType(ChecksumCRC32C)
	checkType(ChecksumSHA1)
	checkType(ChecksumSHA256)
	return t, s
}
