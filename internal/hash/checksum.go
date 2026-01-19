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
	"hash/crc64"
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

// MinIOMultipartChecksumType is as metadata on multipart uploads to indicate checksum type.
const MinIOMultipartChecksumType = "x-minio-multipart-checksum-type"

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
	// ChecksumCRC64NVME indicates CRC64 with 0xad93d23594c93659 polynomial.
	ChecksumCRC64NVME
	// ChecksumFullObject indicates the checksum is of the full object,
	// not checksum of checksums. Should only be set on ChecksumMultipart
	ChecksumFullObject

	// ChecksumNone indicates no checksum.
	ChecksumNone ChecksumType = 0

	baseTypeMask = ChecksumSHA256 | ChecksumSHA1 | ChecksumCRC32 | ChecksumCRC32C | ChecksumCRC64NVME
)

// BaseChecksumTypes is a list of all the base checksum types.
var BaseChecksumTypes = []ChecksumType{ChecksumSHA256, ChecksumSHA1, ChecksumCRC32, ChecksumCRC64NVME, ChecksumCRC32C}

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

// Base returns the base checksum (if any)
func (c ChecksumType) Base() ChecksumType {
	return c & baseTypeMask
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
	case c.Is(ChecksumCRC64NVME):
		return xhttp.AmzChecksumCRC64NVME
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
	case c.Is(ChecksumCRC64NVME):
		return crc64.Size
	}
	return 0
}

// IsSet returns whether the type is valid and known.
func (c ChecksumType) IsSet() bool {
	return !c.Is(ChecksumInvalid) && !c.Base().Is(ChecksumNone)
}

// ChecksumStringToType is like NewChecksumType but without the `mode`
func ChecksumStringToType(alg string) ChecksumType {
	switch strings.ToUpper(alg) {
	case "CRC32":
		return ChecksumCRC32
	case "CRC32C":
		return ChecksumCRC32C
	case "SHA1":
		return ChecksumSHA1
	case "SHA256":
		return ChecksumSHA256
	case "CRC64NVME":
		// AWS seems to ignore full value, and just assume it.
		return ChecksumCRC64NVME
	case "":
		return ChecksumNone
	}
	return ChecksumInvalid
}

// NewChecksumType returns a checksum type based on the algorithm string and obj type.
func NewChecksumType(alg, objType string) ChecksumType {
	full := ChecksumFullObject
	switch objType {
	case xhttp.AmzChecksumTypeFullObject:
	case xhttp.AmzChecksumTypeComposite, "":
		full = 0
	default:
		return ChecksumInvalid
	}

	switch strings.ToUpper(alg) {
	case "CRC32":
		return ChecksumCRC32 | full
	case "CRC32C":
		return ChecksumCRC32C | full
	case "SHA1":
		if full != 0 {
			return ChecksumInvalid
		}
		return ChecksumSHA1
	case "SHA256":
		if full != 0 {
			return ChecksumInvalid
		}
		return ChecksumSHA256
	case "CRC64NVME":
		// AWS seems to ignore full value, and just assume it.
		return ChecksumCRC64NVME
	case "":
		if full != 0 {
			return ChecksumInvalid
		}
		return ChecksumNone
	}
	return ChecksumInvalid
}

// NewChecksumHeader returns a checksum type based on the algorithm string.
func NewChecksumHeader(h http.Header) ChecksumType {
	return NewChecksumType(h.Get(xhttp.AmzChecksumAlgo), h.Get(xhttp.AmzChecksumType))
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
	case c.Is(ChecksumCRC64NVME):
		return "CRC64NVME"
	case c.Is(ChecksumNone):
		return ""
	}
	return "invalid"
}

// StringFull returns the type and all flags as a string.
func (c ChecksumType) StringFull() string {
	out := []string{c.String()}
	if c.Is(ChecksumMultipart) {
		out = append(out, "MULTIPART")
	}
	if c.Is(ChecksumIncludesMultipart) {
		out = append(out, "INCLUDESMP")
	}
	if c.Is(ChecksumTrailing) {
		out = append(out, "TRAILING")
	}
	if c.Is(ChecksumFullObject) {
		out = append(out, "FULLOBJ")
	}
	return strings.Join(out, "|")
}

// FullObjectRequested will return if the checksum type indicates full object checksum was requested.
func (c ChecksumType) FullObjectRequested() bool {
	return c&(ChecksumFullObject) == ChecksumFullObject || c.Is(ChecksumCRC64NVME)
}

// IsMultipartComposite returns true if the checksum is multipart and full object was not requested.
func (c ChecksumType) IsMultipartComposite() bool {
	return c.Is(ChecksumMultipart) && !c.FullObjectRequested()
}

// ObjType returns a string to return as x-amz-checksum-type.
func (c ChecksumType) ObjType() string {
	if c.FullObjectRequested() {
		return xhttp.AmzChecksumTypeFullObject
	}
	if c.IsMultipartComposite() {
		return xhttp.AmzChecksumTypeComposite
	}
	if !c.Is(ChecksumMultipart) {
		return xhttp.AmzChecksumTypeFullObject
	}
	if c.IsSet() {
		return xhttp.AmzChecksumTypeComposite
	}
	return ""
}

// CanMerge will return if the checksum type indicates that checksums can be merged.
func (c ChecksumType) CanMerge() bool {
	return c.Is(ChecksumCRC64NVME) || c.Is(ChecksumCRC32C) || c.Is(ChecksumCRC32)
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
	case c.Is(ChecksumCRC64NVME):
		return crc64.New(crc64Table)
	}
	return nil
}

// Trailing return whether the checksum is trailing.
func (c ChecksumType) Trailing() bool {
	return c.Is(ChecksumTrailing)
}

// NewChecksumFromData returns a new Checksum, using specified algorithm type on data.
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
// Returns whether this is (part of) a multipart checksum.
func ReadCheckSums(b []byte, part int) (cs map[string]string, isMP bool) {
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
			isMP = true
			t, n := binary.Uvarint(b)
			if n < 0 {
				break
			}
			if !typ.FullObjectRequested() {
				cs = fmt.Sprintf("%s-%d", cs, t)
			}
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
			if ckType := typ.ObjType(); ckType != "" {
				res[xhttp.AmzChecksumType] = ckType
			}
		}
	}
	if len(res) == 0 {
		res = nil
	}
	return res, isMP
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
	return NewChecksumWithType(NewChecksumType(alg, ""), value)
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

// ChecksumFromBytes reconstructs a Checksum struct from the serialized bytes created in AppendTo()
// Returns nil if the bytes are invalid or empty.
// AppendTo() can append a serialized Checksum to another already-serialized Checksum,
// however, in practice, we only use one at a time.
// ChecksumFromBytes only returns the first one and no part checksums.
func ChecksumFromBytes(b []byte) *Checksum {
	if len(b) == 0 {
		return nil
	}

	// Read checksum type
	t, n := binary.Uvarint(b)
	if n <= 0 {
		return nil
	}
	b = b[n:]

	typ := ChecksumType(t)
	length := typ.RawByteLen()
	if length == 0 || len(b) < length {
		return nil
	}

	// Read raw checksum bytes
	raw := make([]byte, length)
	copy(raw, b[:length])
	b = b[length:]

	c := &Checksum{
		Type:    typ,
		Raw:     raw,
		Encoded: base64.StdEncoding.EncodeToString(raw),
	}

	// Handle multipart checksums
	if typ.Is(ChecksumMultipart) {
		parts, n := binary.Uvarint(b)
		if n <= 0 {
			return nil
		}
		b = b[n:]

		c.WantParts = int(parts)

		if typ.Is(ChecksumIncludesMultipart) {
			wantLen := int(parts) * length
			if len(b) < wantLen {
				return nil
			}
		}
	}

	if !c.Valid() {
		return nil
	}

	return c
}

// Valid returns whether checksum is valid.
func (c Checksum) Valid() bool {
	if c.Type == ChecksumInvalid {
		return false
	}
	if len(c.Encoded) == 0 || c.Type.Trailing() {
		return c.Type.Is(ChecksumNone) || c.Type.Trailing()
	}
	return c.Type.RawByteLen() == len(c.Raw)
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

// AsMap returns the checksum as a map[string]string.
func (c *Checksum) AsMap() map[string]string {
	if c == nil || !c.Valid() {
		return nil
	}
	return map[string]string{
		c.Type.String():       c.Encoded,
		xhttp.AmzChecksumType: c.Type.ObjType(),
	}
}

// Equal returns whether two checksum structs are equal in all their fields.
func (c *Checksum) Equal(s *Checksum) bool {
	if c == nil || s == nil {
		return c == s
	}
	return c.Type == s.Type &&
		c.Encoded == s.Encoded &&
		bytes.Equal(c.Raw, s.Raw) &&
		c.WantParts == s.WantParts
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
		if k == xhttp.AmzChecksumType {
			w.Header().Set(xhttp.AmzChecksumType, v)
			continue
		}
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
			for _, t := range BaseChecksumTypes {
				if strings.EqualFold(t.Key(), header) {
					duplicates = res != nil
					res = NewChecksumWithType(t|ChecksumTrailing, "")
				}
			}
			if duplicates {
				return nil, ErrInvalidChecksum
			}
		}
		if res != nil {
			switch h.Get(xhttp.AmzChecksumType) {
			case xhttp.AmzChecksumTypeFullObject:
				if !res.Type.CanMerge() {
					return nil, ErrInvalidChecksum
				}
				res.Type |= ChecksumFullObject
			case xhttp.AmzChecksumTypeComposite, "":
			default:
				return nil, ErrInvalidChecksum
			}
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
		t |= NewChecksumHeader(h)
		if h.Get(xhttp.AmzChecksumType) == xhttp.AmzChecksumTypeFullObject {
			if !t.CanMerge() {
				return ChecksumInvalid, ""
			}
			t |= ChecksumFullObject
		}
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
			if h.Get(xhttp.AmzChecksumType) == xhttp.AmzChecksumTypeFullObject {
				if !t.CanMerge() {
					t = ChecksumInvalid
					s = ""
					return
				}
				t |= ChecksumFullObject
			}
			return
		}
	}
	for _, t := range BaseChecksumTypes {
		checkType(t)
	}
	return t, s
}
