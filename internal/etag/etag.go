// Copyright (c) 2015-2021 MinIO, Inc.
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

// Package etag provides an implementation of S3 ETags.
//
// Each S3 object has an associated ETag that can be
// used to e.g. quickly compare objects or check whether
// the content of an object has changed.
//
// In general, an S3 ETag is an MD5 checksum of the object
// content. However, there are many exceptions to this rule.
//
// # Single-part Upload
//
// In case of a basic single-part PUT operation - without server
// side encryption or object compression - the ETag of an object
// is its content MD5.
//
// # Multi-part Upload
//
// The ETag of an object does not correspond to its content MD5
// when the object is uploaded in multiple parts via the S3
// multipart API. Instead, S3 first computes a MD5 of each part:
//
//	 e1 := MD5(part-1)
//	 e2 := MD5(part-2)
//	...
//	 eN := MD5(part-N)
//
// Then, the ETag of the object is computed as MD5 of all individual
// part checksums. S3 also encodes the number of parts into the ETag
// by appending a -<number-of-parts> at the end:
//
//	ETag := MD5(e1 || e2 || e3 ... || eN) || -N
//
//	For example: ceb8853ddc5086cc4ab9e149f8f09c88-5
//
// However, this scheme is only used for multipart objects that are
// not encrypted.
//
// # Server-side Encryption
//
// S3 specifies three types of server-side-encryption - SSE-C, SSE-S3
// and SSE-KMS - with different semantics w.r.t. ETags.
// In case of SSE-S3, the ETag of an object is computed the same as
// for single resp. multipart plaintext objects. In particular,
// the ETag of a singlepart SSE-S3 object is its content MD5.
//
// In case of SSE-C and SSE-KMS, the ETag of an object is computed
// differently. For singlepart uploads the ETag is not the content
// MD5 of the object. For multipart uploads the ETag is also not
// the MD5 of the individual part checksums but it still contains
// the number of parts as suffix.
//
// Instead, the ETag is kind of unpredictable for S3 clients when
// an object is encrypted using SSE-C or SSE-KMS. Maybe AWS S3
// computes the ETag as MD5 of the encrypted content but there is
// no way to verify this assumption since the encryption happens
// inside AWS S3.
// Therefore, S3 clients must not make any assumption about ETags
// in case of SSE-C or SSE-KMS except that the ETag is well-formed.
//
// To put all of this into a simple rule:
//
//	SSE-S3 : ETag == MD5
//	SSE-C  : ETag != MD5
//	SSE-KMS: ETag != MD5
//
// # Encrypted ETags
//
// An S3 implementation has to remember the content MD5 of objects
// in case of SSE-S3. However, storing the ETag of an encrypted
// object in plaintext may reveal some information about the object.
// For example, two objects with the same ETag are identical with
// a very high probability.
//
// Therefore, an S3 implementation may encrypt an ETag before storing
// it. In this case, the stored ETag may not be a well-formed S3 ETag.
// For example, it can be larger due to a checksum added by authenticated
// encryption schemes. Such an ETag must be decrypted before sent to an
// S3 client.
//
// # S3 Clients
//
// There are many different S3 client implementations. Most of them
// access the ETag by looking for the HTTP response header key "Etag".
// However, some of them assume that the header key has to be "ETag"
// (case-sensitive) and will fail otherwise.
// Further, some clients require that the ETag value is a double-quoted
// string. Therefore, this package provides dedicated functions for
// adding and extracting the ETag to/from HTTP headers.
package etag

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/sio"
)

// ETag is a single S3 ETag.
//
// An S3 ETag sometimes corresponds to the MD5 of
// the S3 object content. However, when an object
// is encrypted, compressed or uploaded using
// the S3 multipart API then its ETag is not
// necessarily the MD5 of the object content.
//
// For a more detailed description of S3 ETags
// take a look at the package documentation.
type ETag []byte

// String returns the string representation of the ETag.
//
// The returned string is a hex representation of the
// binary ETag with an optional '-<part-number>' suffix.
func (e ETag) String() string {
	if e.IsMultipart() {
		return hex.EncodeToString(e[:16]) + string(e[16:])
	}
	return hex.EncodeToString(e)
}

// IsEncrypted reports whether the ETag is encrypted.
func (e ETag) IsEncrypted() bool {
	// An encrypted ETag must be at least 32 bytes long.
	// It contains the encrypted ETag value + an authentication
	// code generated by the AEAD cipher.
	//
	// Here is an incorrect implementation of IsEncrypted:
	//
	//   return len(e) > 16 && !bytes.ContainsRune(e, '-')
	//
	// An encrypted ETag may contain some random bytes - e.g.
	// and nonce value. This nonce value may contain a '-'
	// just by its nature of being randomly generated.
	// The above implementation would incorrectly consider
	// such an ETag (with a nonce value containing a '-')
	// as non-encrypted.

	return len(e) >= 32 // We consider all ETags longer than 32 bytes as encrypted
}

// IsMultipart reports whether the ETag belongs to an
// object that has been uploaded using the S3 multipart
// API.
// An S3 multipart ETag has a -<part-number> suffix.
func (e ETag) IsMultipart() bool {
	return len(e) > 16 && !e.IsEncrypted() && bytes.ContainsRune(e, '-')
}

// Parts returns the number of object parts that are
// referenced by this ETag. It returns 1 if the object
// has been uploaded using the S3 singlepart API.
//
// Parts may panic if the ETag is an invalid multipart
// ETag.
func (e ETag) Parts() int {
	if !e.IsMultipart() {
		return 1
	}

	n := bytes.IndexRune(e, '-')
	parts, err := strconv.Atoi(string(e[n+1:]))
	if err != nil {
		panic(err) // malformed ETag
	}
	return parts
}

// Format returns an ETag that is formatted as specified
// by AWS S3.
//
// An AWS S3 ETag is 16 bytes long and, in case of a multipart
// upload, has a `-N` suffix encoding the number of object parts.
// An ETag is not AWS S3 compatible when encrypted. When sending
// an ETag back to an S3 client it has to be formatted to be
// AWS S3 compatible.
//
// Therefore, Format returns the last 16 bytes of an encrypted
// ETag.
//
// In general, a caller has to distinguish the following cases:
//   - The object is a multipart object. In this case,
//     Format returns the ETag unmodified.
//   - The object is a SSE-KMS or SSE-C encrypted single-
//     part object. In this case, Format returns the last
//     16 bytes of the encrypted ETag which will be a random
//     value.
//   - The object is a SSE-S3 encrypted single-part object.
//     In this case, the caller has to decrypt the ETag first
//     before calling Format.
//     S3 clients expect that the ETag of an SSE-S3 encrypted
//     single-part object is equal to the object's content MD5.
//     Formatting the SSE-S3 ETag before decryption will result
//     in a random-looking ETag which an S3 client will not accept.
//
// Hence, a caller has to check:
//
//	if method == SSE-S3 {
//	   ETag, err := Decrypt(key, ETag)
//	   if err != nil {
//	   }
//	}
//	ETag = ETag.Format()
func (e ETag) Format() ETag {
	if !e.IsEncrypted() {
		return e
	}
	return e[len(e)-16:]
}

var _ Tagger = ETag{} // compiler check

// ETag returns the ETag itself.
//
// By providing this method ETag implements
// the Tagger interface.
func (e ETag) ETag() ETag { return e }

// FromContentMD5 decodes and returns the Content-MD5
// as ETag, if set. If no Content-MD5 header is set
// it returns an empty ETag and no error.
func FromContentMD5(h http.Header) (ETag, error) {
	v, ok := h["Content-Md5"]
	if !ok {
		return nil, nil
	}
	if v[0] == "" {
		return nil, errors.New("etag: content-md5 is set but contains no value")
	}
	b, err := base64.StdEncoding.Strict().DecodeString(v[0])
	if err != nil {
		return nil, err
	}
	if len(b) != md5.Size {
		return nil, errors.New("etag: invalid content-md5")
	}
	return ETag(b), nil
}

// ContentMD5Requested - for http.request.header is not request Content-Md5
func ContentMD5Requested(h http.Header) bool {
	_, ok := h[xhttp.ContentMD5]
	return ok
}

// Multipart computes an S3 multipart ETag given a list of
// S3 singlepart ETags. It returns nil if the list of
// ETags is empty.
//
// Any encrypted or multipart ETag will be ignored and not
// used to compute the returned ETag.
func Multipart(etags ...ETag) ETag {
	if len(etags) == 0 {
		return nil
	}

	var n int64
	h := md5.New()
	for _, etag := range etags {
		if !etag.IsMultipart() && !etag.IsEncrypted() {
			h.Write(etag)
			n++
		}
	}
	etag := append(h.Sum(nil), '-')
	return strconv.AppendInt(etag, n, 10)
}

// Set adds the ETag to the HTTP headers. It overwrites any
// existing ETag entry.
//
// Due to legacy S3 clients, that make incorrect assumptions
// about HTTP headers, Set should be used instead of
// http.Header.Set(...). Otherwise, some S3 clients will not
// able to extract the ETag.
func Set(etag ETag, h http.Header) {
	// Some (broken) S3 clients expect the ETag header to
	// literally "ETag" - not "Etag". Further, some clients
	// expect an ETag in double quotes. Therefore, we set the
	// ETag directly as map entry instead of using http.Header.Set
	h["ETag"] = []string{`"` + etag.String() + `"`}
}

// Get extracts and parses an ETag from the given HTTP headers.
// It returns an error when the HTTP headers do not contain
// an ETag entry or when the ETag is malformed.
//
// Get only accepts AWS S3 compatible ETags - i.e. no
// encrypted ETags - and therefore is stricter than Parse.
func Get(h http.Header) (ETag, error) {
	const strict = true
	if v := h.Get("Etag"); v != "" {
		return parse(v, strict)
	}
	v, ok := h["ETag"]
	if !ok || len(v) == 0 {
		return nil, errors.New("etag: HTTP header does not contain an ETag")
	}
	return parse(v[0], strict)
}

// Equal returns true if and only if the two ETags are
// identical.
func Equal(a, b ETag) bool { return bytes.Equal(a, b) }

// Decrypt decrypts the ETag with the given key.
//
// If the ETag is not encrypted, Decrypt returns
// the ETag unmodified.
func Decrypt(key []byte, etag ETag) (ETag, error) {
	const HMACContext = "SSE-etag"

	if !etag.IsEncrypted() {
		return etag, nil
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(HMACContext))
	decryptionKey := mac.Sum(nil)

	plaintext := make([]byte, 0, 16)
	etag, err := sio.DecryptBuffer(plaintext, etag, sio.Config{
		Key: decryptionKey,
	})
	if err != nil {
		return nil, err
	}
	return etag, nil
}

// Parse parses s as an S3 ETag, returning the result.
// The string can be an encrypted, singlepart
// or multipart S3 ETag. It returns an error if s is
// not a valid textual representation of an ETag.
func Parse(s string) (ETag, error) {
	const strict = false
	return parse(s, strict)
}

// parse parse s as an S3 ETag, returning the result.
// It operates in one of two modes:
//   - strict
//   - non-strict
//
// In strict mode, parse only accepts ETags that
// are AWS S3 compatible. In particular, an AWS
// S3 ETag always consists of a 128 bit checksum
// value and an optional -<part-number> suffix.
// Therefore, s must have the following form in
// strict mode:  <32-hex-characters>[-<integer>]
//
// In non-strict mode, parse also accepts ETags
// that are not AWS S3 compatible - e.g. encrypted
// ETags.
func parse(s string, strict bool) (ETag, error) {
	// An S3 ETag may be a double-quoted string.
	// Therefore, we remove double quotes at the
	// start and end, if any.
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = s[1 : len(s)-1]
	}

	// An S3 ETag may be a multipart ETag that
	// contains a '-' followed by a number.
	// If the ETag does not a '-' is either
	// a singlepart or encrypted ETag.
	n := strings.IndexRune(s, '-')
	if n == -1 {
		etag, err := hex.DecodeString(s)
		if err != nil {
			return nil, err
		}
		if strict && len(etag) != 16 { // AWS S3 ETags are always 128 bit long
			return nil, fmt.Errorf("etag: invalid length %d", len(etag))
		}
		return ETag(etag), nil
	}

	prefix, suffix := s[:n], s[n:]
	if len(prefix) != 32 {
		return nil, fmt.Errorf("etag: invalid prefix length %d", len(prefix))
	}
	if len(suffix) <= 1 {
		return nil, errors.New("etag: suffix is not a part number")
	}

	etag, err := hex.DecodeString(prefix)
	if err != nil {
		return nil, err
	}
	partNumber, err := strconv.Atoi(suffix[1:]) // suffix[0] == '-' Therefore, we start parsing at suffix[1]
	if err != nil {
		return nil, err
	}
	if strict && (partNumber == 0 || partNumber > 10000) {
		return nil, fmt.Errorf("etag: invalid part number %d", partNumber)
	}
	return ETag(append(etag, suffix...)), nil
}
