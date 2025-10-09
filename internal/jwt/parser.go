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

package jwt

// This file is a re-implementation of the original code here with some
// additional allocation tweaks reproduced using GODEBUG=allocfreetrace=1
// original file https://github.com/golang-jwt/jwt/blob/main/parser.go
// borrowed under MIT License https://github.com/golang-jwt/jwt/blob/main/LICENSE

import (
	"bytes"
	"crypto"
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/buger/jsonparser"
	"github.com/dustin/go-humanize"
	jwtgo "github.com/golang-jwt/jwt/v4"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/bpool"
)

// SigningMethodHMAC - Implements the HMAC-SHA family of signing methods signing methods
// Expects key type of []byte for both signing and validation
type SigningMethodHMAC struct {
	Name       string
	Hash       crypto.Hash
	HasherPool bpool.Pool[hash.Hash]
}

// Specific instances for HS256, HS384, HS512
var (
	SigningMethodHS256 *SigningMethodHMAC
	SigningMethodHS384 *SigningMethodHMAC
	SigningMethodHS512 *SigningMethodHMAC
)

const base64BufferSize = 64 * humanize.KiByte

var (
	base64BufPool bpool.Pool[*[]byte]
	hmacSigners   []*SigningMethodHMAC
)

func init() {
	base64BufPool = bpool.Pool[*[]byte]{
		New: func() *[]byte {
			buf := make([]byte, base64BufferSize)
			return &buf
		},
	}

	hmacSigners = []*SigningMethodHMAC{
		{Name: "HS256", Hash: crypto.SHA256},
		{Name: "HS384", Hash: crypto.SHA384},
		{Name: "HS512", Hash: crypto.SHA512},
	}
	for i := range hmacSigners {
		h := hmacSigners[i].Hash
		hmacSigners[i].HasherPool.New = func() hash.Hash {
			return h.New()
		}
	}
}

// HashBorrower allows borrowing hashes and will keep track of them.
func (s *SigningMethodHMAC) HashBorrower() HashBorrower {
	return HashBorrower{pool: &s.HasherPool, borrowed: make([]hash.Hash, 0, 2)}
}

// HashBorrower keeps track of borrowed hashers and allows to return them all.
type HashBorrower struct {
	pool     *bpool.Pool[hash.Hash]
	borrowed []hash.Hash
}

// Borrow a single hasher.
func (h *HashBorrower) Borrow() hash.Hash {
	hasher := h.pool.Get()
	h.borrowed = append(h.borrowed, hasher)
	hasher.Reset()
	return hasher
}

// ReturnAll will return all borrowed hashes.
func (h *HashBorrower) ReturnAll() {
	for _, hasher := range h.borrowed {
		h.pool.Put(hasher)
	}
	h.borrowed = nil
}

// StandardClaims are basically standard claims with "accessKey"
type StandardClaims struct {
	AccessKey string `json:"accessKey,omitempty"`
	jwtgo.StandardClaims
}

// UnmarshalJSON provides custom JSON unmarshal.
// This is mainly implemented for speed.
func (c *StandardClaims) UnmarshalJSON(b []byte) (err error) {
	return jsonparser.ObjectEach(b, func(key []byte, value []byte, dataType jsonparser.ValueType, _ int) error {
		if len(key) == 0 {
			return nil
		}
		switch key[0] {
		case 'a':
			if string(key) == "accessKey" {
				if dataType != jsonparser.String {
					return errors.New("accessKey: Expected string")
				}
				c.AccessKey, err = jsonparser.ParseString(value)
				return err
			}
			if string(key) == "aud" {
				if dataType != jsonparser.String {
					return errors.New("aud: Expected string")
				}
				c.Audience, err = jsonparser.ParseString(value)
				return err
			}
		case 'e':
			if string(key) == "exp" {
				if dataType != jsonparser.Number {
					return errors.New("exp: Expected number")
				}
				c.ExpiresAt, err = jsonparser.ParseInt(value)
				return err
			}
		case 'i':
			if string(key) == "iat" {
				if dataType != jsonparser.Number {
					return errors.New("exp: Expected number")
				}
				c.IssuedAt, err = jsonparser.ParseInt(value)
				return err
			}
			if string(key) == "iss" {
				if dataType != jsonparser.String {
					return errors.New("iss: Expected string")
				}
				c.Issuer, err = jsonparser.ParseString(value)
				return err
			}
		case 'n':
			if string(key) == "nbf" {
				if dataType != jsonparser.Number {
					return errors.New("nbf: Expected number")
				}
				c.NotBefore, err = jsonparser.ParseInt(value)
				return err
			}
		case 's':
			if string(key) == "sub" {
				if dataType != jsonparser.String {
					return errors.New("sub: Expected string")
				}
				c.Subject, err = jsonparser.ParseString(value)
				return err
			}
		}
		// Ignore unknown fields
		return nil
	})
}

// MapClaims - implements custom unmarshaller
type MapClaims struct {
	AccessKey string `json:"accessKey,omitempty"`
	jwtgo.MapClaims
}

// GetAccessKey will return the access key.
// If nil an empty string will be returned.
func (c *MapClaims) GetAccessKey() string {
	if c == nil {
		return ""
	}
	return c.AccessKey
}

// NewStandardClaims - initializes standard claims
func NewStandardClaims() *StandardClaims {
	return &StandardClaims{}
}

// SetIssuer sets issuer for these claims
func (c *StandardClaims) SetIssuer(issuer string) {
	c.Issuer = issuer
}

// SetAudience sets audience for these claims
func (c *StandardClaims) SetAudience(aud string) {
	c.Audience = aud
}

// SetExpiry sets expiry in unix epoch secs
func (c *StandardClaims) SetExpiry(t time.Time) {
	c.ExpiresAt = t.Unix()
}

// SetAccessKey sets access key as jwt subject and custom
// "accessKey" field.
func (c *StandardClaims) SetAccessKey(accessKey string) {
	c.Subject = accessKey
	c.AccessKey = accessKey
}

// Valid - implements https://godoc.org/github.com/golang-jwt/jwt#Claims compatible
// claims interface, additionally validates "accessKey" fields.
func (c *StandardClaims) Valid() error {
	if err := c.StandardClaims.Valid(); err != nil {
		return err
	}

	if c.AccessKey == "" && c.Subject == "" {
		return jwtgo.NewValidationError("accessKey/sub missing",
			jwtgo.ValidationErrorClaimsInvalid)
	}

	return nil
}

// NewMapClaims - Initializes a new map claims
func NewMapClaims() *MapClaims {
	return &MapClaims{MapClaims: jwtgo.MapClaims{}}
}

// Set Adds new arbitrary claim keys and values.
func (c *MapClaims) Set(key string, val any) {
	if c == nil {
		return
	}
	c.MapClaims[key] = val
}

// Delete deletes a key named key.
func (c *MapClaims) Delete(key string) {
	if c == nil {
		return
	}
	delete(c.MapClaims, key)
}

// Lookup returns the value and if the key is found.
func (c *MapClaims) Lookup(key string) (value string, ok bool) {
	if c == nil {
		return "", false
	}
	var vinterface any
	vinterface, ok = c.MapClaims[key]
	if ok {
		value, ok = vinterface.(string)
	}
	return value, ok
}

// SetExpiry sets expiry in unix epoch secs
func (c *MapClaims) SetExpiry(t time.Time) {
	c.MapClaims["exp"] = t.Unix()
}

// SetAccessKey sets access key as jwt subject and custom
// "accessKey" field.
func (c *MapClaims) SetAccessKey(accessKey string) {
	c.MapClaims["sub"] = accessKey
	c.MapClaims["accessKey"] = accessKey
}

// Valid - implements https://godoc.org/github.com/golang-jwt/jwt#Claims compatible
// claims interface, additionally validates "accessKey" fields.
func (c *MapClaims) Valid() error {
	if err := c.MapClaims.Valid(); err != nil {
		return err
	}

	if c.AccessKey == "" {
		return jwtgo.NewValidationError("accessKey/sub missing",
			jwtgo.ValidationErrorClaimsInvalid)
	}

	return nil
}

// Map returns underlying low-level map claims.
func (c *MapClaims) Map() map[string]any {
	if c == nil {
		return nil
	}
	return c.MapClaims
}

// MarshalJSON marshals the MapClaims struct
func (c *MapClaims) MarshalJSON() ([]byte, error) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Marshal(c.MapClaims)
}

// ParseWithStandardClaims - parse the token string, valid methods.
func ParseWithStandardClaims(tokenStr string, claims *StandardClaims, key []byte) error {
	// Key is not provided.
	if key == nil {
		// keyFunc was not provided, return error.
		return jwtgo.NewValidationError("no key was provided.", jwtgo.ValidationErrorUnverifiable)
	}

	bufp := base64BufPool.Get()
	defer base64BufPool.Put(bufp)

	tokenBuf := base64BufPool.Get()
	defer base64BufPool.Put(tokenBuf)

	token := *tokenBuf
	// Copy token to buffer, truncate to length.
	token = token[:copy(token[:base64BufferSize], tokenStr)]

	signer, err := ParseUnverifiedStandardClaims(token, claims, *bufp)
	if err != nil {
		return err
	}

	i := bytes.LastIndexByte(token, '.')
	if i < 0 {
		return jwtgo.ErrSignatureInvalid
	}

	n, err := base64DecodeBytes(token[i+1:], *bufp)
	if err != nil {
		return err
	}
	borrow := signer.HashBorrower()
	hasher := hmac.New(borrow.Borrow, key)
	hasher.Write(token[:i])
	if !hmac.Equal((*bufp)[:n], hasher.Sum(nil)) {
		borrow.ReturnAll()
		return jwtgo.ErrSignatureInvalid
	}
	borrow.ReturnAll()

	if claims.AccessKey == "" && claims.Subject == "" {
		return jwtgo.NewValidationError("accessKey/sub missing",
			jwtgo.ValidationErrorClaimsInvalid)
	}

	// Signature is valid, lets validate the claims for
	// other fields such as expiry etc.
	return claims.Valid()
}

// ParseUnverifiedStandardClaims - WARNING: Don't use this method unless you know what you're doing
//
// This method parses the token but doesn't validate the signature. It's only
// ever useful in cases where you know the signature is valid (because it has
// been checked previously in the stack) and you want to extract values from
// it.
func ParseUnverifiedStandardClaims(token []byte, claims *StandardClaims, buf []byte) (*SigningMethodHMAC, error) {
	if bytes.Count(token, []byte(".")) != 2 {
		return nil, jwtgo.ErrSignatureInvalid
	}

	i := bytes.IndexByte(token, '.')
	j := bytes.LastIndexByte(token, '.')

	n, err := base64DecodeBytes(token[:i], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}
	headerDec := buf[:n]
	buf = buf[n:]

	alg, _, _, err := jsonparser.Get(headerDec, "alg")
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	n, err = base64DecodeBytes(token[i+1:j], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	if err = claims.UnmarshalJSON(buf[:n]); err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	for _, signer := range hmacSigners {
		if string(alg) == signer.Name {
			return signer, nil
		}
	}

	return nil, jwtgo.NewValidationError(fmt.Sprintf("signing method (%s) is unavailable.", string(alg)),
		jwtgo.ValidationErrorUnverifiable)
}

// ParseWithClaims - parse the token string, valid methods.
func ParseWithClaims(tokenStr string, claims *MapClaims, fn func(*MapClaims) ([]byte, error)) error {
	// Key lookup function has to be provided.
	if fn == nil {
		// keyFunc was not provided, return error.
		return jwtgo.NewValidationError("no Keyfunc was provided.", jwtgo.ValidationErrorUnverifiable)
	}

	bufp := base64BufPool.Get()
	defer base64BufPool.Put(bufp)

	tokenBuf := base64BufPool.Get()
	defer base64BufPool.Put(tokenBuf)

	token := *tokenBuf
	// Copy token to buffer, truncate to length.
	token = token[:copy(token[:base64BufferSize], tokenStr)]

	signer, err := ParseUnverifiedMapClaims(token, claims, *bufp)
	if err != nil {
		return err
	}

	i := bytes.LastIndexByte(token, '.')
	if i < 0 {
		return jwtgo.ErrSignatureInvalid
	}

	n, err := base64DecodeBytes(token[i+1:], *bufp)
	if err != nil {
		return err
	}

	var ok bool
	claims.AccessKey, ok = claims.Lookup("accessKey")
	if !ok {
		claims.AccessKey, ok = claims.Lookup("sub")
		if !ok {
			return jwtgo.NewValidationError("accessKey/sub missing",
				jwtgo.ValidationErrorClaimsInvalid)
		}
	}

	// Lookup key from claims, claims may not be valid and may return
	// invalid key which is okay as the signature verification will fail.
	key, err := fn(claims)
	if err != nil {
		return err
	}
	borrow := signer.HashBorrower()
	hasher := hmac.New(borrow.Borrow, key)
	hasher.Write([]byte(tokenStr[:i]))
	if !hmac.Equal((*bufp)[:n], hasher.Sum(nil)) {
		borrow.ReturnAll()
		return jwtgo.ErrSignatureInvalid
	}
	borrow.ReturnAll()

	// Signature is valid, lets validate the claims for
	// other fields such as expiry etc.
	return claims.Valid()
}

// base64DecodeBytes returns the bytes represented by the base64 string s.
func base64DecodeBytes(b []byte, buf []byte) (int, error) {
	return base64.RawURLEncoding.Decode(buf, b)
}

// ParseUnverifiedMapClaims - WARNING: Don't use this method unless you know what you're doing
//
// This method parses the token but doesn't validate the signature. It's only
// ever useful in cases where you know the signature is valid (because it has
// been checked previously in the stack) and you want to extract values from
// it.
func ParseUnverifiedMapClaims(token []byte, claims *MapClaims, buf []byte) (*SigningMethodHMAC, error) {
	if bytes.Count(token, []byte(".")) != 2 {
		return nil, jwtgo.ErrSignatureInvalid
	}

	i := bytes.IndexByte(token, '.')
	j := bytes.LastIndexByte(token, '.')

	n, err := base64DecodeBytes(token[:i], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	headerDec := buf[:n]
	buf = buf[n:]
	alg, _, _, err := jsonparser.Get(headerDec, "alg")
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	n, err = base64DecodeBytes(token[i+1:j], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(buf[:n], &claims.MapClaims); err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	for _, signer := range hmacSigners {
		if string(alg) == signer.Name {
			return signer, nil
		}
	}

	return nil, jwtgo.NewValidationError(fmt.Sprintf("signing method (%s) is unavailable.", string(alg)),
		jwtgo.ValidationErrorUnverifiable)
}
