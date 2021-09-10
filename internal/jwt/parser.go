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
	"crypto"
	"crypto/hmac"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	jwtgo "github.com/golang-jwt/jwt"
	jsoniter "github.com/json-iterator/go"
)

// SigningMethodHMAC - Implements the HMAC-SHA family of signing methods signing methods
// Expects key type of []byte for both signing and validation
type SigningMethodHMAC struct {
	Name string
	Hash crypto.Hash
}

// Specific instances for HS256, HS384, HS512
var (
	SigningMethodHS256 *SigningMethodHMAC
	SigningMethodHS384 *SigningMethodHMAC
	SigningMethodHS512 *SigningMethodHMAC
)

var (
	base64BufPool sync.Pool
	hmacSigners   []*SigningMethodHMAC
)

func init() {
	base64BufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 8192)
			return &buf
		},
	}

	hmacSigners = []*SigningMethodHMAC{
		{"HS256", crypto.SHA256},
		{"HS384", crypto.SHA384},
		{"HS512", crypto.SHA512},
	}
}

// StandardClaims are basically standard claims with "accessKey"
type StandardClaims struct {
	AccessKey string `json:"accessKey,omitempty"`
	jwtgo.StandardClaims
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

// Lookup returns the value and if the key is found.
func (c *MapClaims) Lookup(key string) (value string, ok bool) {
	if c == nil {
		return "", false
	}
	var vinterface interface{}
	vinterface, ok = c.MapClaims[key]
	if ok {
		value, ok = vinterface.(string)
	}
	return
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
func (c *MapClaims) Map() map[string]interface{} {
	if c == nil {
		return nil
	}
	return c.MapClaims
}

// MarshalJSON marshals the MapClaims struct
func (c *MapClaims) MarshalJSON() ([]byte, error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Marshal(c.MapClaims)
}

// ParseWithStandardClaims - parse the token string, valid methods.
func ParseWithStandardClaims(tokenStr string, claims *StandardClaims, key []byte) error {
	// Key is not provided.
	if key == nil {
		// keyFunc was not provided, return error.
		return jwtgo.NewValidationError("no key was provided.", jwtgo.ValidationErrorUnverifiable)
	}

	bufp := base64BufPool.Get().(*[]byte)
	defer base64BufPool.Put(bufp)

	signer, err := ParseUnverifiedStandardClaims(tokenStr, claims, *bufp)
	if err != nil {
		return err
	}

	i := strings.LastIndex(tokenStr, ".")
	if i < 0 {
		return jwtgo.ErrSignatureInvalid
	}

	n, err := base64Decode(tokenStr[i+1:], *bufp)
	if err != nil {
		return err
	}

	hasher := hmac.New(signer.Hash.New, key)
	hasher.Write([]byte(tokenStr[:i]))
	if !hmac.Equal((*bufp)[:n], hasher.Sum(nil)) {
		return jwtgo.ErrSignatureInvalid
	}

	if claims.AccessKey == "" && claims.Subject == "" {
		return jwtgo.NewValidationError("accessKey/sub missing",
			jwtgo.ValidationErrorClaimsInvalid)
	}

	// Signature is valid, lets validate the claims for
	// other fields such as expiry etc.
	return claims.Valid()
}

// https://tools.ietf.org/html/rfc7519#page-11
type jwtHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
}

// ParseUnverifiedStandardClaims - WARNING: Don't use this method unless you know what you're doing
//
// This method parses the token but doesn't validate the signature. It's only
// ever useful in cases where you know the signature is valid (because it has
// been checked previously in the stack) and you want to extract values from
// it.
func ParseUnverifiedStandardClaims(tokenString string, claims *StandardClaims, buf []byte) (*SigningMethodHMAC, error) {
	if strings.Count(tokenString, ".") != 2 {
		return nil, jwtgo.ErrSignatureInvalid
	}

	i := strings.Index(tokenString, ".")
	j := strings.LastIndex(tokenString, ".")

	n, err := base64Decode(tokenString[:i], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	var header = jwtHeader{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(buf[:n], &header); err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	n, err = base64Decode(tokenString[i+1:j], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	if err = json.Unmarshal(buf[:n], claims); err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	for _, signer := range hmacSigners {
		if header.Algorithm == signer.Name {
			return signer, nil
		}
	}

	return nil, jwtgo.NewValidationError(fmt.Sprintf("signing method (%s) is unavailable.", header.Algorithm),
		jwtgo.ValidationErrorUnverifiable)
}

// ParseWithClaims - parse the token string, valid methods.
func ParseWithClaims(tokenStr string, claims *MapClaims, fn func(*MapClaims) ([]byte, error)) error {
	// Key lookup function has to be provided.
	if fn == nil {
		// keyFunc was not provided, return error.
		return jwtgo.NewValidationError("no Keyfunc was provided.", jwtgo.ValidationErrorUnverifiable)
	}

	bufp := base64BufPool.Get().(*[]byte)
	defer base64BufPool.Put(bufp)

	signer, err := ParseUnverifiedMapClaims(tokenStr, claims, *bufp)
	if err != nil {
		return err
	}

	i := strings.LastIndex(tokenStr, ".")
	if i < 0 {
		return jwtgo.ErrSignatureInvalid
	}

	n, err := base64Decode(tokenStr[i+1:], *bufp)
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

	hasher := hmac.New(signer.Hash.New, key)
	hasher.Write([]byte(tokenStr[:i]))
	if !hmac.Equal((*bufp)[:n], hasher.Sum(nil)) {
		return jwtgo.ErrSignatureInvalid
	}

	// Signature is valid, lets validate the claims for
	// other fields such as expiry etc.
	return claims.Valid()
}

// base64Decode returns the bytes represented by the base64 string s.
func base64Decode(s string, buf []byte) (int, error) {
	return base64.RawURLEncoding.Decode(buf, []byte(s))
}

// ParseUnverifiedMapClaims - WARNING: Don't use this method unless you know what you're doing
//
// This method parses the token but doesn't validate the signature. It's only
// ever useful in cases where you know the signature is valid (because it has
// been checked previously in the stack) and you want to extract values from
// it.
func ParseUnverifiedMapClaims(tokenString string, claims *MapClaims, buf []byte) (*SigningMethodHMAC, error) {
	if strings.Count(tokenString, ".") != 2 {
		return nil, jwtgo.ErrSignatureInvalid
	}

	i := strings.Index(tokenString, ".")
	j := strings.LastIndex(tokenString, ".")

	n, err := base64Decode(tokenString[:i], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	var header = jwtHeader{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(buf[:n], &header); err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	n, err = base64Decode(tokenString[i+1:j], buf)
	if err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	if err = json.Unmarshal(buf[:n], &claims.MapClaims); err != nil {
		return nil, &jwtgo.ValidationError{Inner: err, Errors: jwtgo.ValidationErrorMalformed}
	}

	for _, signer := range hmacSigners {
		if header.Algorithm == signer.Name {
			return signer, nil
		}
	}

	return nil, jwtgo.NewValidationError(fmt.Sprintf("signing method (%s) is unavailable.", header.Algorithm),
		jwtgo.ValidationErrorUnverifiable)
}
