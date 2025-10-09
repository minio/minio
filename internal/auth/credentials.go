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

package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	"github.com/minio/minio/internal/jwt"
)

const (
	// Minimum length for MinIO access key.
	accessKeyMinLen = 3

	// Maximum length for MinIO access key.
	// There is no max length enforcement for access keys
	accessKeyMaxLen = 20

	// Minimum length for MinIO secret key for both server
	secretKeyMinLen = 8

	// Maximum secret key length for MinIO, this
	// is used when autogenerating new credentials.
	// There is no max length enforcement for secret keys
	secretKeyMaxLen = 40

	// Alpha numeric table used for generating access keys.
	alphaNumericTable = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Total length of the alpha numeric table.
	alphaNumericTableLen = byte(len(alphaNumericTable))

	reservedChars = "=,"
)

// Common errors generated for access and secret key validation.
var (
	ErrInvalidAccessKeyLength   = fmt.Errorf("access key length should be between %d and %d", accessKeyMinLen, accessKeyMaxLen)
	ErrInvalidSecretKeyLength   = fmt.Errorf("secret key length should be between %d and %d", secretKeyMinLen, secretKeyMaxLen)
	ErrNoAccessKeyWithSecretKey = fmt.Errorf("access key must be specified if secret key is specified")
	ErrNoSecretKeyWithAccessKey = fmt.Errorf("secret key must be specified if access key is specified")
	ErrContainsReservedChars    = fmt.Errorf("access key contains one of reserved characters '=' or ','")
)

// AnonymousCredentials simply points to empty credentials
var AnonymousCredentials = Credentials{}

// ContainsReservedChars - returns whether the input string contains reserved characters.
func ContainsReservedChars(s string) bool {
	return strings.ContainsAny(s, reservedChars)
}

// IsAccessKeyValid - validate access key for right length.
func IsAccessKeyValid(accessKey string) bool {
	return len(accessKey) >= accessKeyMinLen
}

// IsSecretKeyValid - validate secret key for right length.
func IsSecretKeyValid(secretKey string) bool {
	return len(secretKey) >= secretKeyMinLen
}

// Default access and secret keys.
const (
	DefaultAccessKey = "minioadmin"
	DefaultSecretKey = "minioadmin"
)

// Default access credentials
var (
	DefaultCredentials = Credentials{
		AccessKey: DefaultAccessKey,
		SecretKey: DefaultSecretKey,
	}
)

// claim key found in credentials which are service accounts
const iamPolicyClaimNameSA = "sa-policy"

const (
	// AccountOn indicates that credentials are enabled
	AccountOn = "on"
	// AccountOff indicates that credentials are disabled
	AccountOff = "off"
)

// Credentials holds access and secret keys.
type Credentials struct {
	AccessKey    string         `xml:"AccessKeyId" json:"accessKey,omitempty" yaml:"accessKey"`
	SecretKey    string         `xml:"SecretAccessKey" json:"secretKey,omitempty" yaml:"secretKey"`
	SessionToken string         `xml:"SessionToken" json:"sessionToken,omitempty" yaml:"sessionToken"`
	Expiration   time.Time      `xml:"Expiration" json:"expiration" yaml:"-"`
	Status       string         `xml:"-" json:"status,omitempty"`
	ParentUser   string         `xml:"-" json:"parentUser,omitempty"`
	Groups       []string       `xml:"-" json:"groups,omitempty"`
	Claims       map[string]any `xml:"-" json:"claims,omitempty"`
	Name         string         `xml:"-" json:"name,omitempty"`
	Description  string         `xml:"-" json:"description,omitempty"`

	// Deprecated: In favor of Description - when reading credentials from
	// storage the value of this field is placed in the Description field above
	// if the existing Description from storage is empty.
	Comment string `xml:"-" json:"comment,omitempty"`
}

func (cred Credentials) String() string {
	var s strings.Builder
	s.WriteString(cred.AccessKey)
	s.WriteString(":")
	s.WriteString(cred.SecretKey)
	if cred.SessionToken != "" {
		s.WriteString("\n")
		s.WriteString(cred.SessionToken)
	}
	if !cred.Expiration.IsZero() && !cred.Expiration.Equal(timeSentinel) {
		s.WriteString("\n")
		s.WriteString(cred.Expiration.String())
	}
	return s.String()
}

// IsExpired - returns whether Credential is expired or not.
func (cred Credentials) IsExpired() bool {
	if cred.Expiration.IsZero() || cred.Expiration.Equal(timeSentinel) {
		return false
	}

	return cred.Expiration.Before(time.Now().UTC())
}

// IsTemp - returns whether credential is temporary or not.
func (cred Credentials) IsTemp() bool {
	return cred.SessionToken != "" && !cred.Expiration.IsZero() && !cred.Expiration.Equal(timeSentinel)
}

// IsServiceAccount - returns whether credential is a service account or not
func (cred Credentials) IsServiceAccount() bool {
	_, ok := cred.Claims[iamPolicyClaimNameSA]
	return cred.ParentUser != "" && ok
}

// IsImpliedPolicy - returns if the policy is implied via ParentUser or not.
func (cred Credentials) IsImpliedPolicy() bool {
	if cred.IsServiceAccount() {
		return cred.Claims[iamPolicyClaimNameSA] == "inherited-policy"
	}
	return false
}

// IsValid - returns whether credential is valid or not.
func (cred Credentials) IsValid() bool {
	// Verify credentials if its enabled or not set.
	if cred.Status == AccountOff {
		return false
	}
	return IsAccessKeyValid(cred.AccessKey) && IsSecretKeyValid(cred.SecretKey) && !cred.IsExpired()
}

// Equal - returns whether two credentials are equal or not.
func (cred Credentials) Equal(ccred Credentials) bool {
	if !ccred.IsValid() {
		return false
	}
	return (cred.AccessKey == ccred.AccessKey && subtle.ConstantTimeCompare([]byte(cred.SecretKey), []byte(ccred.SecretKey)) == 1 &&
		subtle.ConstantTimeCompare([]byte(cred.SessionToken), []byte(ccred.SessionToken)) == 1)
}

var timeSentinel = time.Unix(0, 0).UTC()

// ErrInvalidDuration invalid token expiry
var ErrInvalidDuration = errors.New("invalid token expiry")

// ExpToInt64 - convert input interface value to int64.
func ExpToInt64(expI any) (expAt int64, err error) {
	switch exp := expI.(type) {
	case string:
		expAt, err = strconv.ParseInt(exp, 10, 64)
	case float64:
		expAt, err = int64(exp), nil
	case int64:
		expAt, err = exp, nil
	case int:
		expAt, err = int64(exp), nil
	case uint64:
		expAt, err = int64(exp), nil
	case uint:
		expAt, err = int64(exp), nil
	case json.Number:
		expAt, err = exp.Int64()
	case time.Duration:
		expAt, err = time.Now().UTC().Add(exp).Unix(), nil
	case nil:
		expAt, err = 0, nil
	default:
		expAt, err = 0, ErrInvalidDuration
	}
	if expAt < 0 {
		return 0, ErrInvalidDuration
	}
	return expAt, err
}

// GenerateCredentials - creates randomly generated credentials of maximum
// allowed length.
func GenerateCredentials() (accessKey, secretKey string, err error) {
	accessKey, err = GenerateAccessKey(accessKeyMaxLen, rand.Reader)
	if err != nil {
		return "", "", err
	}
	secretKey, err = GenerateSecretKey(secretKeyMaxLen, rand.Reader)
	if err != nil {
		return "", "", err
	}
	return accessKey, secretKey, nil
}

// GenerateAccessKey returns a new access key generated randomly using
// the given io.Reader. If random is nil, crypto/rand.Reader is used.
// If length <= 0, the access key length is chosen automatically.
//
// GenerateAccessKey returns an error if length is too small for a valid
// access key.
func GenerateAccessKey(length int, random io.Reader) (string, error) {
	if random == nil {
		random = rand.Reader
	}
	if length <= 0 {
		length = accessKeyMaxLen
	}
	if length < accessKeyMinLen {
		return "", errors.New("auth: access key length is too short")
	}

	key := make([]byte, length)
	if _, err := io.ReadFull(random, key); err != nil {
		return "", err
	}
	for i := range key {
		key[i] = alphaNumericTable[key[i]%alphaNumericTableLen]
	}
	return string(key), nil
}

// GenerateSecretKey returns a new secret key generated randomly using
// the given io.Reader. If random is nil, crypto/rand.Reader is used.
// If length <= 0, the secret key length is chosen automatically.
//
// GenerateSecretKey returns an error if length is too small for a valid
// secret key.
func GenerateSecretKey(length int, random io.Reader) (string, error) {
	if random == nil {
		random = rand.Reader
	}
	if length <= 0 {
		length = secretKeyMaxLen
	}
	if length < secretKeyMinLen {
		return "", errors.New("auth: secret key length is too short")
	}

	key := make([]byte, base64.RawStdEncoding.DecodedLen(length))
	if _, err := io.ReadFull(random, key); err != nil {
		return "", err
	}

	s := base64.RawStdEncoding.EncodeToString(key)
	return strings.ReplaceAll(s, "/", "+"), nil
}

// GetNewCredentialsWithMetadata generates and returns new credential with expiry.
func GetNewCredentialsWithMetadata(m map[string]any, tokenSecret string) (Credentials, error) {
	accessKey, secretKey, err := GenerateCredentials()
	if err != nil {
		return Credentials{}, err
	}
	return CreateNewCredentialsWithMetadata(accessKey, secretKey, m, tokenSecret)
}

// CreateNewCredentialsWithMetadata - creates new credentials using the specified access & secret keys
// and generate a session token if a secret token is provided.
func CreateNewCredentialsWithMetadata(accessKey, secretKey string, m map[string]any, tokenSecret string) (cred Credentials, err error) {
	if len(accessKey) < accessKeyMinLen || len(accessKey) > accessKeyMaxLen {
		return Credentials{}, ErrInvalidAccessKeyLength
	}

	if len(secretKey) < secretKeyMinLen || len(secretKey) > secretKeyMaxLen {
		return Credentials{}, ErrInvalidSecretKeyLength
	}

	cred.AccessKey = accessKey
	cred.SecretKey = secretKey
	cred.Status = AccountOn

	if tokenSecret == "" {
		cred.Expiration = timeSentinel
		return cred, nil
	}

	expiry, err := ExpToInt64(m["exp"])
	if err != nil {
		return cred, err
	}
	cred.Expiration = time.Unix(expiry, 0).UTC()

	cred.SessionToken, err = JWTSignWithAccessKey(cred.AccessKey, m, tokenSecret)
	if err != nil {
		return cred, err
	}

	return cred, nil
}

// JWTSignWithAccessKey - generates a session token.
func JWTSignWithAccessKey(accessKey string, m map[string]any, tokenSecret string) (string, error) {
	m["accessKey"] = accessKey
	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.MapClaims(m))
	return jwt.SignedString([]byte(tokenSecret))
}

// ExtractClaims extracts JWT claims from a security token using a secret key
func ExtractClaims(token, secretKey string) (*jwt.MapClaims, error) {
	if token == "" || secretKey == "" {
		return nil, errors.New("invalid argument")
	}

	claims := jwt.NewMapClaims()
	stsTokenCallback := func(claims *jwt.MapClaims) ([]byte, error) {
		return []byte(secretKey), nil
	}

	if err := jwt.ParseWithClaims(token, claims, stsTokenCallback); err != nil {
		return nil, err
	}

	return claims, nil
}

// GetNewCredentials generates and returns new credential.
func GetNewCredentials() (cred Credentials, err error) {
	return GetNewCredentialsWithMetadata(map[string]any{}, "")
}

// CreateCredentials returns new credential with the given access key and secret key.
// Error is returned if given access key or secret key are invalid length.
func CreateCredentials(accessKey, secretKey string) (cred Credentials, err error) {
	if !IsAccessKeyValid(accessKey) {
		return cred, ErrInvalidAccessKeyLength
	}
	if !IsSecretKeyValid(secretKey) {
		return cred, ErrInvalidSecretKeyLength
	}
	cred.AccessKey = accessKey
	cred.SecretKey = secretKey
	cred.Expiration = timeSentinel
	cred.Status = AccountOn
	return cred, nil
}
