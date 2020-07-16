/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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

package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
)

const (
	// Minimum length for MinIO access key.
	accessKeyMinLen = 3

	// Maximum length for MinIO access key.
	// There is no max length enforcement for access keys
	accessKeyMaxLen = 20

	// Minimum length for MinIO secret key for both server and gateway mode.
	secretKeyMinLen = 8

	// Maximum secret key length for MinIO, this
	// is used when autogenerating new credentials.
	// There is no max length enforcement for secret keys
	secretKeyMaxLen = 40

	// Alpha numeric table used for generating access keys.
	alphaNumericTable = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Total length of the alpha numeric table.
	alphaNumericTableLen = byte(len(alphaNumericTable))
)

// Common errors generated for access and secret key validation.
var (
	ErrInvalidAccessKeyLength = fmt.Errorf("access key must be minimum %v or more characters long", accessKeyMinLen)
	ErrInvalidSecretKeyLength = fmt.Errorf("secret key must be minimum %v or more characters long", secretKeyMinLen)
)

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

// Credentials holds access and secret keys.
type Credentials struct {
	AccessKey    string    `xml:"AccessKeyId" json:"accessKey,omitempty"`
	SecretKey    string    `xml:"SecretAccessKey" json:"secretKey,omitempty"`
	Expiration   time.Time `xml:"Expiration" json:"expiration,omitempty"`
	SessionToken string    `xml:"SessionToken" json:"sessionToken,omitempty"`
	Status       string    `xml:"-" json:"status,omitempty"`
	ParentUser   string    `xml:"-" json:"parentUser,omitempty"`
	Groups       []string  `xml:"-" json:"groups,omitempty"`
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
	return cred.ParentUser != "" && (cred.Expiration.IsZero() || cred.Expiration.Equal(timeSentinel))
}

// IsValid - returns whether credential is valid or not.
func (cred Credentials) IsValid() bool {
	// Verify credentials if its enabled or not set.
	if cred.Status == "off" {
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
func ExpToInt64(expI interface{}) (expAt int64, err error) {
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

// GetNewCredentialsWithMetadata generates and returns new credential with expiry.
func GetNewCredentialsWithMetadata(m map[string]interface{}, tokenSecret string) (cred Credentials, err error) {
	readBytes := func(size int) (data []byte, err error) {
		data = make([]byte, size)
		var n int
		if n, err = rand.Read(data); err != nil {
			return nil, err
		} else if n != size {
			return nil, fmt.Errorf("Not enough data. Expected to read: %v bytes, got: %v bytes", size, n)
		}
		return data, nil
	}

	// Generate access key.
	keyBytes, err := readBytes(accessKeyMaxLen)
	if err != nil {
		return cred, err
	}
	for i := 0; i < accessKeyMaxLen; i++ {
		keyBytes[i] = alphaNumericTable[keyBytes[i]%alphaNumericTableLen]
	}
	cred.AccessKey = string(keyBytes)

	// Generate secret key.
	keyBytes, err = readBytes(secretKeyMaxLen)
	if err != nil {
		return cred, err
	}
	cred.SecretKey = strings.Replace(string([]byte(base64.StdEncoding.EncodeToString(keyBytes))[:secretKeyMaxLen]),
		"/", "+", -1)
	cred.Status = "on"

	if tokenSecret == "" {
		cred.Expiration = timeSentinel
		return cred, nil
	}

	expiry, err := ExpToInt64(m["exp"])
	if err != nil {
		return cred, err
	}

	m["accessKey"] = cred.AccessKey
	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.MapClaims(m))

	cred.Expiration = time.Unix(expiry, 0).UTC()
	cred.SessionToken, err = jwt.SignedString([]byte(tokenSecret))
	if err != nil {
		return cred, err
	}

	return cred, nil
}

// GetNewCredentials generates and returns new credential.
func GetNewCredentials() (cred Credentials, err error) {
	return GetNewCredentialsWithMetadata(map[string]interface{}{}, "")
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
	cred.Status = "on"
	return cred, nil
}
