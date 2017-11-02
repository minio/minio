/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"fmt"
)

const (
	// Minimum length for Minio access key.
	accessKeyMinLen = 5

	// Maximum length for Minio access key.
	// There is no max length enforcement for access keys
	accessKeyMaxLen = 20

	// Minimum length for Minio secret key for both server and gateway mode.
	secretKeyMinLen = 8

	// Maximum secret key length for Minio, this
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

// isSecretKeyValid - validate secret key for right length.
func isSecretKeyValid(secretKey string) bool {
	return len(secretKey) >= secretKeyMinLen
}

// Credentials holds access and secret keys.
type Credentials struct {
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
}

// IsValid - returns whether credential is valid or not.
func (cred Credentials) IsValid() bool {
	return IsAccessKeyValid(cred.AccessKey) && isSecretKeyValid(cred.SecretKey)
}

// Equal - returns whether two credentials are equal or not.
func (cred Credentials) Equal(ccred Credentials) bool {
	if !ccred.IsValid() {
		return false
	}
	return cred.AccessKey == ccred.AccessKey && subtle.ConstantTimeCompare([]byte(cred.SecretKey), []byte(ccred.SecretKey)) == 1
}

// MustGetNewCredentials generates and returns new credential.
func MustGetNewCredentials() (cred Credentials) {
	readBytes := func(size int) (data []byte) {
		data = make([]byte, size)
		if n, err := rand.Read(data); err != nil {
			panic(err)
		} else if n != size {
			panic(fmt.Errorf("not enough data read. expected: %v, got: %v", size, n))
		}
		return
	}

	// Generate access key.
	keyBytes := readBytes(accessKeyMaxLen)
	for i := 0; i < accessKeyMaxLen; i++ {
		keyBytes[i] = alphaNumericTable[keyBytes[i]%alphaNumericTableLen]
	}
	cred.AccessKey = string(keyBytes)

	// Generate secret key.
	keyBytes = readBytes(secretKeyMaxLen)
	cred.SecretKey = string([]byte(base64.StdEncoding.EncodeToString(keyBytes))[:secretKeyMaxLen])

	return cred
}

// CreateCredentials returns new credential with the given access key and secret key.
// Error is returned if given access key or secret key are invalid length.
func CreateCredentials(accessKey, secretKey string) (cred Credentials, err error) {
	if !IsAccessKeyValid(accessKey) {
		return cred, ErrInvalidAccessKeyLength
	}
	if !isSecretKeyValid(secretKey) {
		return cred, ErrInvalidSecretKeyLength
	}

	cred.AccessKey = accessKey
	cred.SecretKey = secretKey
	return cred, nil
}
