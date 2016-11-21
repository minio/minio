/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"errors"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/bcrypt"
)

const jwtAlgorithm = "Bearer"

// JWT - jwt auth backend
type JWT struct {
	credential
	expiry time.Duration
}

const (
	// Default JWT token for web handlers is one day.
	defaultJWTExpiry time.Duration = time.Hour * 24

	// Inter-node JWT token expiry is 100 years.
	defaultInterNodeJWTExpiry time.Duration = time.Hour * 24 * 365 * 100
)

// newJWT - returns new JWT object.
func newJWT(expiry time.Duration, cred credential) (*JWT, error) {
	if !isValidAccessKey(cred.AccessKeyID) {
		return nil, errInvalidAccessKeyLength
	}
	if !isValidSecretKey(cred.SecretAccessKey) {
		return nil, errInvalidSecretKeyLength
	}
	return &JWT{cred, expiry}, nil
}

var errInvalidAccessKeyLength = errors.New("Invalid access key, access key should be 5 to 20 characters in length")
var errInvalidSecretKeyLength = errors.New("Invalid secret key, secret key should be 8 to 40 characters in length")

// GenerateToken - generates a new Json Web Token based on the incoming access key.
func (jwt *JWT) GenerateToken(accessKey string) (string, error) {
	// Trim spaces.
	accessKey = strings.TrimSpace(accessKey)

	if !isValidAccessKey(accessKey) {
		return "", errInvalidAccessKeyLength
	}

	tUTCNow := time.Now().UTC()
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.MapClaims{
		// Token expires in 10hrs.
		"exp": tUTCNow.Add(jwt.expiry).Unix(),
		"iat": tUTCNow.Unix(),
		"sub": accessKey,
	})
	return token.SignedString([]byte(jwt.SecretAccessKey))
}

var errInvalidAccessKeyID = errors.New("The access key ID you provided does not exist in our records")
var errAuthentication = errors.New("Authentication failed, check your access credentials")

// Authenticate - authenticates incoming access key and secret key.
func (jwt *JWT) Authenticate(accessKey, secretKey string) error {
	// Trim spaces.
	accessKey = strings.TrimSpace(accessKey)

	if !isValidAccessKey(accessKey) {
		return errInvalidAccessKeyLength
	}
	if !isValidSecretKey(secretKey) {
		return errInvalidSecretKeyLength
	}

	if accessKey != jwt.AccessKeyID {
		return errInvalidAccessKeyID
	}

	hashedSecretKey, _ := bcrypt.GenerateFromPassword([]byte(jwt.SecretAccessKey), bcrypt.DefaultCost)
	if bcrypt.CompareHashAndPassword(hashedSecretKey, []byte(secretKey)) != nil {
		return errAuthentication
	}

	// Success.
	return nil
}
