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
	"fmt"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/bcrypt"
)

const jwtAlgorithm = "Bearer"

// JWT - jwt auth backend
type JWT struct {
	credential
}

// Default - each token expires in 10hrs.
const (
	tokenExpires time.Duration = 10
)

// newJWT - returns new JWT object.
func newJWT() (*JWT, error) {
	if serverConfig == nil {
		return nil, fmt.Errorf("server not initialzed")
	}

	// Save access, secret keys.
	cred := serverConfig.GetCredential()
	if !isValidAccessKey.MatchString(cred.AccessKeyID) {
		return nil, fmt.Errorf("Invalid access key")
	}
	if !isValidSecretKey.MatchString(cred.SecretAccessKey) {
		return nil, fmt.Errorf("Invalid secret key")
	}

	return &JWT{cred}, nil
}

// GenerateToken - generates a new Json Web Token based on the incoming access key.
func (jwt *JWT) GenerateToken(accessKey string) (string, error) {
	// Trim spaces.
	accessKey = strings.TrimSpace(accessKey)

	if !isValidAccessKey.MatchString(accessKey) {
		return "", fmt.Errorf("Invalid access key")
	}

	tUTCNow := time.Now().UTC()
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.MapClaims{
		// Token expires in 10hrs.
		"exp": tUTCNow.Add(time.Hour * tokenExpires).Unix(),
		"iat": tUTCNow.Unix(),
		"sub": accessKey,
	})
	return token.SignedString([]byte(jwt.SecretAccessKey))
}

// Authenticate - authenticates incoming access key and secret key.
func (jwt *JWT) Authenticate(accessKey, secretKey string) error {
	// Trim spaces.
	accessKey = strings.TrimSpace(accessKey)

	if !isValidAccessKey.MatchString(accessKey) {
		return fmt.Errorf("Invalid access key")
	}
	if !isValidSecretKey.MatchString(secretKey) {
		return fmt.Errorf("Invalid secret key")
	}

	if accessKey != jwt.AccessKeyID {
		return fmt.Errorf("Access key does not match")
	}

	hashedSecretKey, _ := bcrypt.GenerateFromPassword([]byte(jwt.SecretAccessKey), bcrypt.DefaultCost)

	if bcrypt.CompareHashAndPassword(hashedSecretKey, []byte(secretKey)) != nil {
		return fmt.Errorf("Authentication failed")
	}

	// Success.
	return nil
}
