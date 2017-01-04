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
	"fmt"
	"net/http"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	jwtreq "github.com/dgrijalva/jwt-go/request"
	"golang.org/x/crypto/bcrypt"
)

const (
	jwtAlgorithm = "Bearer"

	// Default JWT token for web handlers is one day.
	defaultJWTExpiry = 24 * time.Hour

	// Inter-node JWT token expiry is 100 years approx.
	defaultInterNodeJWTExpiry = 100 * 365 * 24 * time.Hour
)

var errInvalidAccessKeyLength = errors.New("Invalid access key, access key should be 5 to 20 characters in length")
var errInvalidSecretKeyLength = errors.New("Invalid secret key, secret key should be 8 to 40 characters in length")

var errInvalidAccessKeyID = errors.New("The access key ID you provided does not exist in our records")
var errAuthentication = errors.New("Authentication failed, check your access credentials")
var errNoAuthToken = errors.New("JWT token missing")

func authenticateJWT(accessKey, secretKey string, expiry time.Duration) (string, error) {
	// Trim spaces.
	accessKey = strings.TrimSpace(accessKey)

	if !isAccessKeyValid(accessKey) {
		return "", errInvalidAccessKeyLength
	}
	if !isSecretKeyValid(secretKey) {
		return "", errInvalidSecretKeyLength
	}

	serverCred := serverConfig.GetCredential()

	// Validate access key.
	if accessKey != serverCred.AccessKey {
		return "", errInvalidAccessKeyID
	}

	// Validate secret key.
	// Using bcrypt to avoid timing attacks.
	if bcrypt.CompareHashAndPassword(serverCred.SecretKeyHash, []byte(secretKey)) != nil {
		return "", errAuthentication
	}

	utcNow := time.Now().UTC()
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.MapClaims{
		"exp": utcNow.Add(expiry).Unix(),
		"iat": utcNow.Unix(),
		"sub": accessKey,
	})

	return token.SignedString([]byte(serverCred.SecretKey))
}

func authenticateNode(accessKey, secretKey string) (string, error) {
	return authenticateJWT(accessKey, secretKey, defaultInterNodeJWTExpiry)
}

func authenticateWeb(accessKey, secretKey string) (string, error) {
	return authenticateJWT(accessKey, secretKey, defaultJWTExpiry)
}

func keyFuncCallback(jwtToken *jwtgo.Token) (interface{}, error) {
	if _, ok := jwtToken.Method.(*jwtgo.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("Unexpected signing method: %v", jwtToken.Header["alg"])
	}

	return []byte(serverConfig.GetCredential().SecretKey), nil
}

func isAuthTokenValid(tokenString string) bool {
	jwtToken, err := jwtgo.Parse(tokenString, keyFuncCallback)
	if err != nil {
		errorIf(err, "Unable to parse JWT token string")
		return false
	}

	return jwtToken.Valid
}

func isHTTPRequestValid(req *http.Request) bool {
	return webReqestAuthenticate(req) == nil
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webReqestAuthenticate(req *http.Request) error {
	jwtToken, err := jwtreq.ParseFromRequest(req, jwtreq.AuthorizationHeaderExtractor, keyFuncCallback)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return errNoAuthToken
		}
		return errAuthentication
	}

	if !jwtToken.Valid {
		return errAuthentication
	}
	return nil
}
