/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	jwtreq "github.com/dgrijalva/jwt-go/request"
	"github.com/minio/minio/pkg/auth"
)

const (
	jwtAlgorithm = "Bearer"

	// Default JWT token for web handlers is one day.
	defaultJWTExpiry = 24 * time.Hour

	// Inter-node JWT token expiry is 100 years approx.
	defaultInterNodeJWTExpiry = 100 * 365 * 24 * time.Hour

	// URL JWT token expiry is one minute (might be exposed).
	defaultURLJWTExpiry = time.Minute
)

var (
	errInvalidAccessKeyID   = errors.New("The access key ID you provided does not exist in our records")
	errChangeCredNotAllowed = errors.New("Changing access key and secret key not allowed")
	errAuthentication       = errors.New("Authentication failed, check your access credentials")
	errNoAuthToken          = errors.New("JWT token missing")
)

func authenticateJWT(accessKey, secretKey string, expiry time.Duration) (string, error) {
	passedCredential, err := auth.CreateCredentials(accessKey, secretKey)
	if err != nil {
		return "", err
	}

	// Access credentials.
	serverCred, errCode := globalServerConfig.GetCredentialByKey(passedCredential.AccessKey)
	if errCode != ErrNone {
		return "", errInvalidAccessKeyID
	}
	if serverCred.AccessKey != passedCredential.AccessKey {
		return "", errInvalidAccessKeyID
	}

	if !serverCred.Equal(passedCredential) {
		return "", errAuthentication
	}

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.StandardClaims{
		ExpiresAt: UTCNow().Add(expiry).Unix(),
		Subject:   accessKey,
	})
	return jwt.SignedString([]byte(serverCred.SecretKey))
}

func authenticateNode(accessKey, secretKey string) (string, error) {
	return authenticateJWT(accessKey, secretKey, defaultInterNodeJWTExpiry)
}

func authenticateWeb(accessKey, secretKey string) (string, error) {
	return authenticateJWT(accessKey, secretKey, defaultJWTExpiry)
}

func authenticateURL(accessKey, secretKey string) (string, error) {
	return authenticateJWT(accessKey, secretKey, defaultURLJWTExpiry)
}

func keyFuncCallback(jwtToken *jwtgo.Token) (interface{}, error) {
	if _, ok := jwtToken.Method.(*jwtgo.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("Unexpected signing method: %v", jwtToken.Header["alg"])
	}
	if claims, ok := jwtToken.Claims.(*jwtgo.StandardClaims); ok {
		accessKey := claims.Subject
		cred, err := globalServerConfig.GetCredentialByKey(accessKey)
		if err != ErrNone {
			return nil, errors.New(getAPIError(err).Description)
		}
		if cred.AccessKey != accessKey {
			return nil, errInvalidAccessKeyID
		}
		return []byte(cred.SecretKey), nil
	}

	return nil, errors.New("Unknown token format")
}

func isAuthTokenValid(tokenString string) bool {
	if tokenString == "" {
		return false
	}
	_, err := tokenAuthenticate(tokenString)
	return err == nil
}

func isHTTPRequestValid(req *http.Request) bool {
	_, err := webRequestAuthenticate(req)
	return err == nil
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request) (cred CustomCredentials, err error) {
	var claims jwtgo.StandardClaims
	_, err = jwtreq.ParseFromRequestWithClaims(req, jwtreq.AuthorizationHeaderExtractor, &claims, keyFuncCallback)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			err = errNoAuthToken
			return
		}
		err = errAuthentication
		return
	}
	cred, errCode := globalServerConfig.GetCredentialByKey(claims.Subject)
	if errCode != ErrNone {
		err = errors.New(getAPIError(errCode).Description)
		return
	}
	return cred, nil
}

func tokenAuthenticate(tokenString string) (cred CustomCredentials, err error) {
	var claims jwtgo.StandardClaims
	_, err = jwtgo.ParseWithClaims(tokenString, &claims, keyFuncCallback)
	if err != nil {
		return
	}
	cred, errCode := globalServerConfig.GetCredentialByKey(claims.Subject)
	if errCode != ErrNone {
		err = errors.New(getAPIError(errCode).Description)
		return
	}
	return cred, nil
}
