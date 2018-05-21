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
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	jwtreq "github.com/dgrijalva/jwt-go/request"
	"github.com/minio/minio/cmd/logger"
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

	// Find a valid set of credentials for the passed in key.
	serverCred := globalServerConfig.GetCredentialForKey(accessKey)

	if serverCred.AccessKey != passedCredential.AccessKey {
		return "", errInvalidAccessKeyID
	}

	if !serverCred.IsValid() || !serverCred.Equal(passedCredential) {
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

	subject := jwtToken.Claims.(*jwtgo.StandardClaims).Subject
	creds := globalServerConfig.GetCredentialForKey(subject)
	if !creds.IsValid() {
		return nil, fmt.Errorf("Invalid key %v", subject)
	}

	return []byte(creds.SecretKey), nil
}

// Allows for a valid token on any bucket.
func isAuthTokenValid(tokenString string) bool {
	if tokenString == "" {
		return false
	}
	var claims jwtgo.StandardClaims
	jwtToken, err := jwtgo.ParseWithClaims(tokenString, &claims, keyFuncCallback)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return false
	}
	if err = claims.Valid(); err != nil {
		logger.LogIf(context.Background(), err)
		return false
	}

	if !jwtToken.Valid {
		return false
	}

	serverCred := globalServerConfig.GetCredentialForKey(claims.Subject)

	return serverCred.IsValid() && claims.Subject == serverCred.AccessKey
}

func isHTTPRequestValid(req *http.Request, bucket string) bool {
	return webRequestAuthenticate(req, bucket) == nil
}

func isHTTPRequestValidAnyKey(req *http.Request) bool {
	_, err := webRequestAuthenticateAnyKey(req)
	return err == nil
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticateAnyKey(req *http.Request) (string, error) {
	var claims jwtgo.StandardClaims
	jwtToken, err := jwtreq.ParseFromRequestWithClaims(req, jwtreq.AuthorizationHeaderExtractor, &claims, keyFuncCallback)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return "", errNoAuthToken
		}
		return "", errAuthentication
	}
	if err = claims.Valid(); err != nil {
		return "", errAuthentication
	}

	if !jwtToken.Valid {
		return "", errAuthentication
	}

	serverCreds := globalServerConfig.GetCredentialForKey(claims.Subject)
	if serverCreds.IsValid() && serverCreds.AccessKey == claims.Subject {
		return claims.Subject, nil
	}

	return "", errInvalidAccessKeyID
}

// Check if the request is authenticated.
// Returns an error and the access key that matched.
// Error returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request, bucket string) error {
	var claims jwtgo.StandardClaims
	jwtToken, err := jwtreq.ParseFromRequestWithClaims(req, jwtreq.AuthorizationHeaderExtractor, &claims, keyFuncCallback)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return errNoAuthToken
		}
		return errAuthentication
	}
	if err = claims.Valid(); err != nil {
		return errAuthentication
	}

	if !jwtToken.Valid {
		return errAuthentication
	}

	// Check the master key first.
	if claims.Subject != globalServerConfig.GetCredential().AccessKey {
		// If this token isn't for the master key, check the bucket key.
		bucketCred := globalServerConfig.GetCredentialForBucket(bucket)
		if !bucketCred.IsValid() || claims.Subject != bucketCred.AccessKey {
			return errInvalidAccessKeyID
		}
	}
	return nil
}
