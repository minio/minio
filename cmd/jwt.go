/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"net/http"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	jwtreq "github.com/dgrijalva/jwt-go/request"
	xjwt "github.com/minio/minio/cmd/jwt"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
)

const (
	jwtAlgorithm = "Bearer"

	// Default JWT token for web handlers is one day.
	defaultJWTExpiry = 24 * time.Hour

	// Inter-node JWT token expiry is 15 minutes.
	defaultInterNodeJWTExpiry = 15 * time.Minute

	// URL JWT token expiry is one minute (might be exposed).
	defaultURLJWTExpiry = time.Minute
)

var (
	errInvalidAccessKeyID   = errors.New("The access key ID you provided does not exist in our records")
	errChangeCredNotAllowed = errors.New("Changing access key and secret key not allowed")
	errAuthentication       = errors.New("Authentication failed, check your access credentials")
	errNoAuthToken          = errors.New("JWT token missing")
	errIncorrectCreds       = errors.New("Current access key or secret key is incorrect")
	errPresignedNotAllowed  = errors.New("Unable to generate shareable URL due to lack of read permissions")
)

func authenticateJWTUsers(accessKey, secretKey string, expiry time.Duration) (string, error) {
	passedCredential, err := auth.CreateCredentials(accessKey, secretKey)
	if err != nil {
		return "", err
	}
	expiresAt := UTCNow().Add(expiry)
	return authenticateJWTUsersWithCredentials(passedCredential, expiresAt)
}

func authenticateJWTUsersWithCredentials(credentials auth.Credentials, expiresAt time.Time) (string, error) {
	serverCred := globalActiveCred
	if serverCred.AccessKey != credentials.AccessKey {
		var ok bool
		serverCred, ok = globalIAMSys.GetUser(credentials.AccessKey)
		if !ok {
			return "", errInvalidAccessKeyID
		}
	}

	if !serverCred.Equal(credentials) {
		return "", errAuthentication
	}

	claims := xjwt.NewMapClaims()
	claims.SetExpiry(expiresAt)
	claims.SetAccessKey(credentials.AccessKey)

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, claims)
	return jwt.SignedString([]byte(serverCred.SecretKey))
}

func authenticateNode(accessKey, secretKey, audience string) (string, error) {
	claims := xjwt.NewStandardClaims()
	claims.SetExpiry(UTCNow().Add(defaultInterNodeJWTExpiry))
	claims.SetAccessKey(accessKey)
	claims.SetAudience(audience)

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, claims)
	return jwt.SignedString([]byte(secretKey))
}

func authenticateWeb(accessKey, secretKey string) (string, error) {
	return authenticateJWTUsers(accessKey, secretKey, defaultJWTExpiry)
}

func authenticateURL(accessKey, secretKey string) (string, error) {
	return authenticateJWTUsers(accessKey, secretKey, defaultURLJWTExpiry)
}

// Callback function used for parsing
func webTokenCallback(claims *xjwt.MapClaims) ([]byte, error) {
	if claims.AccessKey == globalActiveCred.AccessKey {
		return []byte(globalActiveCred.SecretKey), nil
	}
	if globalIAMSys == nil {
		return nil, errInvalidAccessKeyID
	}
	ok, err := globalIAMSys.IsTempUser(claims.AccessKey)
	if err != nil {
		if err == errNoSuchUser {
			return nil, errInvalidAccessKeyID
		}
		return nil, err
	}
	if ok {
		return []byte(globalActiveCred.SecretKey), nil
	}
	cred, ok := globalIAMSys.GetUser(claims.AccessKey)
	if !ok {
		return nil, errInvalidAccessKeyID
	}
	return []byte(cred.SecretKey), nil

}

func isAuthTokenValid(token string) bool {
	_, _, err := webTokenAuthenticate(token)
	return err == nil
}

func webTokenAuthenticate(token string) (*xjwt.MapClaims, bool, error) {
	if token == "" {
		return nil, false, errNoAuthToken
	}
	claims := xjwt.NewMapClaims()
	if err := xjwt.ParseWithClaims(token, claims, webTokenCallback); err != nil {
		return claims, false, errAuthentication
	}
	owner := claims.AccessKey == globalActiveCred.AccessKey
	return claims, owner, nil
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request) (*xjwt.MapClaims, bool, error) {
	token, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(req)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return nil, false, errNoAuthToken
		}
		return nil, false, err
	}
	claims := xjwt.NewMapClaims()
	if err := xjwt.ParseWithClaims(token, claims, webTokenCallback); err != nil {
		return claims, false, errAuthentication
	}
	owner := claims.AccessKey == globalActiveCred.AccessKey
	return claims, owner, nil
}

func newAuthToken(audience string) string {
	cred := globalActiveCred
	token, err := authenticateNode(cred.AccessKey, cred.SecretKey, audience)
	logger.CriticalIf(GlobalContext, err)
	return token
}
