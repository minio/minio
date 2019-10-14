/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	errIncorrectCreds       = errors.New("Current access key or secret key is incorrect")
)

func authenticateJWTUsers(accessKey, secretKey string, expiry time.Duration) (string, error) {
	passedCredential, err := auth.CreateCredentials(accessKey, secretKey)
	if err != nil {
		return "", err
	}

	serverCred := globalServerConfig.GetCredential()
	if serverCred.AccessKey != passedCredential.AccessKey {
		var ok bool
		serverCred, ok = globalIAMSys.GetUser(accessKey)
		if !ok {
			return "", errInvalidAccessKeyID
		}
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

func authenticateJWTAdmin(accessKey, secretKey string, expiry time.Duration) (string, error) {
	passedCredential, err := auth.CreateCredentials(accessKey, secretKey)
	if err != nil {
		return "", err
	}

	serverCred := globalServerConfig.GetCredential()

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
	return authenticateJWTAdmin(accessKey, secretKey, defaultInterNodeJWTExpiry)
}

func authenticateWeb(accessKey, secretKey string) (string, error) {
	return authenticateJWTUsers(accessKey, secretKey, defaultJWTExpiry)
}

func authenticateURL(accessKey, secretKey string) (string, error) {
	return authenticateJWTUsers(accessKey, secretKey, defaultURLJWTExpiry)
}

// Callback function used for parsing
func webTokenCallback(jwtToken *jwtgo.Token) (interface{}, error) {
	if _, ok := jwtToken.Method.(*jwtgo.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("Unexpected signing method: %v", jwtToken.Header["alg"])
	}

	if err := jwtToken.Claims.Valid(); err != nil {
		return nil, errAuthentication
	}

	if claims, ok := jwtToken.Claims.(*jwtgo.StandardClaims); ok {
		if claims.Subject == globalServerConfig.GetCredential().AccessKey {
			return []byte(globalServerConfig.GetCredential().SecretKey), nil
		}
		if globalIAMSys == nil {
			return nil, errInvalidAccessKeyID
		}
		cred, ok := globalIAMSys.GetUser(claims.Subject)
		if !ok {
			return nil, errInvalidAccessKeyID
		}
		return []byte(cred.SecretKey), nil
	}

	return nil, errAuthentication
}

func parseJWTWithClaims(tokenString string, claims jwtgo.Claims) (*jwtgo.Token, error) {
	p := &jwtgo.Parser{
		SkipClaimsValidation: true,
	}
	jwtToken, err := p.ParseWithClaims(tokenString, claims, webTokenCallback)
	if err != nil {
		switch e := err.(type) {
		case *jwtgo.ValidationError:
			if e.Inner == nil {
				return nil, errAuthentication
			}
			return nil, e.Inner
		}
		return nil, errAuthentication
	}
	return jwtToken, nil
}

func isAuthTokenValid(token string) bool {
	_, _, err := webTokenAuthenticate(token)
	return err == nil
}

func webTokenAuthenticate(token string) (standardClaims, bool, error) {
	var claims = jwtgo.StandardClaims{}
	if token == "" {
		return standardClaims{claims}, false, errNoAuthToken
	}

	jwtToken, err := parseJWTWithClaims(token, &claims)
	if err != nil {
		return standardClaims{claims}, false, err
	}
	if !jwtToken.Valid {
		return standardClaims{claims}, false, errAuthentication
	}
	owner := claims.Subject == globalServerConfig.GetCredential().AccessKey
	return standardClaims{claims}, owner, nil
}

// jwt standardClaims
type standardClaims struct {
	jwtgo.StandardClaims
}

func (s standardClaims) Map() map[string]interface{} {
	m := make(map[string]interface{})
	m["sub"] = s.Subject
	m["iss"] = s.Issuer
	m["aud"] = s.Audience
	m["jti"] = s.Id
	return m
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request) (standardClaims, bool, error) {
	var claims = jwtgo.StandardClaims{}
	tokStr, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(req)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return standardClaims{claims}, false, errNoAuthToken
		}
		return standardClaims{claims}, false, err
	}
	jwtToken, err := parseJWTWithClaims(tokStr, &claims)
	if err != nil {
		return standardClaims{claims}, false, err
	}
	if !jwtToken.Valid {
		return standardClaims{claims}, false, errAuthentication
	}
	owner := claims.Subject == globalServerConfig.GetCredential().AccessKey
	return standardClaims{claims}, owner, nil
}

func newAuthToken() string {
	cred := globalServerConfig.GetCredential()
	token, err := authenticateNode(cred.AccessKey, cred.SecretKey)
	logger.CriticalIf(context.Background(), err)
	return token
}
