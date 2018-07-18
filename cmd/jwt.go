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

	serverCred := globalServerConfig.GetCredential()
	if !serverCred.EqualAccessKey(accessKey) {
		ui, ok := globalUsersStore.Get(accessKey)
		if !ok {
			return "", errInvalidAccessKeyID
		}
		serverCred = ui.Credentials
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
	if err := jwtToken.Claims.Valid(); err != nil {
		return nil, errAuthentication
	}
	if claims, ok := jwtToken.Claims.(*jwtgo.StandardClaims); ok {
		if claims.Subject == globalServerConfig.GetCredential().AccessKey {
			return []byte(globalServerConfig.GetCredential().SecretKey), nil
		}
		ui, ok := globalUsersStore.Get(claims.Subject)
		if !ok {
			return nil, errInvalidAccessKeyID
		}
		return []byte(ui.Credentials.SecretKey), nil
	}
	return nil, errAuthentication
}

func parseJWTWithStandardClaims(tokenString string) (*jwtgo.Token, error) {
	var claims jwtgo.StandardClaims

	p := &jwtgo.Parser{
		SkipClaimsValidation: true,
	}
	jwtToken, err := p.ParseWithClaims(tokenString, &claims, keyFuncCallback)
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

func isAuthTokenValid(tokenString string) bool {
	if tokenString == "" {
		return false
	}

	jwtToken, err := parseJWTWithStandardClaims(tokenString)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return false
	}
	return jwtToken.Valid
}

func isHTTPRequestValid(req *http.Request) bool {
	return webRequestAuthenticate(req) == nil
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request) error {
	tokStr, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(req)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return errNoAuthToken
		}
		return err
	}

	jwtToken, err := parseJWTWithStandardClaims(tokStr)
	if err != nil {
		return err
	}
	if !jwtToken.Valid {
		return errAuthentication
	}
	return nil
}

func newAuthToken() string {
	cred := globalServerConfig.GetCredential()
	token, err := authenticateNode(cred.AccessKey, cred.SecretKey)
	logger.CriticalIf(context.Background(), err)
	return token
}
