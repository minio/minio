// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"net/http"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	jwtreq "github.com/golang-jwt/jwt/v4/request"
	lru "github.com/hashicorp/golang-lru"
	"github.com/minio/minio/internal/auth"
	xjwt "github.com/minio/minio/internal/jwt"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
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
	errInvalidAccessKeyID = errors.New("The access key ID you provided does not exist in our records")
	errAuthentication     = errors.New("Authentication failed, check your access credentials")
	errNoAuthToken        = errors.New("JWT token missing")
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
		u, ok := globalIAMSys.GetUser(context.TODO(), credentials.AccessKey)
		if !ok {
			return "", errInvalidAccessKeyID
		}
		serverCred = u.Credentials
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

// cachedAuthenticateNode will cache authenticateNode results for given values up to ttl.
func cachedAuthenticateNode(ttl time.Duration) func(accessKey, secretKey, audience string) (string, error) {
	type key struct {
		accessKey, secretKey, audience string
	}
	type value struct {
		created time.Time
		res     string
		err     error
	}
	cache, err := lru.NewARC(100)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return authenticateNode
	}
	return func(accessKey, secretKey, audience string) (string, error) {
		k := key{accessKey: accessKey, secretKey: secretKey, audience: audience}
		v, ok := cache.Get(k)
		if ok {
			if val, ok := v.(*value); ok && time.Since(val.created) < ttl {
				return val.res, val.err
			}
		}
		s, err := authenticateNode(accessKey, secretKey, audience)
		cache.Add(k, &value{created: time.Now(), res: s, err: err})
		return s, err
	}
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

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func metricsRequestAuthenticate(req *http.Request) (*xjwt.MapClaims, []string, bool, error) {
	token, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(req)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return nil, nil, false, errNoAuthToken
		}
		return nil, nil, false, err
	}
	claims := xjwt.NewMapClaims()
	if err := xjwt.ParseWithClaims(token, claims, func(claims *xjwt.MapClaims) ([]byte, error) {
		if claims.AccessKey == globalActiveCred.AccessKey {
			return []byte(globalActiveCred.SecretKey), nil
		}
		u, ok := globalIAMSys.GetUser(req.Context(), claims.AccessKey)
		if !ok {
			return nil, errInvalidAccessKeyID
		}
		cred := u.Credentials
		return []byte(cred.SecretKey), nil
	}); err != nil {
		return claims, nil, false, errAuthentication
	}
	owner := true
	var groups []string
	if globalActiveCred.AccessKey != claims.AccessKey {
		// Check if the access key is part of users credentials.
		u, ok := globalIAMSys.GetUser(req.Context(), claims.AccessKey)
		if !ok {
			return nil, nil, false, errInvalidAccessKeyID
		}
		ucred := u.Credentials
		// get embedded claims
		eclaims, s3Err := checkClaimsFromToken(req, ucred)
		if s3Err != ErrNone {
			return nil, nil, false, errAuthentication
		}

		for k, v := range eclaims {
			claims.MapClaims[k] = v
		}

		// Now check if we have a sessionPolicy.
		if _, ok = eclaims[iampolicy.SessionPolicyName]; ok {
			owner = false
		} else {
			owner = globalActiveCred.AccessKey == ucred.ParentUser
		}

		groups = ucred.Groups
	}

	return claims, groups, owner, nil
}

// newCachedAuthToken returns a token that is cached up to 15 seconds.
// If globalActiveCred is updated it is reflected at once.
func newCachedAuthToken() func(audience string) string {
	fn := cachedAuthenticateNode(15 * time.Second)
	return func(audience string) string {
		cred := globalActiveCred
		token, err := fn(cred.AccessKey, cred.SecretKey, audience)
		logger.CriticalIf(GlobalContext, err)
		return token
	}
}
