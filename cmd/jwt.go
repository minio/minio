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
	"errors"
	"maps"
	"net/http"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	jwtreq "github.com/golang-jwt/jwt/v4/request"
	"github.com/minio/minio/internal/auth"
	xjwt "github.com/minio/minio/internal/jwt"
	"github.com/minio/pkg/v3/policy"
)

const (
	jwtAlgorithm = "Bearer"

	// Default JWT token for web handlers is one day.
	defaultJWTExpiry = 24 * time.Hour

	// Inter-node JWT token expiry is 100 years approx.
	defaultInterNodeJWTExpiry = 100 * 365 * 24 * time.Hour
)

var (
	errInvalidAccessKeyID = errors.New("The access key ID you provided does not exist in our records")
	errAccessKeyDisabled  = errors.New("The access key you provided is disabled")
	errAuthentication     = errors.New("Authentication failed, check your access credentials")
	errNoAuthToken        = errors.New("JWT token missing")
	errSkewedAuthTime     = errors.New("Skewed authentication date/time")
	errMalformedAuth      = errors.New("Malformed authentication input")
)

func authenticateNode(accessKey, secretKey string) (string, error) {
	claims := xjwt.NewStandardClaims()
	claims.SetExpiry(UTCNow().Add(defaultInterNodeJWTExpiry))
	claims.SetAccessKey(accessKey)

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, claims)
	return jwt.SignedString([]byte(secretKey))
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
		if claims.AccessKey != globalActiveCred.AccessKey {
			u, ok := globalIAMSys.GetUser(req.Context(), claims.AccessKey)
			if !ok {
				// Credentials will be invalid but for disabled
				// return a different error in such a scenario.
				if u.Credentials.Status == auth.AccountOff {
					return nil, errAccessKeyDisabled
				}
				return nil, errInvalidAccessKeyID
			}
			cred := u.Credentials
			// Expired credentials return error.
			if cred.IsTemp() && cred.IsExpired() {
				return nil, errInvalidAccessKeyID
			}
			return []byte(cred.SecretKey), nil
		} // this means claims.AccessKey == rootAccessKey
		if !globalAPIConfig.permitRootAccess() {
			// if root access is disabled, fail this request.
			return nil, errAccessKeyDisabled
		}
		return []byte(globalActiveCred.SecretKey), nil
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

		maps.Copy(claims.MapClaims, eclaims)

		// if root access is disabled, disable all its service accounts and temporary credentials.
		if ucred.ParentUser == globalActiveCred.AccessKey && !globalAPIConfig.permitRootAccess() {
			return nil, nil, false, errAccessKeyDisabled
		}

		// Now check if we have a sessionPolicy.
		if _, ok = eclaims[policy.SessionPolicyName]; ok {
			owner = false
		} else {
			owner = globalActiveCred.AccessKey == ucred.ParentUser
		}

		groups = ucred.Groups
	}

	return claims, groups, owner, nil
}

// newCachedAuthToken returns the cached token.
func newCachedAuthToken() func() string {
	return func() string {
		return globalNodeAuthToken
	}
}
