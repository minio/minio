/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import (
	"fmt"
	"net/http"

	jwtgo "github.com/dgrijalva/jwt-go"
)

const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	jwtAlgorithm    = "Bearer"
)

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type authHandler struct {
	handler http.Handler
}

// setAuthHandler to validate authorization header for the incoming request.
func setAuthHandler(h http.Handler) http.Handler {
	return authHandler{h}
}

func (a authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Verify if request has post policy signature.
	if isRequestPostPolicySignatureV4(r) && r.Method == "POST" {
		a.handler.ServeHTTP(w, r)
		return
	}

	// Verify JWT authorization header is present.
	if isRequestJWT(r) {
		// Validate Authorization header to be valid.
		jwt := InitJWT()
		token, e := jwtgo.ParseFromRequest(r, func(token *jwtgo.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
			}
			return jwt.secretAccessKey, nil
		})
		if e != nil || !token.Valid {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	// For signed, presigned, jwt and anonymous requests let the top level
	// caller handle and verify.
	a.handler.ServeHTTP(w, r)
}
