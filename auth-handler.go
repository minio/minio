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

// handler for validating incoming authorization headers.
func (a authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Verify if request is presigned, validate signature inside each handlers.
	if isRequestPresignedSignatureV4(r) {
		a.handler.ServeHTTP(w, r)
		return
	}

	// Verify if request has post policy signature, validate signature
	// inside POST policy handler.
	if isRequestPostPolicySignatureV4(r) && r.Method == "POST" {
		a.handler.ServeHTTP(w, r)
		return
	}

	// No authorization found, let the top level caller validate if
	// public request is allowed.
	if _, ok := r.Header["Authorization"]; !ok {
		a.handler.ServeHTTP(w, r)
		return
	}

	// Verify if the signature algorithms are known.
	if !isRequestSignatureV4(r) && !isRequestJWT(r) {
		writeErrorResponse(w, r, SignatureVersionNotSupported, r.URL.Path)
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

	// For all other signed requests, let top level caller verify.
	a.handler.ServeHTTP(w, r)
}
