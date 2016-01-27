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

package main

import (
	"fmt"
	"net/http"

	jwtgo "github.com/dgrijalva/jwt-go"
)

type authHandler struct {
	handler http.Handler
}

// AuthHandler -
// Verify if authorization header is of form JWT, reject it otherwise.
func AuthHandler(h http.Handler) http.Handler {
	return authHandler{h}
}

// Ignore request if authorization header is not valid.
func (h authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Let the top level caller handle if the requests should be
	// allowed, if there are no Authorization headers.
	if r.Header.Get("Authorization") == "" {
		h.handler.ServeHTTP(w, r)
		return
	}
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
	h.handler.ServeHTTP(w, r)
}
