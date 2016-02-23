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
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/s3/signature4"
)

const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	jwtAlgorithm    = "Bearer"
)

// Verify if request has JWT.
func isRequestJWT(r *http.Request) bool {
	if _, ok := r.Header["Authorization"]; ok {
		if strings.HasPrefix(r.Header.Get("Authorization"), jwtAlgorithm) {
			return true
		}
	}
	return false
}

// Verify if request has AWS Signature Version '4'.
func isRequestSignatureV4(r *http.Request) bool {
	if _, ok := r.Header["Authorization"]; ok {
		if strings.HasPrefix(r.Header.Get("Authorization"), signV4Algorithm) {
			return true
		}
	}
	return false
}

// Verify if request has AWS Presignature Version '4'.
func isRequestPresignedSignatureV4(r *http.Request) bool {
	if _, ok := r.URL.Query()["X-Amz-Credential"]; ok {
		return true
	}
	return false
}

// Verify if request has AWS Post policy Signature Version '4'.
func isRequestPostPolicySignatureV4(r *http.Request) bool {
	if _, ok := r.Header["Content-Type"]; ok {
		if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
			return true
		}
	}
	return false
}

// Verify if request requires ACL check.
func isRequestRequiresACLCheck(r *http.Request) bool {
	if isRequestSignatureV4(r) || isRequestPresignedSignatureV4(r) || isRequestPostPolicySignatureV4(r) {
		return false
	}
	return true
}

// Verify if request has valid AWS Signature Version '4'.
func isSignV4ReqAuthenticated(sign *signature4.Sign, r *http.Request) bool {
	auth := sign.SetHTTPRequestToVerify(r)
	if isRequestSignatureV4(r) {
		dummyPayload := sha256.Sum256([]byte(""))
		ok, err := auth.DoesSignatureMatch(hex.EncodeToString(dummyPayload[:]))
		if err != nil {
			errorIf(err.Trace(), "Signature verification failed.", nil)
			return false
		}
		return ok
	} else if isRequestPresignedSignatureV4(r) {
		ok, err := auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(), "Presigned signature verification failed.", nil)
			return false
		}
		return ok
	}
	return false
}

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
		// Validate Authorization header if its valid.
		if !isJWTReqAuthenticated(r) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	// For all other signed requests, let top level caller verify.
	a.handler.ServeHTTP(w, r)
}
