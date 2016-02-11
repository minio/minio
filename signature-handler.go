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
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/probe"
	v4 "github.com/minio/minio/pkg/signature"
)

type signatureHandler struct {
	handler http.Handler
}

// setSignatureHandler to validate authorization header for the incoming request.
func setSignatureHandler(h http.Handler) http.Handler {
	return signatureHandler{h}
}

func isRequestSignatureV4(req *http.Request) bool {
	if _, ok := req.Header["Authorization"]; ok {
		if strings.HasPrefix(req.Header.Get("Authorization"), authHeaderPrefix) {
			return ok
		}
	}
	return false
}

func isRequestRequiresACLCheck(req *http.Request) bool {
	if isRequestSignatureV4(req) || isRequestPresignedSignatureV4(req) || isRequestPostPolicySignatureV4(req) {
		return false
	}
	return true
}

func isRequestPresignedSignatureV4(req *http.Request) bool {
	if _, ok := req.URL.Query()["X-Amz-Credential"]; ok {
		return ok
	}
	return false
}

func isRequestPostPolicySignatureV4(req *http.Request) bool {
	if _, ok := req.Header["Content-Type"]; ok {
		if strings.Contains(req.Header.Get("Content-Type"), "multipart/form-data") {
			return true
		}
	}
	return false
}

func (s signatureHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isRequestPostPolicySignatureV4(r) && r.Method == "POST" {
		s.handler.ServeHTTP(w, r)
		return
	}
	var signature *v4.Signature
	if isRequestSignatureV4(r) {
		// For PUT and POST requests with payload, send the call upwards for verification.
		// Or PUT and POST requests without payload, verify here.
		if (r.Body == nil && (r.Method == "PUT" || r.Method == "POST")) || (r.Method != "PUT" && r.Method != "POST") {
			// Init signature V4 verification
			var err *probe.Error
			signature, err = initSignatureV4(r)
			if err != nil {
				switch err.ToGoError() {
				case errInvalidRegion:
					errorIf(err.Trace(), "Unknown region in authorization header.", nil)
					writeErrorResponse(w, r, AuthorizationHeaderMalformed, r.URL.Path)
					return
				case errAccessKeyIDInvalid:
					errorIf(err.Trace(), "Invalid access key id.", nil)
					writeErrorResponse(w, r, InvalidAccessKeyID, r.URL.Path)
					return
				default:
					errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
					writeErrorResponse(w, r, InternalError, r.URL.Path)
					return
				}
			}
			dummySha256Bytes := sha256.Sum256([]byte(""))
			ok, err := signature.DoesSignatureMatch(hex.EncodeToString(dummySha256Bytes[:]))
			if err != nil {
				errorIf(err.Trace(), "Unable to verify signature.", nil)
				writeErrorResponse(w, r, InternalError, r.URL.Path)
				return
			}
			if !ok {
				writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
				return
			}
		}
		s.handler.ServeHTTP(w, r)
		return
	}
	if isRequestPresignedSignatureV4(r) {
		var err *probe.Error
		signature, err = initPresignedSignatureV4(r)
		if err != nil {
			switch err.ToGoError() {
			case errAccessKeyIDInvalid:
				errorIf(err.Trace(), "Invalid access key id requested.", nil)
				writeErrorResponse(w, r, InvalidAccessKeyID, r.URL.Path)
				return
			default:
				errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
				writeErrorResponse(w, r, InternalError, r.URL.Path)
				return
			}
		}
		ok, err := signature.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(), "Unable to verify signature.", nil)
			writeErrorResponse(w, r, InternalError, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		s.handler.ServeHTTP(w, r)
		return
	}
	// call goes up from here, let ACL's verify the validity of the request
	s.handler.ServeHTTP(w, r)
}
