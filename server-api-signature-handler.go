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
	"net/http"

	"github.com/minio/minio/pkg/probe"
	signv4 "github.com/minio/minio/pkg/signature"
)

type signatureHandler struct {
	handler http.Handler
}

// SignatureHandler to validate authorization header for the incoming request.
func SignatureHandler(h http.Handler) http.Handler {
	return signatureHandler{h}
}

func isRequestSignatureV4(req *http.Request) bool {
	if _, ok := req.Header["Authorization"]; ok {
		return ok
	}
	return false
}

func isRequestPresignedSignatureV4(req *http.Request) bool {
	if _, ok := req.URL.Query()["X-Amz-Credential"]; ok {
		return ok
	}
	return false
}

func (s signatureHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var signature *signv4.Signature
	if isRequestSignatureV4(r) {
		// If the request is not a PUT method handle the verification here.
		// For PUT and POST requests with payload, send the call upwards for verification
		if r.Method != "PUT" && r.Method != "POST" {
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
			ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
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
	}
	writeErrorResponse(w, r, AccessDenied, r.URL.Path)
}
