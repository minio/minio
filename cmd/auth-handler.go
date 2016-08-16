/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"strings"
)

// Verify if the request http Header "x-amz-content-sha256" == "UNSIGNED-PAYLOAD"
func isRequestUnsignedPayload(r *http.Request) bool {
	return r.Header.Get("x-amz-content-sha256") == unsignedPayload
}

// Verify if request has JWT.
func isRequestJWT(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), jwtAlgorithm)
}

// Verify if request has AWS Signature Version '4'.
func isRequestSignatureV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), signV4Algorithm)
}

// Verify if request has AWS PreSign Version '4'.
func isRequestPresignedSignatureV4(r *http.Request) bool {
	_, ok := r.URL.Query()["X-Amz-Credential"]
	return ok
}

// Verify if request has AWS Post policy Signature Version '4'.
func isRequestPostPolicySignatureV4(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") && r.Method == "POST"
}

// Verify if the request has AWS Streaming Signature Version '4'. This is only valid for 'PUT' operation.
func isRequestSignStreamingV4(r *http.Request) bool {
	return r.Header.Get("x-amz-content-sha256") == streamingContentSHA256 && r.Method == "PUT"
}

// Authorization type.
type authType int

// List of all supported auth types.
const (
	authTypeUnknown authType = iota
	authTypeAnonymous
	authTypePresigned
	authTypePostPolicy
	authTypeStreamingSigned
	authTypeSigned
	authTypeJWT
)

// Get request authentication type.
func getRequestAuthType(r *http.Request) authType {
	if isRequestSignStreamingV4(r) {
		return authTypeStreamingSigned
	} else if isRequestSignatureV4(r) {
		return authTypeSigned
	} else if isRequestPresignedSignatureV4(r) {
		return authTypePresigned
	} else if isRequestJWT(r) {
		return authTypeJWT
	} else if isRequestPostPolicySignatureV4(r) {
		return authTypePostPolicy
	} else if _, ok := r.Header["Authorization"]; !ok {
		return authTypeAnonymous
	}
	return authTypeUnknown
}

// sum256 calculate sha256 sum for an input byte array
func sum256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// sumMD5 calculate md5 sum for an input byte array
func sumMD5(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// Verify if request has valid AWS Signature Version '4'.
func isReqAuthenticated(r *http.Request) (s3Error APIErrorCode) {
	if r == nil {
		return ErrInternalError
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return ErrInternalError
	}
	// Verify Content-Md5, if payload is set.
	if r.Header.Get("Content-Md5") != "" {
		if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(sumMD5(payload)) {
			return ErrBadDigest
		}
	}
	// Populate back the payload.
	r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	validateRegion := true // Validate region.
	var sha256sum string
	// Skips calculating sha256 on the payload on server,
	// if client requested for it.
	if skipContentSha256Cksum(r) {
		sha256sum = unsignedPayload
	} else {
		sha256sum = hex.EncodeToString(sum256(payload))
	}
	if isRequestSignatureV4(r) {
		return doesSignatureMatch(sha256sum, r, validateRegion)
	} else if isRequestPresignedSignatureV4(r) {
		return doesPresignedSignatureMatch(sha256sum, r, validateRegion)
	}
	return ErrAccessDenied
}

// checkAuth - checks for conditions satisfying the authorization of
// the incoming request. Request should be either Presigned or Signed
// in accordance with AWS S3 Signature V4 requirements. ErrAccessDenied
// is returned for unhandled auth type. Once the auth type is indentified
// request headers and body are used to calculate the signature validating
// the client signature present in request.
func checkAuth(r *http.Request) APIErrorCode {
	aType := getRequestAuthType(r)
	if aType != authTypePresigned && aType != authTypeSigned {
		// For all unhandled auth types return error AccessDenied.
		return ErrAccessDenied
	}
	// Validates the request for both Presigned and Signed.
	return isReqAuthenticated(r)
}

// authHandler - handles all the incoming authorization headers and validates them if possible.
type authHandler struct {
	handler http.Handler
}

// setAuthHandler to validate authorization header for the incoming request.
func setAuthHandler(h http.Handler) http.Handler {
	return authHandler{h}
}

// List of all support S3 auth types.
var supportedS3AuthTypes = map[authType]struct{}{
	authTypeAnonymous:       {},
	authTypePresigned:       {},
	authTypeSigned:          {},
	authTypePostPolicy:      {},
	authTypeStreamingSigned: {},
}

// Validate if the authType is valid and supported.
func isSupportedS3AuthType(aType authType) bool {
	_, ok := supportedS3AuthTypes[aType]
	return ok
}

// handler for validating incoming authorization headers.
func (a authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	aType := getRequestAuthType(r)
	if isSupportedS3AuthType(aType) {
		// Let top level caller validate for anonymous and known signed requests.
		a.handler.ServeHTTP(w, r)
		return
	} else if aType == authTypeJWT {
		// Validate Authorization header if its valid for JWT request.
		if !isJWTReqAuthenticated(r) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		a.handler.ServeHTTP(w, r)
		return
	}
	writeErrorResponse(w, r, ErrSignatureVersionNotSupported, r.URL.Path)
}
