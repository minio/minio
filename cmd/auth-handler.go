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

// Verify if request has AWS Signature Version '2'.
func isRequestSignatureV2(r *http.Request) bool {
	return (!strings.HasPrefix(r.Header.Get("Authorization"), signV4Algorithm) &&
		strings.HasPrefix(r.Header.Get("Authorization"), signV2Algorithm))
}

// Verify if request has AWS PreSign Version '4'.
func isRequestPresignedSignatureV4(r *http.Request) bool {
	_, ok := r.URL.Query()["X-Amz-Credential"]
	return ok
}

// Verify request has AWS PreSign Version '2'.
func isRequestPresignedSignatureV2(r *http.Request) bool {
	_, ok := r.URL.Query()["AWSAccessKeyId"]
	return ok
}

// Verify if request has AWS Post policy Signature Version '4'.
func isRequestPostPolicySignatureV4(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") && r.Method == httpPOST
}

// Verify if the request has AWS Streaming Signature Version '4'. This is only valid for 'PUT' operation.
func isRequestSignStreamingV4(r *http.Request) bool {
	return r.Header.Get("x-amz-content-sha256") == streamingContentSHA256 &&
		r.Method == httpPUT
}

// Authorization type.
type authType int

// List of all supported auth types.
const (
	authTypeUnknown authType = iota
	authTypeAnonymous
	authTypePresigned
	authTypePresignedV2
	authTypePostPolicy
	authTypeStreamingSigned
	authTypeSigned
	authTypeSignedV2
	authTypeJWT
)

// Get request authentication type.
func getRequestAuthType(r *http.Request) authType {
	if isRequestSignatureV2(r) {
		return authTypeSignedV2
	} else if isRequestPresignedSignatureV2(r) {
		return authTypePresignedV2
	} else if isRequestSignStreamingV4(r) {
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

func checkRequestAuthType(r *http.Request, bucket, policyAction, region string) APIErrorCode {
	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypePresignedV2, authTypeSignedV2:
		// Signature V2 validation.
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
		}
		return s3Error
	case authTypeSigned, authTypePresigned:
		s3Error := isReqAuthenticated(r, region)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, "%s", dumpRequest(r))
		}
		return s3Error
	}

	if reqAuthType == authTypeAnonymous && policyAction != "" {
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		sourceIP := getSourceIPAddress(r)
		resource, err := getResource(r.URL.Path, r.Host, globalDomainName)
		if err != nil {
			return ErrInternalError
		}
		return enforceBucketPolicy(bucket, policyAction, resource,
			r.Referer(), sourceIP, r.URL.Query())
	}

	// By default return ErrAccessDenied
	return ErrAccessDenied
}

// Verify if request has valid AWS Signature Version '2'.
func isReqAuthenticatedV2(r *http.Request) (s3Error APIErrorCode) {
	if isRequestSignatureV2(r) {
		return doesSignV2Match(r)
	}
	return doesPresignV2SignatureMatch(r)
}

func reqSignatureV4Verify(r *http.Request, region string) (s3Error APIErrorCode) {
	sha256sum := getContentSha256Cksum(r)
	switch {
	case isRequestSignatureV4(r):
		return doesSignatureMatch(sha256sum, r, region)
	case isRequestPresignedSignatureV4(r):
		return doesPresignedSignatureMatch(sha256sum, r, region)
	default:
		return ErrAccessDenied
	}
}

// Verify if request has valid AWS Signature Version '4'.
func isReqAuthenticated(r *http.Request, region string) (s3Error APIErrorCode) {
	if r == nil {
		return ErrInternalError
	}
	if errCode := reqSignatureV4Verify(r, region); errCode != ErrNone {
		return errCode
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorIf(err, "Unable to read request body for signature verification")
		return ErrInternalError
	}

	// Populate back the payload.
	r.Body = ioutil.NopCloser(bytes.NewReader(payload))

	// Verify Content-Md5, if payload is set.
	if r.Header.Get("Content-Md5") != "" {
		if r.Header.Get("Content-Md5") != getMD5HashBase64(payload) {
			return ErrBadDigest
		}
	}

	if skipContentSha256Cksum(r) {
		return ErrNone
	}

	// Verify that X-Amz-Content-Sha256 Header == sha256(payload)
	// If X-Amz-Content-Sha256 header is not sent then we don't calculate/verify sha256(payload)
	sum := r.Header.Get("X-Amz-Content-Sha256")
	if isRequestPresignedSignatureV4(r) {
		sum = r.URL.Query().Get("X-Amz-Content-Sha256")
	}
	if sum != "" && sum != getSHA256Hash(payload) {
		return ErrContentSHA256Mismatch
	}
	return ErrNone
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
	authTypePresignedV2:     {},
	authTypeSigned:          {},
	authTypeSignedV2:        {},
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
		if !isHTTPRequestValid(r) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		a.handler.ServeHTTP(w, r)
		return
	}
	writeErrorResponse(w, ErrSignatureVersionNotSupported, r.URL)
}
