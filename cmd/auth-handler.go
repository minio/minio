/*
 * Minio Cloud Storage, (C) 2015-2018 Minio, Inc.
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
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
)

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
	return strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") &&
		r.Method == http.MethodPost
}

// Verify if the request has AWS Streaming Signature Version '4'. This is only valid for 'PUT' operation.
func isRequestSignStreamingV4(r *http.Request) bool {
	return r.Header.Get("x-amz-content-sha256") == streamingContentSHA256 &&
		r.Method == http.MethodPut
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

// checkAdminRequestAuthType checks whether the request is a valid signature V2 or V4 request.
// It does not accept presigned or JWT or anonymous requests.
func checkAdminRequestAuthType(r *http.Request, region string) APIErrorCode {
	s3Err := ErrAccessDenied
	if getRequestAuthType(r) == authTypeSigned { // we only support V4 (no presign)
		s3Err = isReqAuthenticated(r, region)
	}
	if s3Err != ErrNone {
		reqInfo := (&logger.ReqInfo{}).AppendTags("requestHeaders", dumpRequest(r))
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, errors.New(getAPIError(s3Err).Description))
	}
	return s3Err
}

func checkRequestAuthType(ctx context.Context, r *http.Request, action policy.Action, bucketName, objectName string) APIErrorCode {
	isOwner := true
	accountName := globalServerConfig.GetCredential().AccessKey

	switch getRequestAuthType(r) {
	case authTypeUnknown:
		return ErrAccessDenied
	case authTypePresignedV2, authTypeSignedV2:
		if errorCode := isReqAuthenticatedV2(r); errorCode != ErrNone {
			return errorCode
		}
	case authTypeSigned, authTypePresigned:
		region := globalServerConfig.GetRegion()
		switch action {
		case policy.GetBucketLocationAction, policy.ListAllMyBucketsAction:
			region = ""
		}

		if errorCode := isReqAuthenticated(r, region); errorCode != ErrNone {
			return errorCode
		}
	default:
		isOwner = false
		accountName = ""
	}

	// LocationConstraint is valid only for CreateBucketAction.
	var locationConstraint string
	if action == policy.CreateBucketAction {
		// To extract region from XML in request body, get copy of request body.
		payload, err := ioutil.ReadAll(io.LimitReader(r.Body, maxLocationConstraintSize))
		if err != nil {
			logger.LogIf(ctx, err)
			return ErrMalformedXML
		}

		// Populate payload to extract location constraint.
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))

		var s3Error APIErrorCode
		locationConstraint, s3Error = parseLocationConstraint(r)
		if s3Error != ErrNone {
			return s3Error
		}

		// Populate payload again to handle it in HTTP handler.
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	}

	if globalPolicySys.IsAllowed(policy.Args{
		AccountName:     accountName,
		Action:          action,
		BucketName:      bucketName,
		ConditionValues: getConditionValues(r, locationConstraint),
		IsOwner:         isOwner,
		ObjectName:      objectName,
	}) {
		return ErrNone
	}

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
	if errCode := reqSignatureV4Verify(r, region); errCode != ErrNone {
		return errCode
	}

	var (
		err                       error
		contentMD5, contentSHA256 []byte
	)
	// Extract 'Content-Md5' if present.
	if _, ok := r.Header["Content-Md5"]; ok {
		contentMD5, err = base64.StdEncoding.Strict().DecodeString(r.Header.Get("Content-Md5"))
		if err != nil || len(contentMD5) == 0 {
			return ErrInvalidDigest
		}
	}

	// Extract either 'X-Amz-Content-Sha256' header or 'X-Amz-Content-Sha256' query parameter (if V4 presigned)
	// Do not verify 'X-Amz-Content-Sha256' if skipSHA256.
	if skipSHA256 := skipContentSha256Cksum(r); !skipSHA256 && isRequestPresignedSignatureV4(r) {
		if sha256Sum, ok := r.URL.Query()["X-Amz-Content-Sha256"]; ok && len(sha256Sum) > 0 {
			contentSHA256, err = hex.DecodeString(sha256Sum[0])
			if err != nil {
				return ErrContentSHA256Mismatch
			}
		}
	} else if _, ok := r.Header["X-Amz-Content-Sha256"]; !skipSHA256 && ok {
		contentSHA256, err = hex.DecodeString(r.Header.Get("X-Amz-Content-Sha256"))
		if err != nil || len(contentSHA256) == 0 {
			return ErrContentSHA256Mismatch
		}
	}

	// Verify 'Content-Md5' and/or 'X-Amz-Content-Sha256' if present.
	// The verification happens implicit during reading.
	reader, err := hash.NewReader(r.Body, -1, hex.EncodeToString(contentMD5), hex.EncodeToString(contentSHA256))
	if err != nil {
		return toAPIErrorCode(err)
	}
	r.Body = ioutil.NopCloser(reader)
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
