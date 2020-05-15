/*
 * MinIO Cloud Storage, (C) 2015-2018 MinIO, Inc.
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
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
	xjwt "github.com/minio/minio/cmd/jwt"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/hash"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

// Verify if request has JWT.
func isRequestJWT(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get(xhttp.Authorization), jwtAlgorithm)
}

// Verify if request has AWS Signature Version '4'.
func isRequestSignatureV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get(xhttp.Authorization), signV4Algorithm)
}

// Verify if request has AWS Signature Version '2'.
func isRequestSignatureV2(r *http.Request) bool {
	return (!strings.HasPrefix(r.Header.Get(xhttp.Authorization), signV4Algorithm) &&
		strings.HasPrefix(r.Header.Get(xhttp.Authorization), signV2Algorithm))
}

// Verify if request has AWS PreSign Version '4'.
func isRequestPresignedSignatureV4(r *http.Request) bool {
	_, ok := r.URL.Query()[xhttp.AmzCredential]
	return ok
}

// Verify request has AWS PreSign Version '2'.
func isRequestPresignedSignatureV2(r *http.Request) bool {
	_, ok := r.URL.Query()[xhttp.AmzAccessKeyID]
	return ok
}

// Verify if request has AWS Post policy Signature Version '4'.
func isRequestPostPolicySignatureV4(r *http.Request) bool {
	return strings.Contains(r.Header.Get(xhttp.ContentType), "multipart/form-data") &&
		r.Method == http.MethodPost
}

// Verify if the request has AWS Streaming Signature Version '4'. This is only valid for 'PUT' operation.
func isRequestSignStreamingV4(r *http.Request) bool {
	return r.Header.Get(xhttp.AmzContentSha256) == streamingContentSHA256 &&
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
	authTypeSTS
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
	} else if _, ok := r.URL.Query()[xhttp.Action]; ok {
		return authTypeSTS
	} else if _, ok := r.Header[xhttp.Authorization]; !ok {
		return authTypeAnonymous
	}
	return authTypeUnknown
}

func validateAdminSignature(ctx context.Context, r *http.Request, region string) (auth.Credentials, map[string]interface{}, bool, APIErrorCode) {
	var cred auth.Credentials
	var owner bool
	s3Err := ErrAccessDenied
	if _, ok := r.Header[xhttp.AmzContentSha256]; ok &&
		getRequestAuthType(r) == authTypeSigned && !skipContentSha256Cksum(r) {
		// We only support admin credentials to access admin APIs.
		cred, owner, s3Err = getReqAccessKeyV4(r, region, serviceS3)
		if s3Err != ErrNone {
			return cred, nil, owner, s3Err
		}

		// we only support V4 (no presign) with auth body
		s3Err = isReqAuthenticated(ctx, r, region, serviceS3)
	}
	if s3Err != ErrNone {
		reqInfo := (&logger.ReqInfo{}).AppendTags("requestHeaders", dumpRequest(r))
		ctx := logger.SetReqInfo(ctx, reqInfo)
		logger.LogIf(ctx, errors.New(getAPIError(s3Err).Description), logger.Application)
		return cred, nil, owner, s3Err
	}

	claims, s3Err := checkClaimsFromToken(r, cred)
	if s3Err != ErrNone {
		return cred, nil, owner, s3Err
	}

	return cred, claims, owner, ErrNone
}

// checkAdminRequestAuthType checks whether the request is a valid signature V2 or V4 request.
// It does not accept presigned or JWT or anonymous requests.
func checkAdminRequestAuthType(ctx context.Context, r *http.Request, action iampolicy.AdminAction, region string) (auth.Credentials, APIErrorCode) {
	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, region)
	if s3Err != ErrNone {
		return cred, s3Err
	}
	if globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          iampolicy.Action(action),
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		IsOwner:         owner,
		Claims:          claims,
	}) {
		// Request is allowed return the appropriate access key.
		return cred, ErrNone
	}

	return cred, ErrAccessDenied
}

// Fetch the security token set by the client.
func getSessionToken(r *http.Request) (token string) {
	token = r.Header.Get(xhttp.AmzSecurityToken)
	if token != "" {
		return token
	}
	return r.URL.Query().Get(xhttp.AmzSecurityToken)
}

// Fetch claims in the security token returned by the client, doesn't return
// errors - upon errors the returned claims map will be empty.
func mustGetClaimsFromToken(r *http.Request) map[string]interface{} {
	claims, _ := getClaimsFromToken(r, getSessionToken(r))
	return claims
}

// Fetch claims in the security token returned by the client.
func getClaimsFromToken(r *http.Request, token string) (map[string]interface{}, error) {
	claims := xjwt.NewMapClaims()
	if token == "" {
		return claims.Map(), nil
	}

	stsTokenCallback := func(claims *xjwt.MapClaims) ([]byte, error) {
		// JWT token for x-amz-security-token is signed with admin
		// secret key, temporary credentials become invalid if
		// server admin credentials change. This is done to ensure
		// that clients cannot decode the token using the temp
		// secret keys and generate an entirely new claim by essentially
		// hijacking the policies. We need to make sure that this is
		// based an admin credential such that token cannot be decoded
		// on the client side and is treated like an opaque value.
		return []byte(globalActiveCred.SecretKey), nil
	}

	if err := xjwt.ParseWithClaims(token, claims, stsTokenCallback); err != nil {
		return nil, errAuthentication
	}

	if globalPolicyOPA == nil {
		// If OPA is not set and if ldap claim key is set, allow the claim.
		if _, ok := claims.Lookup(ldapUser); ok {
			return claims.Map(), nil
		}

		// If OPA is not set, session token should
		// have a policy and its mandatory, reject
		// requests without policy claim.
		_, pokOpenID := claims.Lookup(iamPolicyClaimNameOpenID())
		_, pokSA := claims.Lookup(iamPolicyClaimNameSA())
		if !pokOpenID && !pokSA {
			return nil, errAuthentication
		}

		sp, spok := claims.Lookup(iampolicy.SessionPolicyName)
		if !spok {
			return claims.Map(), nil
		}
		// Looks like subpolicy is set and is a string, if set then its
		// base64 encoded, decode it. Decoding fails reject such requests.
		spBytes, err := base64.StdEncoding.DecodeString(sp)
		if err != nil {
			// Base64 decoding fails, we should log to indicate
			// something is malforming the request sent by client.
			logger.LogIf(r.Context(), err, logger.Application)
			return nil, errAuthentication
		}
		claims.MapClaims[iampolicy.SessionPolicyName] = string(spBytes)
	}

	return claims.Map(), nil
}

// Fetch claims in the security token returned by the client and validate the token.
func checkClaimsFromToken(r *http.Request, cred auth.Credentials) (map[string]interface{}, APIErrorCode) {
	token := getSessionToken(r)
	if token != "" && cred.AccessKey == "" {
		return nil, ErrNoAccessKey
	}
	if cred.IsServiceAccount() && token == "" {
		token = cred.SessionToken
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(cred.SessionToken)) != 1 {
		return nil, ErrInvalidToken
	}
	claims, err := getClaimsFromToken(r, token)
	if err != nil {
		return nil, toAPIErrorCode(r.Context(), err)
	}
	return claims, ErrNone
}

// Check request auth type verifies the incoming http request
// - validates the request signature
// - validates the policy action if anonymous tests bucket policies if any,
//   for authenticated requests validates IAM policies.
// returns APIErrorCode if any to be replied to the client.
func checkRequestAuthType(ctx context.Context, r *http.Request, action policy.Action, bucketName, objectName string) (s3Err APIErrorCode) {
	_, _, s3Err = checkRequestAuthTypeToAccessKey(ctx, r, action, bucketName, objectName)
	return s3Err
}

// Check request auth type verifies the incoming http request
// - validates the request signature
// - validates the policy action if anonymous tests bucket policies if any,
//   for authenticated requests validates IAM policies.
// returns APIErrorCode if any to be replied to the client.
// Additionally returns the accessKey used in the request, and if this request is by an admin.
func checkRequestAuthTypeToAccessKey(ctx context.Context, r *http.Request, action policy.Action, bucketName, objectName string) (accessKey string, owner bool, s3Err APIErrorCode) {
	var cred auth.Credentials
	switch getRequestAuthType(r) {
	case authTypeUnknown, authTypeStreamingSigned:
		return accessKey, owner, ErrSignatureVersionNotSupported
	case authTypePresignedV2, authTypeSignedV2:
		if s3Err = isReqAuthenticatedV2(r); s3Err != ErrNone {
			return accessKey, owner, s3Err
		}
		cred, owner, s3Err = getReqAccessKeyV2(r)
	case authTypeSigned, authTypePresigned:
		region := globalServerRegion
		switch action {
		case policy.GetBucketLocationAction, policy.ListAllMyBucketsAction:
			region = ""
		}
		if s3Err = isReqAuthenticated(ctx, r, region, serviceS3); s3Err != ErrNone {
			return accessKey, owner, s3Err
		}
		cred, owner, s3Err = getReqAccessKeyV4(r, region, serviceS3)
	}
	if s3Err != ErrNone {
		return accessKey, owner, s3Err
	}

	var claims map[string]interface{}
	claims, s3Err = checkClaimsFromToken(r, cred)
	if s3Err != ErrNone {
		return accessKey, owner, s3Err
	}

	// LocationConstraint is valid only for CreateBucketAction.
	var locationConstraint string
	if action == policy.CreateBucketAction {
		// To extract region from XML in request body, get copy of request body.
		payload, err := ioutil.ReadAll(io.LimitReader(r.Body, maxLocationConstraintSize))
		if err != nil {
			logger.LogIf(ctx, err, logger.Application)
			return accessKey, owner, ErrMalformedXML
		}

		// Populate payload to extract location constraint.
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))

		var s3Error APIErrorCode
		locationConstraint, s3Error = parseLocationConstraint(r)
		if s3Error != ErrNone {
			return accessKey, owner, s3Error
		}

		// Populate payload again to handle it in HTTP handler.
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	}

	if cred.AccessKey == "" {
		if globalPolicySys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Action:          action,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, locationConstraint, "", nil),
			IsOwner:         false,
			ObjectName:      objectName,
		}) {
			// Request is allowed return the appropriate access key.
			return cred.AccessKey, owner, ErrNone
		}
		return cred.AccessKey, owner, ErrAccessDenied
	}
	if globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          iampolicy.Action(action),
		BucketName:      bucketName,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		ObjectName:      objectName,
		IsOwner:         owner,
		Claims:          claims,
	}) {
		// Request is allowed return the appropriate access key.
		return cred.AccessKey, owner, ErrNone
	}
	return cred.AccessKey, owner, ErrAccessDenied
}

// Verify if request has valid AWS Signature Version '2'.
func isReqAuthenticatedV2(r *http.Request) (s3Error APIErrorCode) {
	if isRequestSignatureV2(r) {
		return doesSignV2Match(r)
	}
	return doesPresignV2SignatureMatch(r)
}

func reqSignatureV4Verify(r *http.Request, region string, stype serviceType) (s3Error APIErrorCode) {
	sha256sum := getContentSha256Cksum(r, stype)
	switch {
	case isRequestSignatureV4(r):
		return doesSignatureMatch(sha256sum, r, region, stype)
	case isRequestPresignedSignatureV4(r):
		return doesPresignedSignatureMatch(sha256sum, r, region, stype)
	default:
		return ErrAccessDenied
	}
}

// Verify if request has valid AWS Signature Version '4'.
func isReqAuthenticated(ctx context.Context, r *http.Request, region string, stype serviceType) (s3Error APIErrorCode) {
	if errCode := reqSignatureV4Verify(r, region, stype); errCode != ErrNone {
		return errCode
	}

	var (
		err                       error
		contentMD5, contentSHA256 []byte
	)

	// Extract 'Content-Md5' if present.
	contentMD5, err = checkValidMD5(r.Header)
	if err != nil {
		return ErrInvalidDigest
	}

	// Extract either 'X-Amz-Content-Sha256' header or 'X-Amz-Content-Sha256' query parameter (if V4 presigned)
	// Do not verify 'X-Amz-Content-Sha256' if skipSHA256.
	if skipSHA256 := skipContentSha256Cksum(r); !skipSHA256 && isRequestPresignedSignatureV4(r) {
		if sha256Sum, ok := r.URL.Query()[xhttp.AmzContentSha256]; ok && len(sha256Sum) > 0 {
			contentSHA256, err = hex.DecodeString(sha256Sum[0])
			if err != nil {
				return ErrContentSHA256Mismatch
			}
		}
	} else if _, ok := r.Header[xhttp.AmzContentSha256]; !skipSHA256 && ok {
		contentSHA256, err = hex.DecodeString(r.Header.Get(xhttp.AmzContentSha256))
		if err != nil || len(contentSHA256) == 0 {
			return ErrContentSHA256Mismatch
		}
	}

	// Verify 'Content-Md5' and/or 'X-Amz-Content-Sha256' if present.
	// The verification happens implicit during reading.
	reader, err := hash.NewReader(r.Body, -1, hex.EncodeToString(contentMD5),
		hex.EncodeToString(contentSHA256), -1, globalCLIContext.StrictS3Compat)
	if err != nil {
		return toAPIErrorCode(ctx, err)
	}
	r.Body = reader
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
		if _, _, authErr := webRequestAuthenticate(r); authErr != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		a.handler.ServeHTTP(w, r)
		return
	} else if aType == authTypeSTS {
		a.handler.ServeHTTP(w, r)
		return
	}
	writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrSignatureVersionNotSupported), r.URL, guessIsBrowserReq(r))
}

func validateSignature(atype authType, r *http.Request) (auth.Credentials, bool, map[string]interface{}, APIErrorCode) {
	var cred auth.Credentials
	var owner bool
	var s3Err APIErrorCode
	switch atype {
	case authTypeUnknown, authTypeStreamingSigned:
		return cred, owner, nil, ErrSignatureVersionNotSupported
	case authTypeSignedV2, authTypePresignedV2:
		if s3Err = isReqAuthenticatedV2(r); s3Err != ErrNone {
			return cred, owner, nil, s3Err
		}
		cred, owner, s3Err = getReqAccessKeyV2(r)
	case authTypePresigned, authTypeSigned:
		region := globalServerRegion
		if s3Err = isReqAuthenticated(GlobalContext, r, region, serviceS3); s3Err != ErrNone {
			return cred, owner, nil, s3Err
		}
		cred, owner, s3Err = getReqAccessKeyV4(r, region, serviceS3)
	}
	if s3Err != ErrNone {
		return cred, owner, nil, s3Err
	}

	claims, s3Err := checkClaimsFromToken(r, cred)
	if s3Err != ErrNone {
		return cred, owner, nil, s3Err
	}

	return cred, owner, claims, ErrNone
}

func isPutRetentionAllowed(bucketName, objectName string, retDays int, retDate time.Time, retMode objectlock.RetMode, byPassSet bool, r *http.Request, cred auth.Credentials, owner bool, claims map[string]interface{}) (s3Err APIErrorCode) {
	var retSet bool
	if cred.AccessKey == "" {
		conditions := getConditionValues(r, "", "", nil)
		conditions["object-lock-mode"] = []string{string(retMode)}
		conditions["object-lock-retain-until-date"] = []string{retDate.Format(time.RFC3339)}
		if retDays > 0 {
			conditions["object-lock-remaining-retention-days"] = []string{strconv.Itoa(retDays)}
		}
		if retMode == objectlock.RetGovernance && byPassSet {
			byPassSet = globalPolicySys.IsAllowed(policy.Args{
				AccountName:     cred.AccessKey,
				Action:          policy.Action(policy.BypassGovernanceRetentionAction),
				BucketName:      bucketName,
				ConditionValues: conditions,
				IsOwner:         false,
				ObjectName:      objectName,
			})
		}
		if globalPolicySys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Action:          policy.Action(policy.PutObjectRetentionAction),
			BucketName:      bucketName,
			ConditionValues: conditions,
			IsOwner:         false,
			ObjectName:      objectName,
		}) {
			retSet = true
		}
		if byPassSet || retSet {
			return ErrNone
		}
		return ErrAccessDenied
	}

	conditions := getConditionValues(r, "", cred.AccessKey, claims)
	conditions["object-lock-mode"] = []string{string(retMode)}
	conditions["object-lock-retain-until-date"] = []string{retDate.Format(time.RFC3339)}
	if retDays > 0 {
		conditions["object-lock-remaining-retention-days"] = []string{strconv.Itoa(retDays)}
	}
	if retMode == objectlock.RetGovernance && byPassSet {
		byPassSet = globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     cred.AccessKey,
			Action:          policy.BypassGovernanceRetentionAction,
			BucketName:      bucketName,
			ObjectName:      objectName,
			ConditionValues: conditions,
			IsOwner:         owner,
			Claims:          claims,
		})
	}
	if globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          policy.PutObjectRetentionAction,
		BucketName:      bucketName,
		ConditionValues: conditions,
		ObjectName:      objectName,
		IsOwner:         owner,
		Claims:          claims,
	}) {
		retSet = true
	}
	if byPassSet || retSet {
		return ErrNone
	}
	return ErrAccessDenied
}

// isPutActionAllowed - check if PUT operation is allowed on the resource, this
// call verifies bucket policies and IAM policies, supports multi user
// checks etc.
func isPutActionAllowed(atype authType, bucketName, objectName string, r *http.Request, action iampolicy.Action) (s3Err APIErrorCode) {
	var cred auth.Credentials
	var owner bool
	switch atype {
	case authTypeUnknown:
		return ErrSignatureVersionNotSupported
	case authTypeSignedV2, authTypePresignedV2:
		cred, owner, s3Err = getReqAccessKeyV2(r)
	case authTypeStreamingSigned, authTypePresigned, authTypeSigned:
		region := globalServerRegion
		cred, owner, s3Err = getReqAccessKeyV4(r, region, serviceS3)
	}
	if s3Err != ErrNone {
		return s3Err
	}

	claims, s3Err := checkClaimsFromToken(r, cred)
	if s3Err != ErrNone {
		return s3Err
	}

	// Do not check for PutObjectRetentionAction permission,
	// if mode and retain until date are not set.
	// Can happen when bucket has default lock config set
	if action == iampolicy.PutObjectRetentionAction &&
		r.Header.Get(xhttp.AmzObjectLockMode) == "" &&
		r.Header.Get(xhttp.AmzObjectLockRetainUntilDate) == "" {
		return ErrNone
	}

	if cred.AccessKey == "" {
		if globalPolicySys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Action:          policy.Action(action),
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", "", nil),
			IsOwner:         false,
			ObjectName:      objectName,
		}) {
			return ErrNone
		}
		return ErrAccessDenied
	}

	if globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          action,
		BucketName:      bucketName,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		ObjectName:      objectName,
		IsOwner:         owner,
		Claims:          claims,
	}) {
		return ErrNone
	}
	return ErrAccessDenied
}
