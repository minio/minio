/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/iam/validator"
	"github.com/minio/minio/pkg/wildcard"
)

const (
	// STS API version.
	stsAPIVersion = "2011-06-15"

	// STS API action constants
	clientGrants = "AssumeRoleWithClientGrants"
	webIdentity  = "AssumeRoleWithWebIdentity"
	assumeRole   = "AssumeRole"
)

// stsAPIHandlers implements and provides http handlers for AWS STS API.
type stsAPIHandlers struct{}

// registerSTSRouter - registers AWS STS compatible APIs.
func registerSTSRouter(router *mux.Router) {
	// Initialize STS.
	sts := &stsAPIHandlers{}

	// STS Router
	stsRouter := router.NewRoute().PathPrefix("/").Subrouter()

	// Assume roles with no JWT, handles AssumeRole.
	stsRouter.Methods("POST").MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
		ctypeOk := wildcard.MatchSimple("application/x-www-form-urlencoded*", r.Header.Get("Content-Type"))
		authOk := wildcard.MatchSimple("AWS4-HMAC-SHA256*", r.Header.Get("Authorization"))
		noQueries := len(r.URL.Query()) == 0
		return ctypeOk && authOk && noQueries
	}).HandlerFunc(httpTraceAll(sts.AssumeRole))

	// Assume roles with JWT handler, handles both ClientGrants and WebIdentity.
	stsRouter.Methods("POST").MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
		ctypeOk := wildcard.MatchSimple("application/x-www-form-urlencoded*", r.Header.Get("Content-Type"))
		noQueries := len(r.URL.Query()) == 0
		return ctypeOk && noQueries
	}).HandlerFunc(httpTraceAll(sts.AssumeRoleWithJWT))

	// AssumeRoleWithClientGrants
	stsRouter.Methods("POST").HandlerFunc(httpTraceAll(sts.AssumeRoleWithClientGrants)).
		Queries("Action", clientGrants).
		Queries("Version", stsAPIVersion).
		Queries("Token", "{Token:.*}")

	// AssumeRoleWithWebIdentity
	stsRouter.Methods("POST").HandlerFunc(httpTraceAll(sts.AssumeRoleWithWebIdentity)).
		Queries("Action", webIdentity).
		Queries("Version", stsAPIVersion).
		Queries("WebIdentityToken", "{Token:.*}")

}

func checkAssumeRoleAuth(ctx context.Context, r *http.Request) (user auth.Credentials, stsErr STSErrorCode) {
	switch getRequestAuthType(r) {
	default:
		return user, ErrSTSAccessDenied
	case authTypeSigned:
		s3Err := isReqAuthenticated(ctx, r, globalServerConfig.GetRegion(), serviceSTS)
		if STSErrorCode(s3Err) != ErrSTSNone {
			return user, STSErrorCode(s3Err)
		}
		var owner bool
		user, owner, s3Err = getReqAccessKeyV4(r, globalServerConfig.GetRegion(), serviceSTS)
		if STSErrorCode(s3Err) != ErrSTSNone {
			return user, STSErrorCode(s3Err)
		}
		// Root credentials are not allowed to use STS API
		if owner {
			return user, ErrSTSAccessDenied
		}
	}

	// Session tokens are not allowed in STS AssumeRole requests.
	if getSessionToken(r) != "" {
		return user, ErrSTSAccessDenied
	}

	return user, ErrSTSNone
}

// AssumeRole - implementation of AWS STS API AssumeRole to get temporary
// credentials for regular users on Minio.
// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
func (sts *stsAPIHandlers) AssumeRole(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRole")

	user, stsErr := checkAssumeRoleAuth(ctx, r)
	if stsErr != ErrSTSNone {
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(stsErr))
		return
	}

	if err := r.ParseForm(); err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	if r.Form.Get("Policy") != "" {
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	if r.Form.Get("Version") != stsAPIVersion {
		logger.LogIf(ctx, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSMissingParameter))
		return
	}

	action := r.Form.Get("Action")
	switch action {
	case assumeRole:
	default:
		logger.LogIf(ctx, fmt.Errorf("Unsupported action %s", action))
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	ctx = newContext(r, w, action)
	defer logger.AuditLog(w, r, action, nil)

	var err error
	m := make(map[string]interface{})
	m["exp"], err = validator.GetDefaultExpiration(r.Form.Get("DurationSeconds"))
	if err != nil {
		switch err {
		case validator.ErrInvalidDuration:
			writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		default:
			logger.LogIf(ctx, err)
			writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		}
		return
	}

	policyName, err := globalIAMSys.GetUserPolicy(user.AccessKey)
	if err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	// This policy is the policy associated with the user
	// requesting for temporary credentials. The temporary
	// credentials will inherit the same policy requirements.
	m["policy"] = policyName

	secret := globalServerConfig.GetCredential().SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInternalError))
		return
	}

	// Set the newly generated credentials.
	if err = globalIAMSys.SetTempUser(cred.AccessKey, cred, policyName); err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInternalError))
		return
	}

	// Notify all other MinIO peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	assumeRoleResponse := &AssumeRoleResponse{
		Result: AssumeRoleResult{
			Credentials: cred,
		},
	}

	assumeRoleResponse.ResponseMetadata.RequestID = w.Header().Get(responseRequestIDKey)
	writeSuccessResponseXML(w, encodeResponse(assumeRoleResponse))
}

func (sts *stsAPIHandlers) AssumeRoleWithJWT(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleJWTCommon")

	// Parse the incoming form data.
	if err := r.ParseForm(); err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	if r.Form.Get("Policy") != "" {
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	if r.Form.Get("Version") != stsAPIVersion {
		logger.LogIf(ctx, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSMissingParameter))
		return
	}

	action := r.Form.Get("Action")
	switch action {
	case clientGrants, webIdentity:
	default:
		logger.LogIf(ctx, fmt.Errorf("Unsupported action %s", action))
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	ctx = newContext(r, w, action)
	defer logger.AuditLog(w, r, action, nil)

	if globalIAMValidators == nil {
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSNotInitialized))
		return
	}

	v, err := globalIAMValidators.Get("jwt")
	if err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	token := r.Form.Get("Token")
	if token == "" {
		token = r.Form.Get("WebIdentityToken")
	}

	m, err := v.Validate(token, r.Form.Get("DurationSeconds"))
	if err != nil {
		switch err {
		case validator.ErrTokenExpired:
			switch action {
			case clientGrants:
				writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSClientGrantsExpiredToken))
			case webIdentity:
				writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSWebIdentityExpiredToken))
			}
			return
		case validator.ErrInvalidDuration:
			writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
			return
		}
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInvalidParameterValue))
		return
	}

	secret := globalServerConfig.GetCredential().SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInternalError))
		return
	}

	// JWT has requested a custom claim with policy value set.
	// This is a MinIO STS API specific value, this value should
	// be set and configured on your identity provider as part of
	// JWT custom claims.
	var policyName string
	if v, ok := m["policy"]; ok {
		policyName, _ = v.(string)
	}

	var subFromToken string
	if v, ok := m["sub"]; ok {
		subFromToken, _ = v.(string)
	}

	// Set the newly generated credentials.
	if err = globalIAMSys.SetTempUser(cred.AccessKey, cred, policyName); err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, stsErrCodes.ToSTSErr(ErrSTSInternalError))
		return
	}

	// Notify all other MinIO peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	var encodedSuccessResponse []byte
	switch action {
	case clientGrants:
		clientGrantsResponse := &AssumeRoleWithClientGrantsResponse{
			Result: ClientGrantsResult{
				Credentials:      cred,
				SubjectFromToken: subFromToken,
			},
		}
		clientGrantsResponse.ResponseMetadata.RequestID = w.Header().Get(responseRequestIDKey)
		encodedSuccessResponse = encodeResponse(clientGrantsResponse)
	case webIdentity:
		webIdentityResponse := &AssumeRoleWithWebIdentityResponse{
			Result: WebIdentityResult{
				Credentials:                 cred,
				SubjectFromWebIdentityToken: subFromToken,
			},
		}
		webIdentityResponse.ResponseMetadata.RequestID = w.Header().Get(responseRequestIDKey)
		encodedSuccessResponse = encodeResponse(webIdentityResponse)
	}

	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// AssumeRoleWithWebIdentity - implementation of AWS STS API supporting OAuth2.0
// users from web identity provider such as Facebook, Google, or any OpenID
// Connect-compatible identity provider.
//
// Eg:-
//    $ curl https://minio:9000/?Action=AssumeRoleWithWebIdentity&WebIdentityToken=<jwt>
func (sts *stsAPIHandlers) AssumeRoleWithWebIdentity(w http.ResponseWriter, r *http.Request) {
	sts.AssumeRoleWithJWT(w, r)
}

// AssumeRoleWithClientGrants - implementation of AWS STS extension API supporting
// OAuth2.0 client credential grants.
//
// Eg:-
//    $ curl https://minio:9000/?Action=AssumeRoleWithClientGrants&Token=<jwt>
func (sts *stsAPIHandlers) AssumeRoleWithClientGrants(w http.ResponseWriter, r *http.Request) {
	sts.AssumeRoleWithJWT(w, r)
}
