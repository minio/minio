/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/iam/validator"
)

const (
	// STS API version.
	stsAPIVersion = "2011-06-15"
)

// stsAPIHandlers implements and provides http handlers for AWS STS API.
type stsAPIHandlers struct{}

// registerSTSRouter - registers AWS STS compatible APIs.
func registerSTSRouter(router *mux.Router) {
	// Initialize STS.
	sts := &stsAPIHandlers{}

	// STS Router
	stsRouter := router.NewRoute().PathPrefix("/").Subrouter()

	// AssumeRoleWithClientGrants
	stsRouter.Methods("POST").HandlerFunc(httpTraceAll(sts.AssumeRoleWithClientGrants)).
		Queries("Action", "AssumeRoleWithClientGrants").
		Queries("Version", stsAPIVersion).
		Queries("Token", "{Token:.*}")

	// AssumeRoleWithWebIdentity
	stsRouter.Methods("POST").HandlerFunc(httpTraceAll(sts.AssumeRoleWithWebIdentity)).
		Queries("Action", "AssumeRoleWithWebIdentity").
		Queries("Version", stsAPIVersion).
		Queries("WebIdentityToken", "{Token:.*}")

}

// AssumeRoleWithWebIdentity - implementation of AWS STS API supporting OAuth2.0
// users from web identity provider such as Facebook, Google, or any OpenID
// Connect-compatible identity provider.
//
// Eg:-
//    $ curl https://minio:9000/?Action=AssumeRoleWithWebIdentity&WebIdentityToken=<jwt>
func (sts *stsAPIHandlers) AssumeRoleWithWebIdentity(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleWithWebIdentity")

	defer logger.AuditLog(w, r, "AssumeRoleWithWebIdentity", nil)

	if globalIAMValidators == nil {
		writeSTSErrorResponse(w, ErrSTSNotInitialized)
		return
	}

	// NOTE: this API only accepts JWT tokens.
	v, err := globalIAMValidators.Get("jwt")
	if err != nil {
		writeSTSErrorResponse(w, ErrSTSInvalidParameterValue)
		return
	}

	vars := mux.Vars(r)
	m, err := v.Validate(vars["Token"], r.URL.Query().Get("DurationSeconds"))
	if err != nil {
		switch err {
		case validator.ErrTokenExpired:
			writeSTSErrorResponse(w, ErrSTSWebIdentityExpiredToken)
		case validator.ErrInvalidDuration:
			writeSTSErrorResponse(w, ErrSTSInvalidParameterValue)
		default:
			logger.LogIf(ctx, err)
			writeSTSErrorResponse(w, ErrSTSInvalidParameterValue)
		}
		return
	}

	secret := globalServerConfig.GetCredential().SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, ErrSTSInternalError)
		return
	}

	// JWT has requested a custom claim with policy value set.
	// This is a Minio STS API specific value, this value should
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
		writeSTSErrorResponse(w, ErrSTSInternalError)
		return
	}

	// Notify all other Minio peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	encodedSuccessResponse := encodeResponse(&AssumeRoleWithWebIdentityResponse{
		Result: WebIdentityResult{
			Credentials:                 cred,
			SubjectFromWebIdentityToken: subFromToken,
		},
	})

	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// AssumeRoleWithClientGrants - implementation of AWS STS extension API supporting
// OAuth2.0 client credential grants.
//
// Eg:-
//    $ curl https://minio:9000/?Action=AssumeRoleWithClientGrants&Token=<jwt>
func (sts *stsAPIHandlers) AssumeRoleWithClientGrants(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleWithClientGrants")

	defer logger.AuditLog(w, r, "AssumeRoleWithClientGrants", nil)

	if globalIAMValidators == nil {
		writeSTSErrorResponse(w, ErrSTSNotInitialized)
		return
	}

	// NOTE: this API only accepts JWT tokens.
	v, err := globalIAMValidators.Get("jwt")
	if err != nil {
		writeSTSErrorResponse(w, ErrSTSInvalidParameterValue)
		return
	}

	vars := mux.Vars(r)
	m, err := v.Validate(vars["Token"], r.URL.Query().Get("DurationSeconds"))
	if err != nil {
		switch err {
		case validator.ErrTokenExpired:
			writeSTSErrorResponse(w, ErrSTSClientGrantsExpiredToken)
		case validator.ErrInvalidDuration:
			writeSTSErrorResponse(w, ErrSTSInvalidParameterValue)
		default:
			logger.LogIf(ctx, err)
			writeSTSErrorResponse(w, ErrSTSInvalidParameterValue)
		}
		return
	}

	secret := globalServerConfig.GetCredential().SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		logger.LogIf(ctx, err)
		writeSTSErrorResponse(w, ErrSTSInternalError)
		return
	}

	// JWT has requested a custom claim with policy value set.
	// This is a Minio STS API specific value, this value should
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
		writeSTSErrorResponse(w, ErrSTSInternalError)
		return
	}

	// Notify all other Minio peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	encodedSuccessResponse := encodeResponse(&AssumeRoleWithClientGrantsResponse{
		Result: ClientGrantsResult{
			Credentials:      cred,
			SubjectFromToken: subFromToken,
		},
	})

	writeSuccessResponseXML(w, encodedSuccessResponse)
}
