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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	xldap "github.com/minio/minio/cmd/config/identity/ldap"
	"github.com/minio/minio/cmd/config/identity/openid"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/wildcard"
	ldap "gopkg.in/ldap.v3"
)

const (
	// STS API version.
	stsAPIVersion = "2011-06-15"

	// STS API action constants
	clientGrants = "AssumeRoleWithClientGrants"
	webIdentity  = "AssumeRoleWithWebIdentity"
	ldapIdentity = "AssumeRoleWithLDAPIdentity"
	assumeRole   = "AssumeRole"

	stsRequestBodyLimit = 10 * (1 << 20) // 10 MiB

	// LDAP claim keys
	ldapUser   = "ldapUser"
	ldapGroups = "ldapGroups"
)

// stsAPIHandlers implements and provides http handlers for AWS STS API.
type stsAPIHandlers struct{}

// registerSTSRouter - registers AWS STS compatible APIs.
func registerSTSRouter(router *mux.Router) {
	// Initialize STS.
	sts := &stsAPIHandlers{}

	// STS Router
	stsRouter := router.NewRoute().PathPrefix(SlashSeparator).Subrouter()

	// Assume roles with no JWT, handles AssumeRole.
	stsRouter.Methods(http.MethodPost).MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
		ctypeOk := wildcard.MatchSimple("application/x-www-form-urlencoded*", r.Header.Get(xhttp.ContentType))
		authOk := wildcard.MatchSimple(signV4Algorithm+"*", r.Header.Get(xhttp.Authorization))
		noQueries := len(r.URL.Query()) == 0
		return ctypeOk && authOk && noQueries
	}).HandlerFunc(httpTraceAll(sts.AssumeRole))

	// Assume roles with JWT handler, handles both ClientGrants and WebIdentity.
	stsRouter.Methods("POST").MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
		ctypeOk := wildcard.MatchSimple("application/x-www-form-urlencoded*", r.Header.Get(xhttp.ContentType))
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

	// AssumeRoleWithLDAPIdentity
	stsRouter.Methods("POST").HandlerFunc(httpTraceAll(sts.AssumeRoleWithLDAPIdentity)).
		Queries("Action", ldapIdentity).
		Queries("Version", stsAPIVersion).
		Queries("LDAPUsername", "{LDAPUsername:.*}").
		Queries("LDAPPassword", "{LDAPPassword:.*}")
}

func checkAssumeRoleAuth(ctx context.Context, r *http.Request) (user auth.Credentials, stsErr STSErrorCode) {
	switch getRequestAuthType(r) {
	default:
		return user, ErrSTSAccessDenied
	case authTypeSigned:
		s3Err := isReqAuthenticated(ctx, r, globalServerRegion, serviceSTS)
		if STSErrorCode(s3Err) != ErrSTSNone {
			return user, STSErrorCode(s3Err)
		}
		var owner bool
		user, owner, s3Err = getReqAccessKeyV4(r, globalServerRegion, serviceSTS)
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
		writeSTSErrorResponse(ctx, w, stsErr, nil)
		return
	}
	if err := r.ParseForm(); err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	if r.Form.Get("Version") != stsAPIVersion {
		writeSTSErrorResponse(ctx, w, ErrSTSMissingParameter, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		return
	}

	action := r.Form.Get("Action")
	switch action {
	case assumeRole:
	default:
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	ctx = newContext(r, w, action)
	defer logger.AuditLog(w, r, action, nil)

	sessionPolicyStr := r.Form.Get("Policy")
	// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
	// The plain text that you use for both inline and managed session
	// policies shouldn't exceed 2048 characters.
	if len(sessionPolicyStr) > 2048 {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Session policy shouldn't exceed 2048 characters"))
		return
	}

	if len(sessionPolicyStr) > 0 {
		sessionPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(sessionPolicyStr)))
		if err != nil {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
			return
		}

		// Version in policy must not be empty
		if sessionPolicy.Version == "" {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Version cannot be empty expecting '2012-10-17'"))
			return
		}
	}

	var err error
	m := make(map[string]interface{})
	m["exp"], err = openid.GetDefaultExpiration(r.Form.Get("DurationSeconds"))
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	policies, err := globalIAMSys.PolicyDBGet(user.AccessKey, false)
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	policyName := ""
	if len(policies) > 0 {
		policyName = policies[0]
	}

	// This policy is the policy associated with the user
	// requesting for temporary credentials. The temporary
	// credentials will inherit the same policy requirements.
	m[iamPolicyName()] = policyName

	if len(sessionPolicyStr) > 0 {
		m[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString([]byte(sessionPolicyStr))
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInternalError, err)
		return
	}

	// Set the newly generated credentials.
	if err = globalIAMSys.SetTempUser(cred.AccessKey, cred, policyName); err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInternalError, err)
		return
	}

	// Notify all other MinIO peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUser(cred.AccessKey, true) {
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

	assumeRoleResponse.ResponseMetadata.RequestID = w.Header().Get(xhttp.AmzRequestID)
	writeSuccessResponseXML(w, encodeResponse(assumeRoleResponse))
}

func (sts *stsAPIHandlers) AssumeRoleWithJWT(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleJWTCommon")

	// Parse the incoming form data.
	if err := r.ParseForm(); err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	if r.Form.Get("Version") != stsAPIVersion {
		writeSTSErrorResponse(ctx, w, ErrSTSMissingParameter, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		return
	}

	action := r.Form.Get("Action")
	switch action {
	case clientGrants, webIdentity:
	default:
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	ctx = newContext(r, w, action)
	defer logger.AuditLog(w, r, action, nil)

	if globalOpenIDValidators == nil {
		writeSTSErrorResponse(ctx, w, ErrSTSNotInitialized, errServerNotInitialized)
		return
	}

	v, err := globalOpenIDValidators.Get("jwt")
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	token := r.Form.Get("Token")
	if token == "" {
		token = r.Form.Get("WebIdentityToken")
	}

	m, err := v.Validate(token, r.Form.Get("DurationSeconds"))
	if err != nil {
		switch err {
		case openid.ErrTokenExpired:
			switch action {
			case clientGrants:
				writeSTSErrorResponse(ctx, w, ErrSTSClientGrantsExpiredToken, err)
			case webIdentity:
				writeSTSErrorResponse(ctx, w, ErrSTSWebIdentityExpiredToken, err)
			}
			return
		case openid.ErrInvalidDuration:
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
			return
		}
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	sessionPolicyStr := r.Form.Get("Policy")
	// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
	// The plain text that you use for both inline and managed session
	// policies shouldn't exceed 2048 characters.
	if len(sessionPolicyStr) > 2048 {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Session policy should not exceed 2048 characters"))
		return
	}

	if len(sessionPolicyStr) > 0 {
		sessionPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(sessionPolicyStr)))
		if err != nil {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
			return
		}

		// Version in policy must not be empty
		if sessionPolicy.Version == "" {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Invalid session policy version"))
			return
		}
	}

	if len(sessionPolicyStr) > 0 {
		m[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString([]byte(sessionPolicyStr))
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInternalError, err)
		return
	}

	// JWT has requested a custom claim with policy value set.
	// This is a MinIO STS API specific value, this value should
	// be set and configured on your identity provider as part of
	// JWT custom claims.
	var policyName string
	if v, ok := m[iamPolicyName()]; ok {
		policyName, _ = v.(string)
	}

	var subFromToken string
	if v, ok := m["sub"]; ok {
		subFromToken, _ = v.(string)
	}

	// Set the newly generated credentials.
	if err = globalIAMSys.SetTempUser(cred.AccessKey, cred, policyName); err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInternalError, err)
		return
	}

	// Notify all other MinIO peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUser(cred.AccessKey, true) {
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
		clientGrantsResponse.ResponseMetadata.RequestID = w.Header().Get(xhttp.AmzRequestID)
		encodedSuccessResponse = encodeResponse(clientGrantsResponse)
	case webIdentity:
		webIdentityResponse := &AssumeRoleWithWebIdentityResponse{
			Result: WebIdentityResult{
				Credentials:                 cred,
				SubjectFromWebIdentityToken: subFromToken,
			},
		}
		webIdentityResponse.ResponseMetadata.RequestID = w.Header().Get(xhttp.AmzRequestID)
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

// AssumeRoleWithLDAPIdentity - implements user auth against LDAP server
func (sts *stsAPIHandlers) AssumeRoleWithLDAPIdentity(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleWithLDAPIdentity")

	// Parse the incoming form data.
	if err := r.ParseForm(); err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	if r.Form.Get("Version") != stsAPIVersion {
		writeSTSErrorResponse(ctx, w, ErrSTSMissingParameter, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		return
	}

	action := r.Form.Get("Action")
	switch action {
	case ldapIdentity:
	default:
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	ctx = newContext(r, w, action)
	defer logger.AuditLog(w, r, action, nil)

	ldapUsername := r.Form.Get("LDAPUsername")
	ldapPassword := r.Form.Get("LDAPPassword")

	if ldapUsername == "" || ldapPassword == "" {
		writeSTSErrorResponse(ctx, w, ErrSTSMissingParameter, fmt.Errorf("LDAPUsername and LDAPPassword cannot be empty"))
		return
	}

	sessionPolicyStr := r.Form.Get("Policy")
	// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
	// The plain text that you use for both inline and managed session
	// policies shouldn't exceed 2048 characters.
	if len(sessionPolicyStr) > 2048 {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Session policy should not exceed 2048 characters"))
		return
	}

	if len(sessionPolicyStr) > 0 {
		sessionPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(sessionPolicyStr)))
		if err != nil {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
			return
		}

		// Version in policy must not be empty
		if sessionPolicy.Version == "" {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("Version needs to be specified in session policy"))
			return
		}
	}

	ldapConn, err := globalLDAPConfig.Connect()
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("LDAP server connection failure: %v", err))
		return
	}
	if ldapConn == nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("LDAP server not configured: %v", err))
		return
	}

	usernameSubs, _ := xldap.NewSubstituter("username", ldapUsername)
	// We ignore error below as we already validated the username
	// format string at startup.
	usernameDN, _ := usernameSubs.Substitute(globalLDAPConfig.UsernameFormat)
	// Bind with user credentials to validate the password
	if err = ldapConn.Bind(usernameDN, ldapPassword); err != nil {
		err = fmt.Errorf("LDAP authentication failure: %v", err)
		writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, err)
		return
	}

	groups := []string{}
	if globalLDAPConfig.GroupSearchFilter != "" {
		// Verified user credentials. Now we find the groups they are
		// a member of.
		searchSubs, _ := xldap.NewSubstituter(
			"username", ldapUsername,
			"usernamedn", usernameDN,
		)
		// We ignore error below as we already validated the search string
		// at startup.
		groupSearchFilter, _ := searchSubs.Substitute(globalLDAPConfig.GroupSearchFilter)
		baseDN, _ := searchSubs.Substitute(globalLDAPConfig.GroupSearchBaseDN)
		searchRequest := ldap.NewSearchRequest(
			baseDN,
			ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
			groupSearchFilter,
			[]string{globalLDAPConfig.GroupNameAttribute},
			nil,
		)

		sr, err := ldapConn.Search(searchRequest)
		if err != nil {
			writeSTSErrorResponse(ctx, w, ErrSTSInvalidParameterValue, fmt.Errorf("LDAP search failure: %v", err))
			return
		}
		for _, entry := range sr.Entries {
			// We only queried one attribute, so we only look up
			// the first one.
			groups = append(groups, entry.Attributes[0].Values...)
		}
	}
	expiryDur := globalLDAPConfig.GetExpiryDuration()
	m := map[string]interface{}{
		"exp":        UTCNow().Add(expiryDur).Unix(),
		"ldapUser":   ldapUsername,
		"ldapGroups": groups,
	}

	if len(sessionPolicyStr) > 0 {
		m[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString([]byte(sessionPolicyStr))
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInternalError, err)
		return
	}

	policyName := ""
	// Set the newly generated credentials.
	if err = globalIAMSys.SetTempUser(cred.AccessKey, cred, policyName); err != nil {
		writeSTSErrorResponse(ctx, w, ErrSTSInternalError, err)
		return
	}

	// Notify all other MinIO peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUser(cred.AccessKey, true) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	ldapIdentityResponse := &AssumeRoleWithLDAPResponse{
		Result: LDAPIdentityResult{
			Credentials: cred,
		},
	}
	ldapIdentityResponse.ResponseMetadata.RequestID = w.Header().Get(xhttp.AmzRequestID)
	encodedSuccessResponse := encodeResponse(ldapIdentityResponse)

	writeSuccessResponseXML(w, encodedSuccessResponse)
}
