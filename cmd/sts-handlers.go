/// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
	"github.com/minio/pkg/wildcard"
)

const (
	// STS API version.
	stsAPIVersion             = "2011-06-15"
	stsVersion                = "Version"
	stsAction                 = "Action"
	stsPolicy                 = "Policy"
	stsToken                  = "Token"
	stsRoleArn                = "RoleArn"
	stsWebIdentityToken       = "WebIdentityToken"
	stsWebIdentityAccessToken = "WebIdentityAccessToken" // only valid if UserInfo is enabled.
	stsDurationSeconds        = "DurationSeconds"
	stsLDAPUsername           = "LDAPUsername"
	stsLDAPPassword           = "LDAPPassword"

	// STS API action constants
	clientGrants        = "AssumeRoleWithClientGrants"
	webIdentity         = "AssumeRoleWithWebIdentity"
	ldapIdentity        = "AssumeRoleWithLDAPIdentity"
	clientCertificate   = "AssumeRoleWithCertificate"
	customTokenIdentity = "AssumeRoleWithCustomToken"
	assumeRole          = "AssumeRole"

	stsRequestBodyLimit = 10 * (1 << 20) // 10 MiB

	// JWT claim keys
	expClaim = "exp"
	subClaim = "sub"
	audClaim = "aud"
	issClaim = "iss"

	// JWT claim to check the parent user
	parentClaim = "parent"

	// LDAP claim keys
	ldapUser  = "ldapUser"
	ldapUserN = "ldapUsername"

	// Role Claim key
	roleArnClaim = "roleArn"
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
		noQueries := len(r.URL.RawQuery) == 0
		return ctypeOk && authOk && noQueries
	}).HandlerFunc(httpTraceAll(sts.AssumeRole))

	// Assume roles with JWT handler, handles both ClientGrants and WebIdentity.
	stsRouter.Methods(http.MethodPost).MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
		ctypeOk := wildcard.MatchSimple("application/x-www-form-urlencoded*", r.Header.Get(xhttp.ContentType))
		noQueries := len(r.URL.RawQuery) == 0
		return ctypeOk && noQueries
	}).HandlerFunc(httpTraceAll(sts.AssumeRoleWithSSO))

	// AssumeRoleWithClientGrants
	stsRouter.Methods(http.MethodPost).HandlerFunc(httpTraceAll(sts.AssumeRoleWithClientGrants)).
		Queries(stsAction, clientGrants).
		Queries(stsVersion, stsAPIVersion).
		Queries(stsToken, "{Token:.*}")

	// AssumeRoleWithWebIdentity
	stsRouter.Methods(http.MethodPost).HandlerFunc(httpTraceAll(sts.AssumeRoleWithWebIdentity)).
		Queries(stsAction, webIdentity).
		Queries(stsVersion, stsAPIVersion).
		Queries(stsWebIdentityToken, "{Token:.*}")

	// AssumeRoleWithLDAPIdentity
	stsRouter.Methods(http.MethodPost).HandlerFunc(httpTraceAll(sts.AssumeRoleWithLDAPIdentity)).
		Queries(stsAction, ldapIdentity).
		Queries(stsVersion, stsAPIVersion).
		Queries(stsLDAPUsername, "{LDAPUsername:.*}").
		Queries(stsLDAPPassword, "{LDAPPassword:.*}")

	// AssumeRoleWithCertificate
	stsRouter.Methods(http.MethodPost).HandlerFunc(httpTraceAll(sts.AssumeRoleWithCertificate)).
		Queries(stsAction, clientCertificate).
		Queries(stsVersion, stsAPIVersion)

	// AssumeRoleWithCustomToken
	stsRouter.Methods(http.MethodPost).HandlerFunc(httpTraceAll(sts.AssumeRoleWithCustomToken)).
		Queries(stsAction, customTokenIdentity).
		Queries(stsVersion, stsAPIVersion)
}

func checkAssumeRoleAuth(ctx context.Context, r *http.Request) (user auth.Credentials, isErrCodeSTS bool, stsErr STSErrorCode) {
	if !isRequestSignatureV4(r) {
		return user, true, ErrSTSAccessDenied
	}

	s3Err := isReqAuthenticated(ctx, r, globalSite.Region, serviceSTS)
	if s3Err != ErrNone {
		return user, false, STSErrorCode(s3Err)
	}

	user, _, s3Err = getReqAccessKeyV4(r, globalSite.Region, serviceSTS)
	if s3Err != ErrNone {
		return user, false, STSErrorCode(s3Err)
	}

	// Temporary credentials or Service accounts cannot generate further temporary credentials.
	if user.IsTemp() || user.IsServiceAccount() {
		return user, true, ErrSTSAccessDenied
	}

	// Session tokens are not allowed in STS AssumeRole requests.
	if getSessionToken(r) != "" {
		return user, true, ErrSTSAccessDenied
	}

	return user, true, ErrSTSNone
}

func parseForm(r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return err
	}
	for k, v := range r.PostForm {
		if _, ok := r.Form[k]; !ok {
			r.Form[k] = v
		}
	}
	return nil
}

// AssumeRole - implementation of AWS STS API AssumeRole to get temporary
// credentials for regular users on Minio.
// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
func (sts *stsAPIHandlers) AssumeRole(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRole")

	claims := make(map[string]interface{})
	defer logger.AuditLog(ctx, w, r, claims)

	// Check auth here (otherwise r.Form will have unexpected values from
	// the call to `parseForm` below), but return failure only after we are
	// able to validate that it is a valid STS request, so that we are able
	// to send an appropriate audit log.
	user, isErrCodeSTS, stsErr := checkAssumeRoleAuth(ctx, r)

	if err := parseForm(r); err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	if r.Form.Get(stsVersion) != stsAPIVersion {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get(stsVersion), stsAPIVersion))
		return
	}

	action := r.Form.Get(stsAction)
	switch action {
	case assumeRole:
	default:
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	ctx = newContext(r, w, action)

	// Validate the authentication result here so that failures will be
	// audit-logged.
	if stsErr != ErrSTSNone {
		writeSTSErrorResponse(ctx, w, isErrCodeSTS, stsErr, nil)
		return
	}

	sessionPolicyStr := r.Form.Get(stsPolicy)
	// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
	// The plain text that you use for both inline and managed session
	// policies shouldn't exceed 2048 characters.
	if len(sessionPolicyStr) > 2048 {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Session policy shouldn't exceed 2048 characters"))
		return
	}

	if len(sessionPolicyStr) > 0 {
		sessionPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(sessionPolicyStr)))
		if err != nil {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
			return
		}

		// Version in policy must not be empty
		if sessionPolicy.Version == "" {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Version cannot be empty expecting '2012-10-17'"))
			return
		}
	}

	duration, err := openid.GetDefaultExpiration(r.Form.Get(stsDurationSeconds))
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	claims[expClaim] = UTCNow().Add(duration).Unix()
	claims[parentClaim] = user.AccessKey

	// Validate that user.AccessKey's policies can be retrieved - it may not
	// be in case the user is disabled.
	if _, err = globalIAMSys.PolicyDBGet(user.AccessKey, false); err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	if len(sessionPolicyStr) > 0 {
		claims[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString([]byte(sessionPolicyStr))
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(claims, secret)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Set the parent of the temporary access key, so that it's access
	// policy is inherited from `user.AccessKey`.
	cred.ParentUser = user.AccessKey

	// Set the newly generated credentials.
	updatedAt, err := globalIAMSys.SetTempUser(ctx, cred.AccessKey, cred, "")
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Call hook for site replication.
	if cred.ParentUser != globalActiveCred.AccessKey {
		if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSTSAcc,
			STSCredential: &madmin.SRSTSCredential{
				AccessKey:    cred.AccessKey,
				SecretKey:    cred.SecretKey,
				SessionToken: cred.SessionToken,
				ParentUser:   cred.ParentUser,
			},
			UpdatedAt: updatedAt,
		}); err != nil {
			logger.LogIf(ctx, err)
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

func (sts *stsAPIHandlers) AssumeRoleWithSSO(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleSSOCommon")

	claims := make(map[string]interface{})
	defer logger.AuditLog(ctx, w, r, claims)

	// Parse the incoming form data.
	if err := parseForm(r); err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	if r.Form.Get(stsVersion) != stsAPIVersion {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		return
	}

	action := r.Form.Get(stsAction)
	switch action {
	case ldapIdentity:
		sts.AssumeRoleWithLDAPIdentity(w, r)
		return
	case clientGrants, webIdentity:
	default:
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	ctx = newContext(r, w, action)

	token := r.Form.Get(stsToken)
	if token == "" {
		token = r.Form.Get(stsWebIdentityToken)
	}

	accessToken := r.Form.Get(stsWebIdentityAccessToken)

	roleArn := openid.DummyRoleARN
	if globalIAMSys.HasRolePolicy() {
		var err error
		roleArnStr := r.Form.Get(stsRoleArn)
		roleArn, _, err = globalIAMSys.GetRolePolicy(roleArnStr)
		if err != nil {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue,
				fmt.Errorf("Error processing %s parameter: %v", stsRoleArn, err))
			return
		}

	}

	// Validate JWT; check clientID in claims matches the one associated with the roleArn
	if err := globalOpenIDConfig.Validate(roleArn, token, accessToken, r.Form.Get(stsDurationSeconds), claims); err != nil {
		switch err {
		case openid.ErrTokenExpired:
			switch action {
			case clientGrants:
				writeSTSErrorResponse(ctx, w, true, ErrSTSClientGrantsExpiredToken, err)
			case webIdentity:
				writeSTSErrorResponse(ctx, w, true, ErrSTSWebIdentityExpiredToken, err)
			}
			return
		case auth.ErrInvalidDuration:
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
			return
		}
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	var policyName string
	if globalIAMSys.HasRolePolicy() {
		// If roleArn is used, we set it as a claim, and use the
		// associated policy when credentials are used.
		claims[roleArnClaim] = roleArn.String()
	} else {
		// If no role policy is configured, then we use claims from the
		// JWT. This is a MinIO STS API specific value, this value
		// should be set and configured on your identity provider as
		// part of JWT custom claims.
		policySet, ok := iampolicy.GetPoliciesFromClaims(claims, iamPolicyClaimNameOpenID())
		policies := strings.Join(policySet.ToSlice(), ",")
		if ok {
			policyName = globalIAMSys.CurrentPolicies(policies)
		}

		if newGlobalAuthZPluginFn() == nil {
			if !ok {
				writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue,
					fmt.Errorf("%s claim missing from the JWT token, credentials will not be generated", iamPolicyClaimNameOpenID()))
				return
			} else if policyName == "" {
				writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue,
					fmt.Errorf("None of the given policies (`%s`) are defined, credentials will not be generated", policies))
				return
			}
		}
		claims[iamPolicyClaimNameOpenID()] = policyName
	}

	sessionPolicyStr := r.Form.Get(stsPolicy)
	// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
	// The plain text that you use for both inline and managed session
	// policies shouldn't exceed 2048 characters.
	if len(sessionPolicyStr) > 2048 {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Session policy should not exceed 2048 characters"))
		return
	}

	if len(sessionPolicyStr) > 0 {
		sessionPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(sessionPolicyStr)))
		if err != nil {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
			return
		}

		// Version in policy must not be empty
		if sessionPolicy.Version == "" {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Invalid session policy version"))
			return
		}

		claims[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString([]byte(sessionPolicyStr))
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(claims, secret)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// https://openid.net/specs/openid-connect-core-1_0.html#ClaimStability
	// claim is only considered stable when subject and iss are used together
	// this is to ensure that ParentUser doesn't change and we get to use
	// parentUser as per the requirements for service accounts for OpenID
	// based logins.
	var subFromToken string
	if v, ok := claims[subClaim]; ok {
		subFromToken, _ = v.(string)
	}

	if subFromToken == "" {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue,
			errors.New("STS JWT Token has `sub` claim missing, `sub` claim is mandatory"))
		return
	}

	var issFromToken string
	if v, ok := claims[issClaim]; ok {
		issFromToken, _ = v.(string)
	}

	// Since issFromToken can have `/` characters (it is typically the
	// provider URL), we hash and encode it to base64 here. This is needed
	// because there will be a policy mapping stored on drives whose
	// filename is this parentUser: therefore, it needs to have only valid
	// filename characters and needs to have bounded length.
	{
		h := sha256.New()
		h.Write([]byte("openid:" + subFromToken + ":" + issFromToken))
		bs := h.Sum(nil)
		cred.ParentUser = base64.RawURLEncoding.EncodeToString(bs)
	}

	// Set the newly generated credentials.
	updatedAt, err := globalIAMSys.SetTempUser(ctx, cred.AccessKey, cred, policyName)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Call hook for site replication.
	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemSTSAcc,
		STSCredential: &madmin.SRSTSCredential{
			AccessKey:           cred.AccessKey,
			SecretKey:           cred.SecretKey,
			SessionToken:        cred.SessionToken,
			ParentUser:          cred.ParentUser,
			ParentPolicyMapping: policyName,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		logger.LogIf(ctx, err)
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
	sts.AssumeRoleWithSSO(w, r)
}

// AssumeRoleWithClientGrants - implementation of AWS STS extension API supporting
// OAuth2.0 client credential grants.
//
// Eg:-
//    $ curl https://minio:9000/?Action=AssumeRoleWithClientGrants&Token=<jwt>
func (sts *stsAPIHandlers) AssumeRoleWithClientGrants(w http.ResponseWriter, r *http.Request) {
	sts.AssumeRoleWithSSO(w, r)
}

// AssumeRoleWithLDAPIdentity - implements user auth against LDAP server
func (sts *stsAPIHandlers) AssumeRoleWithLDAPIdentity(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleWithLDAPIdentity")

	claims := make(map[string]interface{})
	defer logger.AuditLog(ctx, w, r, claims, stsLDAPPassword)

	// Parse the incoming form data.
	if err := parseForm(r); err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	if r.Form.Get(stsVersion) != stsAPIVersion {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter,
			fmt.Errorf("Invalid STS API version %s, expecting %s", r.Form.Get("Version"), stsAPIVersion))
		return
	}

	ldapUsername := r.Form.Get(stsLDAPUsername)
	ldapPassword := r.Form.Get(stsLDAPPassword)

	if ldapUsername == "" || ldapPassword == "" {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, fmt.Errorf("LDAPUsername and LDAPPassword cannot be empty"))
		return
	}

	action := r.Form.Get(stsAction)
	switch action {
	case ldapIdentity:
	default:
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	sessionPolicyStr := r.Form.Get(stsPolicy)
	// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
	// The plain text that you use for both inline and managed session
	// policies shouldn't exceed 2048 characters.
	if len(sessionPolicyStr) > 2048 {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Session policy should not exceed 2048 characters"))
		return
	}

	if len(sessionPolicyStr) > 0 {
		sessionPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(sessionPolicyStr)))
		if err != nil {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
			return
		}

		// Version in policy must not be empty
		if sessionPolicy.Version == "" {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Version needs to be specified in session policy"))
			return
		}
	}

	ldapUserDN, groupDistNames, err := globalLDAPConfig.Bind(ldapUsername, ldapPassword)
	if err != nil {
		err = fmt.Errorf("LDAP server error: %w", err)
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	// Check if this user or their groups have a policy applied.
	ldapPolicies, _ := globalIAMSys.PolicyDBGet(ldapUserDN, false, groupDistNames...)
	if len(ldapPolicies) == 0 && newGlobalAuthZPluginFn() == nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue,
			fmt.Errorf("expecting a policy to be set for user `%s` or one of their groups: `%s` - rejecting this request",
				ldapUserDN, strings.Join(groupDistNames, "`,`")))
		return
	}

	expiryDur, err := globalLDAPConfig.GetExpiryDuration(r.Form.Get(stsDurationSeconds))
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	claims[expClaim] = UTCNow().Add(expiryDur).Unix()
	claims[ldapUser] = ldapUserDN
	claims[ldapUserN] = ldapUsername

	if len(sessionPolicyStr) > 0 {
		claims[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString([]byte(sessionPolicyStr))
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(claims, secret)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Set the parent of the temporary access key, this is useful
	// in obtaining service accounts by this cred.
	cred.ParentUser = ldapUserDN

	// Set this value to LDAP groups, LDAP user can be part
	// of large number of groups
	cred.Groups = groupDistNames

	// Set the newly generated credentials, policyName is empty on purpose
	// LDAP policies are applied automatically using their ldapUser, ldapGroups
	// mapping.
	updatedAt, err := globalIAMSys.SetTempUser(ctx, cred.AccessKey, cred, "")
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Call hook for site replication.
	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemSTSAcc,
		STSCredential: &madmin.SRSTSCredential{
			AccessKey:    cred.AccessKey,
			SecretKey:    cred.SecretKey,
			SessionToken: cred.SessionToken,
			ParentUser:   cred.ParentUser,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		logger.LogIf(ctx, err)
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

// AssumeRoleWithCertificate implements user authentication with client certificates.
// It verifies the client-provided X.509 certificate, maps the certificate to an S3 policy
// and returns temp. S3 credentials to the client.
//
// API endpoint: https://minio:9000?Action=AssumeRoleWithCertificate&Version=2011-06-15
func (sts *stsAPIHandlers) AssumeRoleWithCertificate(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleWithCertificate")

	claims := make(map[string]interface{})
	defer logger.AuditLog(ctx, w, r, claims)

	if !globalSTSTLSConfig.Enabled {
		writeSTSErrorResponse(ctx, w, true, ErrSTSNotInitialized, errors.New("STS API 'AssumeRoleWithCertificate' is disabled"))
		return
	}

	// We have to establish a TLS connection and the
	// client must provide exactly one client certificate.
	// Otherwise, we don't have a certificate to verify or
	// the policy lookup would ambigious.
	if r.TLS == nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInsecureConnection, errors.New("No TLS connection attempt"))
		return
	}

	// A client may send a certificate chain such that we end up
	// with multiple peer certificates. However, we can only accept
	// a single client certificate. Otherwise, the certificate to
	// policy mapping would be ambigious.
	// However, we can filter all CA certificates and only check
	// whether they client has sent exactly one (non-CA) leaf certificate.
	peerCertificates := make([]*x509.Certificate, 0, len(r.TLS.PeerCertificates))
	for _, cert := range r.TLS.PeerCertificates {
		if cert.IsCA {
			continue
		}
		peerCertificates = append(peerCertificates, cert)
	}
	r.TLS.PeerCertificates = peerCertificates

	// Now, we have to check that the client has provided exactly one leaf
	// certificate that we can map to a policy.
	if len(r.TLS.PeerCertificates) == 0 {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, errors.New("No client certificate provided"))
		return
	}
	if len(r.TLS.PeerCertificates) > 1 {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, errors.New("More than one client certificate provided"))
		return
	}

	certificate := r.TLS.PeerCertificates[0]
	if !globalSTSTLSConfig.InsecureSkipVerify { // Verify whether the client certificate has been issued by a trusted CA.
		_, err := certificate.Verify(x509.VerifyOptions{
			KeyUsages: []x509.ExtKeyUsage{
				x509.ExtKeyUsageClientAuth,
			},
			Roots: globalRootCAs,
		})
		if err != nil {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidClientCertificate, err)
			return
		}
	} else {
		// Technically, there is no security argument for verifying the key usage
		// when we don't verify that the certificate has been issued by a trusted CA.
		// Any client can create a certificate with arbitrary key usage settings.
		//
		// However, this check ensures that a certificate with an invalid key usage
		// gets rejected even when we skip certificate verification. This helps
		// clients detect malformed certificates during testing instead of e.g.
		// a self-signed certificate that works while a comparable certificate
		// issued by a trusted CA fails due to the MinIO server being less strict
		// w.r.t. key usage verification.
		//
		// Basically, MinIO is more consistent (from a client perspective) when
		// we verify the key usage all the time.
		var validKeyUsage bool
		for _, usage := range certificate.ExtKeyUsage {
			if usage == x509.ExtKeyUsageAny || usage == x509.ExtKeyUsageClientAuth {
				validKeyUsage = true
				break
			}
		}
		if !validKeyUsage {
			writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, errors.New("certificate is not valid for client authentication"))
			return
		}
	}

	// We map the X.509 subject common name to the policy. So, a client
	// with the common name "foo" will be associated with the policy "foo".
	// Other mapping functions - e.g. public-key hash based mapping - are
	// possible but not implemented.
	//
	// Group mapping is not possible with standard X.509 certificates.
	if certificate.Subject.CommonName == "" {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, errors.New("certificate subject CN cannot be empty"))
		return
	}

	expiry, err := globalSTSTLSConfig.GetExpiryDuration(r.Form.Get(stsDurationSeconds))
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSMissingParameter, err)
		return
	}

	// We set the expiry of the temp. credentials to the minimum of the
	// configured expiry and the duration until the certificate itself
	// expires.
	// We must not issue credentials that out-live the certificate.
	if validUntil := time.Until(certificate.NotAfter); validUntil < expiry {
		expiry = validUntil
	}

	// Associate any service accounts to the certificate CN
	parentUser := "tls:" + certificate.Subject.CommonName

	claims[expClaim] = UTCNow().Add(expiry).Unix()
	claims[subClaim] = certificate.Subject.CommonName
	claims[audClaim] = certificate.Subject.Organization
	claims[issClaim] = certificate.Issuer.CommonName
	claims[parentClaim] = parentUser

	tmpCredentials, err := auth.GetNewCredentialsWithMetadata(claims, globalActiveCred.SecretKey)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	tmpCredentials.ParentUser = parentUser
	policyName := certificate.Subject.CommonName
	updatedAt, err := globalIAMSys.SetTempUser(ctx, tmpCredentials.AccessKey, tmpCredentials, policyName)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Call hook for site replication.
	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemSTSAcc,
		STSCredential: &madmin.SRSTSCredential{
			AccessKey:           tmpCredentials.AccessKey,
			SecretKey:           tmpCredentials.SecretKey,
			SessionToken:        tmpCredentials.SessionToken,
			ParentUser:          tmpCredentials.ParentUser,
			ParentPolicyMapping: policyName,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		logger.LogIf(ctx, err)
	}

	response := new(AssumeRoleWithCertificateResponse)
	response.Result.Credentials = tmpCredentials
	response.Metadata.RequestID = w.Header().Get(xhttp.AmzRequestID)
	writeSuccessResponseXML(w, encodeResponse(response))
}

// AssumeRoleWithCustomToken implements user authentication with custom tokens.
// These tokens are opaque to MinIO and are verified by a configured (external)
// Identity Management Plugin.
//
// API endpoint: https://minio:9000?Action=AssumeRoleWithCustomToken&Token=xxx
func (sts *stsAPIHandlers) AssumeRoleWithCustomToken(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AssumeRoleWithCustomToken")

	claims := make(map[string]interface{})
	defer logger.AuditLog(ctx, w, r, claims)

	if globalAuthNPlugin == nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSNotInitialized, errors.New("STS API 'AssumeRoleWithCustomToken' is disabled"))
		return
	}

	action := r.Form.Get(stsAction)
	if action != customTokenIdentity {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Unsupported action %s", action))
		return
	}

	token := r.Form.Get(stsToken)
	if token == "" {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Invalid empty `Token` parameter provided"))
		return
	}

	durationParam := r.Form.Get(stsDurationSeconds)
	var requestedDuration int
	if durationParam != "" {
		var err error
		requestedDuration, err = strconv.Atoi(durationParam)
		if err != nil {
			writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, fmt.Errorf("Invalid requested duration: %s", durationParam))
			return
		}
	}

	roleArnStr := r.Form.Get(stsRoleArn)
	roleArn, _, err := globalIAMSys.GetRolePolicy(roleArnStr)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue,
			fmt.Errorf("Error processing parameter %s: %v", stsRoleArn, err))
		return
	}

	res, err := globalAuthNPlugin.Authenticate(roleArn, token)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInvalidParameterValue, err)
		return
	}

	// If authentication failed, return the error message to the user.
	if res.Failure != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSUpstreamError, errors.New(res.Failure.Reason))
		return
	}

	// It is required that parent user be set.
	if res.Success.User == "" {
		writeSTSErrorResponse(ctx, w, true, ErrSTSUpstreamError, errors.New("A valid user was not returned by the authenticator."))
		return
	}

	// Expiry is set as minimum of requested value and value allowed by auth
	// plugin.
	expiry := res.Success.MaxValiditySeconds
	if durationParam != "" && requestedDuration < expiry {
		expiry = requestedDuration
	}

	parentUser := "custom:" + res.Success.User

	// metadata map
	claims[expClaim] = UTCNow().Add(time.Duration(expiry) * time.Second).Unix()
	claims[subClaim] = parentUser
	claims[roleArnClaim] = roleArn.String()
	claims[parentClaim] = parentUser

	// Add all other claims from the plugin **without** replacing any
	// existing claims.
	for k, v := range res.Success.Claims {
		if _, ok := claims[k]; !ok {
			claims[k] = v
		}
	}

	tmpCredentials, err := auth.GetNewCredentialsWithMetadata(claims, globalActiveCred.SecretKey)
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	tmpCredentials.ParentUser = parentUser
	updatedAt, err := globalIAMSys.SetTempUser(ctx, tmpCredentials.AccessKey, tmpCredentials, "")
	if err != nil {
		writeSTSErrorResponse(ctx, w, true, ErrSTSInternalError, err)
		return
	}

	// Call hook for site replication.
	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemSTSAcc,
		STSCredential: &madmin.SRSTSCredential{
			AccessKey:    tmpCredentials.AccessKey,
			SecretKey:    tmpCredentials.SecretKey,
			SessionToken: tmpCredentials.SessionToken,
			ParentUser:   tmpCredentials.ParentUser,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	response := new(AssumeRoleWithCustomTokenResponse)
	response.Result.Credentials = tmpCredentials
	response.Result.AssumedUser = parentUser
	response.Metadata.RequestID = w.Header().Get(xhttp.AmzRequestID)
	writeSuccessResponseXML(w, encodeResponse(response))
}
