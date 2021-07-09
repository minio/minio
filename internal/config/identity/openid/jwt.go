// Copyright (c) 2015-2021 MinIO, Inc.
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

package openid

import (
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	jwtgo "github.com/golang-jwt/jwt"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/identity/openid/provider"
	"github.com/minio/pkg/env"
	iampolicy "github.com/minio/pkg/iam/policy"
	xnet "github.com/minio/pkg/net"
)

// Config - OpenID Config
// RSA authentication target arguments
type Config struct {
	JWKS struct {
		URL *xnet.URL `json:"url"`
	} `json:"jwks"`
	URL          *xnet.URL `json:"url,omitempty"`
	ClaimPrefix  string    `json:"claimPrefix,omitempty"`
	ClaimName    string    `json:"claimName,omitempty"`
	RedirectURI  string    `json:"redirectURI,omitempty"`
	DiscoveryDoc DiscoveryDoc
	ClientID     string
	ClientSecret string
	provider     provider.Provider
	publicKeys   map[string]crypto.PublicKey
	transport    *http.Transport
	closeRespFn  func(io.ReadCloser)
	mutex        *sync.Mutex
}

// LookupUser lookup userid for the provider
func (r *Config) LookupUser(userid string) (provider.User, error) {
	if r.provider != nil {
		user, err := r.provider.LookupUser(userid)
		if err != nil && err != provider.ErrAccessTokenExpired {
			return user, err
		}
		if err == provider.ErrAccessTokenExpired {
			if err = r.provider.LoginWithClientID(r.ClientID, r.ClientSecret); err != nil {
				return user, err
			}
			user, err = r.provider.LookupUser(userid)
		}
		return user, err
	}
	// Without any specific logic for a provider, all accounts
	// are always enabled.
	return provider.User{ID: userid, Enabled: true}, nil
}

const (
	keyCloakVendor = "keycloak"
)

// InitializeProvider initializes if any additional vendor specific
// information was provided, initialization will return an error
// initial login fails.
func (r *Config) InitializeProvider(kvs config.KVS) error {
	vendor := env.Get(EnvIdentityOpenIDVendor, kvs.Get(Vendor))
	if vendor == "" {
		return nil
	}
	switch vendor {
	case keyCloakVendor:
		adminURL := env.Get(EnvIdentityOpenIDKeyCloakAdminURL, kvs.Get(KeyCloakAdminURL))
		realm := env.Get(EnvIdentityOpenIDKeyCloakRealm, kvs.Get(KeyCloakRealm))
		return r.InitializeKeycloakProvider(adminURL, realm)
	default:
		return fmt.Errorf("Unsupport vendor %s", keyCloakVendor)
	}
}

// ProviderEnabled returns true if any vendor specific provider is enabled.
func (r *Config) ProviderEnabled() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.provider != nil
}

// InitializeKeycloakProvider - initializes keycloak provider
func (r *Config) InitializeKeycloakProvider(adminURL, realm string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var err error
	r.provider, err = provider.KeyCloak(
		provider.WithAdminURL(adminURL),
		provider.WithOpenIDConfig(provider.DiscoveryDoc(r.DiscoveryDoc)),
		provider.WithTransport(r.transport),
		provider.WithRealm(realm),
	)
	return err
}

// PopulatePublicKey - populates a new publickey from the JWKS URL.
func (r *Config) PopulatePublicKey() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.JWKS.URL == nil || r.JWKS.URL.String() == "" {
		return nil
	}
	transport := http.DefaultTransport
	if r.transport != nil {
		transport = r.transport
	}
	client := &http.Client{
		Transport: transport,
	}
	resp, err := client.Get(r.JWKS.URL.String())
	if err != nil {
		return err
	}
	defer r.closeRespFn(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	var jwk JWKS
	if err = json.NewDecoder(resp.Body).Decode(&jwk); err != nil {
		return err
	}

	for _, key := range jwk.Keys {
		r.publicKeys[key.Kid], err = key.DecodePublicKey()
		if err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalJSON - decodes JSON data.
func (r *Config) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subConfig Config
	var sr subConfig

	if err := json.Unmarshal(data, &sr); err != nil {
		return err
	}

	ar := Config(sr)
	if ar.JWKS.URL == nil || ar.JWKS.URL.String() == "" {
		*r = ar
		return nil
	}

	*r = ar
	return nil
}

// JWT - rs client grants provider details.
type JWT struct {
	Config
}

// GetDefaultExpiration - returns the expiration seconds expected.
func GetDefaultExpiration(dsecs string) (time.Duration, error) {
	defaultExpiryDuration := time.Duration(60) * time.Minute // Defaults to 1hr.
	if dsecs != "" {
		expirySecs, err := strconv.ParseInt(dsecs, 10, 64)
		if err != nil {
			return 0, auth.ErrInvalidDuration
		}

		// The duration, in seconds, of the role session.
		// The value can range from 900 seconds (15 minutes)
		// up to 7 days.
		if expirySecs < 900 || expirySecs > 604800 {
			return 0, auth.ErrInvalidDuration
		}

		defaultExpiryDuration = time.Duration(expirySecs) * time.Second
	}
	return defaultExpiryDuration, nil
}

func updateClaimsExpiry(dsecs string, claims map[string]interface{}) error {
	expStr := claims["exp"]
	if expStr == "" {
		return ErrTokenExpired
	}

	// No custom duration requested, the claims can be used as is.
	if dsecs == "" {
		return nil
	}

	expAt, err := auth.ExpToInt64(expStr)
	if err != nil {
		return err
	}

	defaultExpiryDuration, err := GetDefaultExpiration(dsecs)
	if err != nil {
		return err
	}

	// Verify if JWT expiry is lesser than default expiry duration,
	// if that is the case then set the default expiration to be
	// from the JWT expiry claim.
	if time.Unix(expAt, 0).UTC().Sub(time.Now().UTC()) < defaultExpiryDuration {
		defaultExpiryDuration = time.Unix(expAt, 0).UTC().Sub(time.Now().UTC())
	} // else honor the specified expiry duration.

	expiry := time.Now().UTC().Add(defaultExpiryDuration).Unix()
	claims["exp"] = strconv.FormatInt(expiry, 10) // update with new expiry.
	return nil
}

// Validate - validates the access token.
func (p *JWT) Validate(token, dsecs string) (map[string]interface{}, error) {
	jp := new(jwtgo.Parser)
	jp.ValidMethods = []string{
		"RS256", "RS384", "RS512", "ES256", "ES384", "ES512",
		"RS3256", "RS3384", "RS3512", "ES3256", "ES3384", "ES3512",
	}

	keyFuncCallback := func(jwtToken *jwtgo.Token) (interface{}, error) {
		kid, ok := jwtToken.Header["kid"].(string)
		if !ok {
			return nil, fmt.Errorf("Invalid kid value %v", jwtToken.Header["kid"])
		}
		return p.publicKeys[kid], nil
	}

	var claims jwtgo.MapClaims
	jwtToken, err := jp.ParseWithClaims(token, &claims, keyFuncCallback)
	if err != nil {
		// Re-populate the public key in-case the JWKS
		// pubkeys are refreshed
		if err = p.PopulatePublicKey(); err != nil {
			return nil, err
		}
		jwtToken, err = jwtgo.ParseWithClaims(token, &claims, keyFuncCallback)
		if err != nil {
			return nil, err
		}
	}

	if !jwtToken.Valid {
		return nil, ErrTokenExpired
	}

	if err = updateClaimsExpiry(dsecs, claims); err != nil {
		return nil, err
	}

	return claims, nil
}

// ID returns the provider name and authentication type.
func (p *JWT) ID() ID {
	return "jwt"
}

// OpenID keys and envs.
const (
	JwksURL      = "jwks_url"
	ConfigURL    = "config_url"
	ClaimName    = "claim_name"
	ClaimPrefix  = "claim_prefix"
	ClientID     = "client_id"
	ClientSecret = "client_secret"
	Vendor       = "vendor"
	Scopes       = "scopes"
	RedirectURI  = "redirect_uri"

	// Vendor specific ENV only enabled if the Vendor matches == "vendor"
	KeyCloakRealm    = "keycloak_realm"
	KeyCloakAdminURL = "keycloak_admin_url"

	EnvIdentityOpenIDVendor       = "MINIO_IDENTITY_OPENID_VENDOR"
	EnvIdentityOpenIDClientID     = "MINIO_IDENTITY_OPENID_CLIENT_ID"
	EnvIdentityOpenIDClientSecret = "MINIO_IDENTITY_OPENID_CLIENT_SECRET"
	EnvIdentityOpenIDJWKSURL      = "MINIO_IDENTITY_OPENID_JWKS_URL"
	EnvIdentityOpenIDURL          = "MINIO_IDENTITY_OPENID_CONFIG_URL"
	EnvIdentityOpenIDClaimName    = "MINIO_IDENTITY_OPENID_CLAIM_NAME"
	EnvIdentityOpenIDClaimPrefix  = "MINIO_IDENTITY_OPENID_CLAIM_PREFIX"
	EnvIdentityOpenIDRedirectURI  = "MINIO_IDENTITY_OPENID_REDIRECT_URI"
	EnvIdentityOpenIDScopes       = "MINIO_IDENTITY_OPENID_SCOPES"

	// Vendor specific ENVs only enabled if the Vendor matches == "vendor"
	EnvIdentityOpenIDKeyCloakRealm    = "MINIO_IDENTITY_OPENID_KEYCLOAK_REALM"
	EnvIdentityOpenIDKeyCloakAdminURL = "MINIO_IDENTITY_OPENID_KEYCLOAK_ADMIN_URL"
)

// DiscoveryDoc - parses the output from openid-configuration
// for example https://accounts.google.com/.well-known/openid-configuration
type DiscoveryDoc struct {
	Issuer                           string   `json:"issuer,omitempty"`
	AuthEndpoint                     string   `json:"authorization_endpoint,omitempty"`
	TokenEndpoint                    string   `json:"token_endpoint,omitempty"`
	UserInfoEndpoint                 string   `json:"userinfo_endpoint,omitempty"`
	RevocationEndpoint               string   `json:"revocation_endpoint,omitempty"`
	JwksURI                          string   `json:"jwks_uri,omitempty"`
	ResponseTypesSupported           []string `json:"response_types_supported,omitempty"`
	SubjectTypesSupported            []string `json:"subject_types_supported,omitempty"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported,omitempty"`
	ScopesSupported                  []string `json:"scopes_supported,omitempty"`
	TokenEndpointAuthMethods         []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	ClaimsSupported                  []string `json:"claims_supported,omitempty"`
	CodeChallengeMethodsSupported    []string `json:"code_challenge_methods_supported,omitempty"`
}

func parseDiscoveryDoc(u *xnet.URL, transport *http.Transport, closeRespFn func(io.ReadCloser)) (DiscoveryDoc, error) {
	d := DiscoveryDoc{}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return d, err
	}
	clnt := http.Client{
		Transport: transport,
	}
	resp, err := clnt.Do(req)
	if err != nil {
		clnt.CloseIdleConnections()
		return d, err
	}
	defer closeRespFn(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return d, err
	}
	dec := json.NewDecoder(resp.Body)
	if err = dec.Decode(&d); err != nil {
		return d, err
	}
	return d, nil
}

// DefaultKVS - default config for OpenID config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   ConfigURL,
			Value: "",
		},
		config.KV{
			Key:   ClientID,
			Value: "",
		},
		config.KV{
			Key:   ClientSecret,
			Value: "",
		},
		config.KV{
			Key:   ClaimName,
			Value: iampolicy.PolicyName,
		},
		config.KV{
			Key:   ClaimPrefix,
			Value: "",
		},
		config.KV{
			Key:   RedirectURI,
			Value: "",
		},
		config.KV{
			Key:   Scopes,
			Value: "",
		},
		config.KV{
			Key:   JwksURL,
			Value: "",
		},
	}
)

// Enabled returns if jwks is enabled.
func Enabled(kvs config.KVS) bool {
	return kvs.Get(JwksURL) != ""
}

// LookupConfig lookup jwks from config, override with any ENVs.
func LookupConfig(kvs config.KVS, transport *http.Transport, closeRespFn func(io.ReadCloser)) (c Config, err error) {
	if err = config.CheckValidKeys(config.IdentityOpenIDSubSys, kvs, DefaultKVS); err != nil {
		return c, err
	}

	jwksURL := env.Get(EnvIamJwksURL, "") // Legacy
	if jwksURL == "" {
		jwksURL = env.Get(EnvIdentityOpenIDJWKSURL, kvs.Get(JwksURL))
	}

	c = Config{
		ClaimName:    env.Get(EnvIdentityOpenIDClaimName, kvs.Get(ClaimName)),
		ClaimPrefix:  env.Get(EnvIdentityOpenIDClaimPrefix, kvs.Get(ClaimPrefix)),
		RedirectURI:  env.Get(EnvIdentityOpenIDRedirectURI, kvs.Get(RedirectURI)),
		publicKeys:   make(map[string]crypto.PublicKey),
		ClientID:     env.Get(EnvIdentityOpenIDClientID, kvs.Get(ClientID)),
		ClientSecret: env.Get(EnvIdentityOpenIDClientSecret, kvs.Get(ClientSecret)),
		transport:    transport,
		closeRespFn:  closeRespFn,
		mutex:        &sync.Mutex{}, // allocate for copying
	}

	configURL := env.Get(EnvIdentityOpenIDURL, kvs.Get(ConfigURL))
	if configURL != "" {
		c.URL, err = xnet.ParseHTTPURL(configURL)
		if err != nil {
			return c, err
		}
		c.DiscoveryDoc, err = parseDiscoveryDoc(c.URL, transport, closeRespFn)
		if err != nil {
			return c, err
		}
	}

	if scopeList := env.Get(EnvIdentityOpenIDScopes, kvs.Get(Scopes)); scopeList != "" {
		var scopes []string
		for _, scope := range strings.Split(scopeList, ",") {
			scope = strings.TrimSpace(scope)
			if scope == "" {
				return c, config.Errorf("empty scope value is not allowed '%s', please refer to our documentation", scopeList)
			}
			scopes = append(scopes, scope)
		}
		// Replace the discovery document scopes by client customized scopes.
		c.DiscoveryDoc.ScopesSupported = scopes
	}

	if c.ClaimName == "" {
		c.ClaimName = iampolicy.PolicyName
	}

	if jwksURL == "" {
		// Fallback to discovery document jwksURL
		jwksURL = c.DiscoveryDoc.JwksURI
	}

	if jwksURL == "" {
		return c, nil
	}

	c.JWKS.URL, err = xnet.ParseHTTPURL(jwksURL)
	if err != nil {
		return c, err
	}

	if err = c.PopulatePublicKey(); err != nil {
		return c, err
	}

	if err = c.InitializeProvider(kvs); err != nil {
		return c, err
	}

	return c, nil
}

// NewJWT - initialize new jwt authenticator.
func NewJWT(c Config) *JWT {
	return &JWT{c}
}
