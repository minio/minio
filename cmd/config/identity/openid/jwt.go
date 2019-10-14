/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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

package openid

import (
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
)

// Config - OpenID Config
// RSA authentication target arguments
type Config struct {
	JWKS struct {
		URL *xnet.URL `json:"url"`
	} `json:"jwks"`
	URL          *xnet.URL `json:"url,omitempty"`
	ClaimPrefix  string    `json:"claimPrefix,omitempty"`
	DiscoveryDoc DiscoveryDoc
	publicKeys   map[string]crypto.PublicKey
	transport    *http.Transport
	closeRespFn  func(io.ReadCloser)
}

// PopulatePublicKey - populates a new publickey from the JWKS URL.
func (r *Config) PopulatePublicKey() error {
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

func expToInt64(expI interface{}) (expAt int64, err error) {
	switch exp := expI.(type) {
	case float64:
		expAt = int64(exp)
	case int64:
		expAt = exp
	case json.Number:
		expAt, err = exp.Int64()
		if err != nil {
			return 0, err
		}
	default:
		return 0, ErrInvalidDuration
	}
	return expAt, nil
}

// GetDefaultExpiration - returns the expiration seconds expected.
func GetDefaultExpiration(dsecs string) (time.Duration, error) {
	defaultExpiryDuration := time.Duration(60) * time.Minute // Defaults to 1hr.
	if dsecs != "" {
		expirySecs, err := strconv.ParseInt(dsecs, 10, 64)
		if err != nil {
			return 0, ErrInvalidDuration
		}
		// The duration, in seconds, of the role session.
		// The value can range from 900 seconds (15 minutes)
		// to 12 hours.
		if expirySecs < 900 || expirySecs > 43200 {
			return 0, ErrInvalidDuration
		}

		defaultExpiryDuration = time.Duration(expirySecs) * time.Second
	}
	return defaultExpiryDuration, nil
}

// Validate - validates the access token.
func (p *JWT) Validate(token, dsecs string) (map[string]interface{}, error) {
	jp := new(jwtgo.Parser)
	jp.ValidMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}

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

	expAt, err := expToInt64(claims["exp"])
	if err != nil {
		return nil, err
	}

	defaultExpiryDuration, err := GetDefaultExpiration(dsecs)
	if err != nil {
		return nil, err
	}

	if time.Unix(expAt, 0).UTC().Sub(time.Now().UTC()) < defaultExpiryDuration {
		defaultExpiryDuration = time.Unix(expAt, 0).UTC().Sub(time.Now().UTC())
	}

	expiry := time.Now().UTC().Add(defaultExpiryDuration).Unix()
	if expAt < expiry {
		claims["exp"] = strconv.FormatInt(expAt, 64)
	}

	return claims, nil

}

// ID returns the provider name and authentication type.
func (p *JWT) ID() ID {
	return "jwt"
}

// OpenID keys and envs.
const (
	JwksURL     = "jwks_url"
	ConfigURL   = "config_url"
	ClaimPrefix = "claim_prefix"

	EnvIdentityOpenIDJWKSURL     = "MINIO_IDENTITY_OPENID_JWKS_URL"
	EnvIdentityOpenIDURL         = "MINIO_IDENTITY_OPENID_CONFIG_URL"
	EnvIdentityOpenIDClaimPrefix = "MINIO_IDENTITY_OPENID_CLAIM_PREFIX"
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
		config.State:   config.StateOff,
		config.Comment: "This is a default OpenID configuration",
		JwksURL:        "",
		ConfigURL:      "",
		ClaimPrefix:    "",
	}
)

// LookupConfig lookup jwks from config, override with any ENVs.
func LookupConfig(kv config.KVS, transport *http.Transport, closeRespFn func(io.ReadCloser)) (c Config, err error) {
	if err = config.CheckValidKeys(config.IdentityOpenIDSubSys, kv, DefaultKVS); err != nil {
		return c, err
	}

	stateBool, err := config.ParseBool(kv.Get(config.State))
	if err != nil {
		return c, err
	}
	if !stateBool {
		return c, nil
	}

	c = Config{
		ClaimPrefix: env.Get(EnvIdentityOpenIDClaimPrefix, kv.Get(ClaimPrefix)),
		publicKeys:  make(map[string]crypto.PublicKey),
		transport:   transport,
		closeRespFn: closeRespFn,
	}

	jwksURL := env.Get(EnvIamJwksURL, "") // Legacy
	if jwksURL == "" {
		jwksURL = env.Get(EnvIdentityOpenIDJWKSURL, kv.Get(JwksURL))
	}

	configURL := env.Get(EnvIdentityOpenIDURL, kv.Get(ConfigURL))
	if configURL != "" {
		c.URL, err = xnet.ParseURL(configURL)
		if err != nil {
			return c, err
		}
		c.DiscoveryDoc, err = parseDiscoveryDoc(c.URL, transport, closeRespFn)
		if err != nil {
			return c, err
		}
	}
	if jwksURL == "" {
		// Fallback to discovery document jwksURL
		jwksURL = c.DiscoveryDoc.JwksURI
	}
	if jwksURL != "" {
		c.JWKS.URL, err = xnet.ParseURL(jwksURL)
		if err != nil {
			return c, err
		}
		if err = c.PopulatePublicKey(); err != nil {
			return c, err
		}
	}
	return c, nil
}

// NewJWT - initialize new jwt authenticator.
func NewJWT(c Config) *JWT {
	return &JWT{c}
}
