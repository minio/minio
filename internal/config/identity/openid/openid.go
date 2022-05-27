// Copyright (c) 2015-2022 MinIO, Inc.
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
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/arn"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/identity/openid/provider"
	"github.com/minio/minio/internal/hash/sha256"
	"github.com/minio/pkg/env"
	iampolicy "github.com/minio/pkg/iam/policy"
	xnet "github.com/minio/pkg/net"
)

// OpenID keys and envs.
const (
	JwksURL       = "jwks_url"
	ConfigURL     = "config_url"
	ClaimName     = "claim_name"
	ClaimUserinfo = "claim_userinfo"
	ClaimPrefix   = "claim_prefix"
	ClientID      = "client_id"
	ClientSecret  = "client_secret"
	RolePolicy    = "role_policy"
	DisplayName   = "display_name"

	Vendor             = "vendor"
	Scopes             = "scopes"
	RedirectURI        = "redirect_uri"
	RedirectURIDynamic = "redirect_uri_dynamic"

	// Vendor specific ENV only enabled if the Vendor matches == "vendor"
	KeyCloakRealm    = "keycloak_realm"
	KeyCloakAdminURL = "keycloak_admin_url"

	EnvIdentityOpenIDEnable             = "MINIO_IDENTITY_OPENID_ENABLE"
	EnvIdentityOpenIDVendor             = "MINIO_IDENTITY_OPENID_VENDOR"
	EnvIdentityOpenIDClientID           = "MINIO_IDENTITY_OPENID_CLIENT_ID"
	EnvIdentityOpenIDClientSecret       = "MINIO_IDENTITY_OPENID_CLIENT_SECRET"
	EnvIdentityOpenIDURL                = "MINIO_IDENTITY_OPENID_CONFIG_URL"
	EnvIdentityOpenIDClaimName          = "MINIO_IDENTITY_OPENID_CLAIM_NAME"
	EnvIdentityOpenIDClaimUserInfo      = "MINIO_IDENTITY_OPENID_CLAIM_USERINFO"
	EnvIdentityOpenIDClaimPrefix        = "MINIO_IDENTITY_OPENID_CLAIM_PREFIX"
	EnvIdentityOpenIDRolePolicy         = "MINIO_IDENTITY_OPENID_ROLE_POLICY"
	EnvIdentityOpenIDRedirectURI        = "MINIO_IDENTITY_OPENID_REDIRECT_URI"
	EnvIdentityOpenIDRedirectURIDynamic = "MINIO_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC"
	EnvIdentityOpenIDScopes             = "MINIO_IDENTITY_OPENID_SCOPES"
	EnvIdentityOpenIDDisplayName        = "MINIO_IDENTITY_OPENID_DISPLAY_NAME"

	// Vendor specific ENVs only enabled if the Vendor matches == "vendor"
	EnvIdentityOpenIDKeyCloakRealm    = "MINIO_IDENTITY_OPENID_KEYCLOAK_REALM"
	EnvIdentityOpenIDKeyCloakAdminURL = "MINIO_IDENTITY_OPENID_KEYCLOAK_ADMIN_URL"
)

// DefaultKVS - default config for OpenID config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: "",
		},
		config.KV{
			Key:   DisplayName,
			Value: "",
		},
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
			Key:   ClaimUserinfo,
			Value: "",
		},
		config.KV{
			Key:   RolePolicy,
			Value: "",
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
			Key:   RedirectURIDynamic,
			Value: "off",
		},
		config.KV{
			Key:   Scopes,
			Value: "",
		},
	}
)

var errSingleProvider = config.Errorf("Only one OpenID provider can be configured if not using role policy mapping")

// DummyRoleARN is used to indicate that the user associated with it was
// authenticated via policy-claim based OpenID provider.
var DummyRoleARN = func() arn.ARN {
	v, err := arn.NewIAMRoleARN("dummy-internal", "")
	if err != nil {
		panic("should not happen!")
	}
	return v
}()

// Config - OpenID Config
type Config struct {
	Enabled bool

	// map of roleARN to providerCfg's
	arnProviderCfgsMap map[arn.ARN]*providerCfg

	// map of config names to providerCfg's
	ProviderCfgs map[string]*providerCfg

	pubKeys          publicKeys
	roleArnPolicyMap map[arn.ARN]string

	transport   http.RoundTripper
	closeRespFn func(io.ReadCloser)
}

// Clone returns a cloned copy of OpenID config.
func (r *Config) Clone() Config {
	if r == nil {
		return Config{}
	}
	cfg := Config{
		Enabled:            r.Enabled,
		arnProviderCfgsMap: make(map[arn.ARN]*providerCfg, len(r.arnProviderCfgsMap)),
		ProviderCfgs:       make(map[string]*providerCfg, len(r.ProviderCfgs)),
		pubKeys:            r.pubKeys,
		roleArnPolicyMap:   make(map[arn.ARN]string, len(r.roleArnPolicyMap)),
		transport:          r.transport,
		closeRespFn:        r.closeRespFn,
	}
	for k, v := range r.arnProviderCfgsMap {
		cfg.arnProviderCfgsMap[k] = v
	}
	for k, v := range r.ProviderCfgs {
		cfg.ProviderCfgs[k] = v
	}
	for k, v := range r.roleArnPolicyMap {
		cfg.roleArnPolicyMap[k] = v
	}
	return cfg
}

// LookupConfig lookup jwks from config, override with any ENVs.
func LookupConfig(kvsMap map[string]config.KVS, transport http.RoundTripper, closeRespFn func(io.ReadCloser), serverRegion string) (c Config, err error) {
	openIDClientTransport := http.DefaultTransport
	if transport != nil {
		openIDClientTransport = transport
	}
	c = Config{
		Enabled:            false,
		arnProviderCfgsMap: map[arn.ARN]*providerCfg{},
		ProviderCfgs:       map[string]*providerCfg{},
		pubKeys: publicKeys{
			RWMutex: &sync.RWMutex{},
			pkMap:   map[string]crypto.PublicKey{},
		},
		roleArnPolicyMap: map[arn.ARN]string{},
		transport:        openIDClientTransport,
		closeRespFn:      closeRespFn,
	}

	// Make a copy of the config we received so we can mutate it safely.
	kvsMap2 := make(map[string]config.KVS, len(kvsMap))
	for k, v := range kvsMap {
		kvsMap2[k] = v
	}

	// Add in each configuration name found from environment variables, i.e.
	// if we see MINIO_IDENTITY_OPENID_CONFIG_URL_2, we add the key "2" to
	// `kvsMap2` if it does not already exist.
	envs := env.List(EnvIdentityOpenIDURL + config.Default)
	for _, k := range envs {
		cfgName := strings.TrimPrefix(k, EnvIdentityOpenIDURL+config.Default)
		if cfgName == "" {
			return c, config.Errorf("Environment variable must have a non-empty config name: %s", k)
		}

		// It is possible that some variables were specified via config
		// commands and some variables are intended to be overridden
		// from the environment, so we ensure that the key is not
		// overwritten in `kvsMap2` as it may have existing config.
		if _, ok := kvsMap2[cfgName]; !ok {
			kvsMap2[cfgName] = DefaultKVS
		}
	}

	var (
		hasLegacyPolicyMapping = false
		seenClientIDs          = set.NewStringSet()
	)
	for cfgName, kvs := range kvsMap2 {
		// remove this since we have removed support for this already.
		kvs.Delete(JwksURL)

		if err = config.CheckValidKeys(config.IdentityOpenIDSubSys, kvs, DefaultKVS); err != nil {
			return c, err
		}

		getCfgVal := func(envVar, cfgParam string) string {
			if cfgName != config.Default {
				envVar += config.Default + cfgName
			}
			return env.Get(envVar, kvs.Get(cfgParam))
		}

		// In the past, when only one openID provider was allowed, there
		// was no `enable` parameter - the configuration is turned off
		// by clearing the values. With multiple providers, we support
		// individually enabling/disabling provider configurations. If
		// the enable parameter's value is non-empty, we use that
		// setting, otherwise we treat it as enabled if some important
		// parameters are non-empty.
		var (
			cfgEnableVal        = getCfgVal(EnvIdentityOpenIDEnable, config.Enable)
			isExplicitlyEnabled = false
		)
		if cfgEnableVal != "" {
			isExplicitlyEnabled = true
		}

		var enabled bool
		if isExplicitlyEnabled {
			enabled, err = config.ParseBool(cfgEnableVal)
			if err != nil {
				return c, err
			}
			// No need to continue loading if the config is not enabled.
			if !enabled {
				continue
			}
		}

		p := newProviderCfgFromConfig(getCfgVal)
		configURL := getCfgVal(EnvIdentityOpenIDURL, ConfigURL)

		if !isExplicitlyEnabled {
			enabled = true
			if p.ClientID == "" && p.ClientSecret == "" && configURL == "" {
				enabled = false
			}
		}

		// No need to continue loading if the config is not enabled.
		if !enabled {
			continue
		}

		// Validate that client ID has not been duplicately specified.
		if seenClientIDs.Contains(p.ClientID) {
			return c, config.Errorf("Client ID %s is present with multiple OpenID configurations", p.ClientID)
		}
		seenClientIDs.Add(p.ClientID)

		p.URL, err = xnet.ParseHTTPURL(configURL)
		if err != nil {
			return c, err
		}
		configURLDomain := p.URL.Hostname()
		p.DiscoveryDoc, err = parseDiscoveryDoc(p.URL, transport, closeRespFn)
		if err != nil {
			return c, err
		}

		if p.ClaimUserinfo && configURL == "" {
			return c, errors.New("please specify config_url to enable fetching claims from UserInfo endpoint")
		}

		if scopeList := getCfgVal(EnvIdentityOpenIDScopes, Scopes); scopeList != "" {
			var scopes []string
			for _, scope := range strings.Split(scopeList, ",") {
				scope = strings.TrimSpace(scope)
				if scope == "" {
					return c, config.Errorf("empty scope value is not allowed '%s', please refer to our documentation", scopeList)
				}
				scopes = append(scopes, scope)
			}
			// Replace the discovery document scopes by client customized scopes.
			p.DiscoveryDoc.ScopesSupported = scopes
		}

		// Check if claim name is the non-default value and role policy is set.
		if p.ClaimName != iampolicy.PolicyName && p.RolePolicy != "" {
			// In the unlikely event that the user specifies
			// `iampolicy.PolicyName` as the claim name explicitly and sets
			// a role policy, this check is thwarted, but we will be using
			// the role policy anyway.
			return c, config.Errorf("Role Policy (=`%s`) and Claim Name (=`%s`) cannot both be set", p.RolePolicy, p.ClaimName)
		}

		jwksURL := p.DiscoveryDoc.JwksURI
		if jwksURL == "" {
			return c, config.Errorf("no JWKS URI found in your provider's discovery doc (config_url=%s)", configURL)
		}

		p.JWKS.URL, err = xnet.ParseHTTPURL(jwksURL)
		if err != nil {
			return c, err
		}

		if p.RolePolicy != "" {
			// RolePolicy is validated by IAM System during its
			// initialization.

			// Generate role ARN as combination of provider domain and
			// prefix of client ID.
			domain := configURLDomain
			if domain == "" {
				// Attempt to parse the JWKs URI.
				domain = p.JWKS.URL.Hostname()
				if domain == "" {
					return c, config.Errorf("unable to parse a domain from the OpenID config")
				}
			}
			if p.ClientID == "" {
				return c, config.Errorf("client ID must not be empty")
			}

			// We set the resource ID of the role arn as a hash of client
			// ID, so we can get a short roleARN that stays the same on
			// restart.
			var resourceID string
			{
				h := sha1.New()
				h.Write([]byte(p.ClientID))
				bs := h.Sum(nil)
				resourceID = base64.RawURLEncoding.EncodeToString(bs)
			}
			p.roleArn, err = arn.NewIAMRoleARN(resourceID, serverRegion)
			if err != nil {
				return c, config.Errorf("unable to generate ARN from the OpenID config: %v", err)
			}

			c.roleArnPolicyMap[p.roleArn] = p.RolePolicy
		} else if p.ClaimName == "" {
			return c, config.Errorf("A role policy or claim name must be specified")
		}

		if err = p.initializeProvider(getCfgVal, c.transport); err != nil {
			return c, err
		}

		arnKey := p.roleArn
		if p.RolePolicy == "" {
			arnKey = DummyRoleARN
			hasLegacyPolicyMapping = true
			// Ensure that when a JWT policy claim based provider
			// exists, it is the only one.
			if _, ok := c.arnProviderCfgsMap[DummyRoleARN]; ok {
				return c, errSingleProvider
			}
		}

		c.arnProviderCfgsMap[arnKey] = &p
		c.ProviderCfgs[cfgName] = &p

		if err = c.PopulatePublicKey(arnKey); err != nil {
			return c, err
		}
	}

	// Ensure that when a JWT policy claim based provider
	// exists, it is the only one.
	if hasLegacyPolicyMapping && len(c.ProviderCfgs) > 1 {
		return c, errSingleProvider
	}

	c.Enabled = true

	return c, nil
}

// Enabled returns if configURL is enabled.
func Enabled(kvs config.KVS) bool {
	return kvs.Get(ConfigURL) != ""
}

// GetSettings - fetches OIDC settings for site-replication related validation.
// NOTE that region must be populated by caller as this package does not know.
func (r *Config) GetSettings() madmin.OpenIDSettings {
	res := madmin.OpenIDSettings{}
	if !r.Enabled {
		return res
	}

	for arn, provCfg := range r.arnProviderCfgsMap {
		hashedSecret := ""
		{
			h := sha256.New()
			h.Write([]byte(provCfg.ClientSecret))
			bs := h.Sum(nil)
			hashedSecret = base64.RawURLEncoding.EncodeToString(bs)
		}
		if arn != DummyRoleARN {
			if res.Roles != nil {
				res.Roles = make(map[string]madmin.OpenIDProviderSettings)
			}
			res.Roles[arn.String()] = madmin.OpenIDProviderSettings{
				ClaimUserinfoEnabled: provCfg.ClaimUserinfo,
				RolePolicy:           provCfg.RolePolicy,
				ClientID:             provCfg.ClientID,
				HashedClientSecret:   hashedSecret,
			}
		} else {
			res.ClaimProvider = madmin.OpenIDProviderSettings{
				ClaimUserinfoEnabled: provCfg.ClaimUserinfo,
				RolePolicy:           provCfg.RolePolicy,
				ClientID:             provCfg.ClientID,
				HashedClientSecret:   hashedSecret,
			}
		}

	}

	return res
}

// GetIAMPolicyClaimName - returns the policy claim name for the (at most one)
// provider configured without a role policy.
func (r *Config) GetIAMPolicyClaimName() string {
	pCfg, ok := r.arnProviderCfgsMap[DummyRoleARN]
	if !ok {
		return ""
	}
	return pCfg.ClaimPrefix + pCfg.ClaimName
}

// LookupUser lookup userid for the provider
func (r Config) LookupUser(roleArn, userid string) (provider.User, error) {
	// Can safely ignore error here as empty or invalid ARNs will not be
	// mapped.
	arnVal, _ := arn.Parse(roleArn)
	pCfg, ok := r.arnProviderCfgsMap[arnVal]
	if ok {
		user, err := pCfg.provider.LookupUser(userid)
		if err != nil && err != provider.ErrAccessTokenExpired {
			return user, err
		}
		if err == provider.ErrAccessTokenExpired {
			if err = pCfg.provider.LoginWithClientID(pCfg.ClientID, pCfg.ClientSecret); err != nil {
				return user, err
			}
			user, err = pCfg.provider.LookupUser(userid)
		}
		return user, err
	}
	// Without any specific logic for a provider, all accounts
	// are always enabled.
	return provider.User{ID: userid, Enabled: true}, nil
}

// ProviderEnabled returns true if any vendor specific provider is enabled.
func (r Config) ProviderEnabled() bool {
	if !r.Enabled {
		return false
	}
	for _, v := range r.arnProviderCfgsMap {
		if v.provider != nil {
			return true
		}
	}
	return false
}

// GetRoleInfo - returns ARN to policies map if a role policy based openID
// provider is configured. Otherwise returns nil.
func (r Config) GetRoleInfo() map[arn.ARN]string {
	for _, p := range r.arnProviderCfgsMap {
		if p.RolePolicy != "" {
			return r.roleArnPolicyMap
		}
	}
	return nil
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
		// up to 365 days.
		if expirySecs < 900 || expirySecs > 31536000 {
			return 0, auth.ErrInvalidDuration
		}

		defaultExpiryDuration = time.Duration(expirySecs) * time.Second
	}
	return defaultExpiryDuration, nil
}
