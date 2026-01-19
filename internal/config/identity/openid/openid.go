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
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/arn"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/identity/openid/provider"
	"github.com/minio/minio/internal/hash/sha256"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/minio/pkg/v3/policy"
)

// OpenID keys and envs.
const (
	ClientID          = "client_id"
	ClientSecret      = "client_secret"
	ConfigURL         = "config_url"
	ClaimName         = "claim_name"
	ClaimUserinfo     = "claim_userinfo"
	RolePolicy        = "role_policy"
	DisplayName       = "display_name"
	UserReadableClaim = "user_readable_claim"
	UserIDClaim       = "user_id_claim"

	Scopes             = "scopes"
	RedirectURI        = "redirect_uri"
	RedirectURIDynamic = "redirect_uri_dynamic"
	Vendor             = "vendor"

	// Vendor specific ENV only enabled if the Vendor matches == "vendor"
	KeyCloakRealm    = "keycloak_realm"
	KeyCloakAdminURL = "keycloak_admin_url"

	// Removed params
	JwksURL     = "jwks_url"
	ClaimPrefix = "claim_prefix"
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
			Value: policy.PolicyName,
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
			Key:           ClaimPrefix,
			Value:         "",
			HiddenIfEmpty: true,
		},
		config.KV{
			Key:           RedirectURI,
			Value:         "",
			HiddenIfEmpty: true,
		},
		config.KV{
			Key:   RedirectURIDynamic,
			Value: "off",
		},
		config.KV{
			Key:   Scopes,
			Value: "",
		},
		config.KV{
			Key:   Vendor,
			Value: "",
		},
		config.KV{
			Key:   KeyCloakRealm,
			Value: "",
		},
		config.KV{
			Key:   KeyCloakAdminURL,
			Value: "",
		},
		config.KV{
			Key:   UserReadableClaim,
			Value: "",
		},
		config.KV{
			Key:   UserIDClaim,
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
	maps.Copy(cfg.arnProviderCfgsMap, r.arnProviderCfgsMap)
	maps.Copy(cfg.ProviderCfgs, r.ProviderCfgs)
	maps.Copy(cfg.roleArnPolicyMap, r.roleArnPolicyMap)
	return cfg
}

// LookupConfig lookup jwks from config, override with any ENVs.
func LookupConfig(s config.Config, transport http.RoundTripper, closeRespFn func(io.ReadCloser), serverRegion string) (c Config, err error) {
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
			pkMap:   map[string]any{},
		},
		roleArnPolicyMap: map[arn.ARN]string{},
		transport:        openIDClientTransport,
		closeRespFn:      closeRespFn,
	}

	seenClientIDs := set.NewStringSet()

	deprecatedKeys := []string{JwksURL}

	// remove this since we have removed support for this already.
	for k := range s[config.IdentityOpenIDSubSys] {
		for _, dk := range deprecatedKeys {
			kvs := s[config.IdentityOpenIDSubSys][k]
			kvs.Delete(dk)
			s[config.IdentityOpenIDSubSys][k] = kvs
		}
	}

	if err := s.CheckValidKeys(config.IdentityOpenIDSubSys, deprecatedKeys); err != nil {
		return c, err
	}

	openIDTargets, err := s.GetAvailableTargets(config.IdentityOpenIDSubSys)
	if err != nil {
		return c, err
	}

	for _, cfgName := range openIDTargets {
		getCfgVal := func(cfgParam string) string {
			// As parameters are already validated, we skip checking
			// if the config param was found.
			val, _, _ := s.ResolveConfigParam(config.IdentityOpenIDSubSys, cfgName, cfgParam, false)
			return val
		}

		// In the past, when only one openID provider was allowed, there
		// was no `enable` parameter - the configuration is turned off
		// by clearing the values. With multiple providers, we support
		// individually enabling/disabling provider configurations. If
		// the enable parameter's value is non-empty, we use that
		// setting, otherwise we treat it as enabled if some important
		// parameters are non-empty.
		var (
			cfgEnableVal        = getCfgVal(config.Enable)
			isExplicitlyEnabled = cfgEnableVal != ""
		)

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
		configURL := getCfgVal(ConfigURL)

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

		if scopeList := getCfgVal(Scopes); scopeList != "" {
			var scopes []string
			for scope := range strings.SplitSeq(scopeList, ",") {
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
		if p.ClaimName != policy.PolicyName && p.RolePolicy != "" {
			// In the unlikely event that the user specifies
			// `policy.PolicyName` as the claim name explicitly and sets
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
			// Ensure that at most one JWT policy claim based provider may be
			// defined.
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

	c.Enabled = true

	return c, nil
}

// ErrProviderConfigNotFound - represents a non-existing provider error.
var ErrProviderConfigNotFound = errors.New("provider configuration not found")

// GetConfigInfo - returns configuration and related info for the given IDP
// provider.
func (r *Config) GetConfigInfo(s config.Config, cfgName string) ([]madmin.IDPCfgInfo, error) {
	openIDConfigs, err := s.GetAvailableTargets(config.IdentityOpenIDSubSys)
	if err != nil {
		return nil, err
	}

	present := slices.Contains(openIDConfigs, cfgName)

	if !present {
		return nil, ErrProviderConfigNotFound
	}

	kvsrcs, err := s.GetResolvedConfigParams(config.IdentityOpenIDSubSys, cfgName, true)
	if err != nil {
		return nil, err
	}

	res := make([]madmin.IDPCfgInfo, 0, len(kvsrcs)+1)
	for _, kvsrc := range kvsrcs {
		// skip returning default config values.
		if kvsrc.Src == config.ValueSourceDef {
			if kvsrc.Key != madmin.EnableKey {
				continue
			}
			// for EnableKey we set an explicit on/off from live configuration
			// if it is present.
			if _, ok := r.ProviderCfgs[cfgName]; !ok {
				// No live config is present
				continue
			}
			if r.Enabled {
				kvsrc.Value = "on"
			} else {
				kvsrc.Value = "off"
			}
		}
		res = append(res, madmin.IDPCfgInfo{
			Key:   kvsrc.Key,
			Value: kvsrc.Value,
			IsCfg: true,
			IsEnv: kvsrc.Src == config.ValueSourceEnv,
		})
	}

	if provCfg, exists := r.ProviderCfgs[cfgName]; exists && provCfg.RolePolicy != "" {
		// Append roleARN
		res = append(res, madmin.IDPCfgInfo{
			Key:   "roleARN",
			Value: provCfg.roleArn.String(),
			IsCfg: false,
		})
	}

	// sort the structs by the key
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})

	return res, nil
}

// GetConfigList - list openID configurations
func (r *Config) GetConfigList(s config.Config) ([]madmin.IDPListItem, error) {
	openIDConfigs, err := s.GetAvailableTargets(config.IdentityOpenIDSubSys)
	if err != nil {
		return nil, err
	}

	var res []madmin.IDPListItem
	for _, cfg := range openIDConfigs {
		pcfg, ok := r.ProviderCfgs[cfg]
		if !ok {
			res = append(res, madmin.IDPListItem{
				Type:    "openid",
				Name:    cfg,
				Enabled: false,
			})
		} else {
			var roleARN string
			if pcfg.RolePolicy != "" {
				roleARN = pcfg.roleArn.String()
			}
			res = append(res, madmin.IDPListItem{
				Type:    "openid",
				Name:    cfg,
				Enabled: r.Enabled,
				RoleARN: roleARN,
			})
		}
	}

	return res, nil
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
	h := sha256.New()
	hashedSecret := ""
	for arn, provCfg := range r.arnProviderCfgsMap {
		h.Write([]byte(provCfg.ClientSecret))
		hashedSecret = base64.RawURLEncoding.EncodeToString(h.Sum(nil))
		h.Reset()
		if arn != DummyRoleARN {
			if res.Roles == nil {
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
	timeout := env.Get(config.EnvMinioStsDuration, "")
	defaultExpiryDuration, err := time.ParseDuration(timeout)
	if err != nil {
		defaultExpiryDuration = time.Hour
	}
	if timeout == "" && dsecs != "" {
		expirySecs, err := strconv.ParseInt(dsecs, 10, 64)
		if err != nil {
			return 0, auth.ErrInvalidDuration
		}

		// The duration, in seconds, of the role session.
		// The value can range from 900 seconds (15 minutes)
		// up to 365 days.
		if expirySecs < config.MinExpiration || expirySecs > config.MaxExpiration {
			return 0, auth.ErrInvalidDuration
		}

		defaultExpiryDuration = time.Duration(expirySecs) * time.Second
	} else if timeout == "" && dsecs == "" {
		return time.Hour, nil
	}

	if defaultExpiryDuration.Seconds() < config.MinExpiration || defaultExpiryDuration.Seconds() > config.MaxExpiration {
		return 0, auth.ErrInvalidDuration
	}

	return defaultExpiryDuration, nil
}

// GetUserReadableClaim returns the human readable claim name for the given
// configuration name.
func (r Config) GetUserReadableClaim(cfgName string) string {
	pCfg, ok := r.ProviderCfgs[cfgName]
	if ok {
		return pCfg.UserReadableClaim
	}
	return ""
}

// GetUserIDClaim returns the user ID claim for the given configuration name, or "sub" if not set.
func (r Config) GetUserIDClaim(cfgName string) string {
	pCfg, ok := r.ProviderCfgs[cfgName]
	if ok {
		if pCfg.UserIDClaim != "" {
			return pCfg.UserIDClaim
		}
		return "sub"
	}
	return "" // an incorrect config should be handled outside this function
}
