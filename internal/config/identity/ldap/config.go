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

package ldap

import (
	"crypto/x509"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

const (
	defaultLDAPExpiry = time.Hour * 1

	dnDelimiter = ";"

	minLDAPExpiry time.Duration = 15 * time.Minute
	maxLDAPExpiry time.Duration = 365 * 24 * time.Hour
)

// Config contains AD/LDAP server connectivity information.
type Config struct {
	Enabled bool `json:"enabled"`

	// E.g. "ldap.minio.io:636"
	ServerAddr string `json:"serverAddr"`

	// User DN search parameters
	UserDNSearchBaseDistName  string   `json:"userDNSearchBaseDN"`
	UserDNSearchBaseDistNames []string `json:"-"` // Generated field
	UserDNSearchFilter        string   `json:"userDNSearchFilter"`

	// Group search parameters
	GroupSearchBaseDistName  string   `json:"groupSearchBaseDN"`
	GroupSearchBaseDistNames []string `json:"-"` // Generated field
	GroupSearchFilter        string   `json:"groupSearchFilter"`

	// Lookup bind LDAP service account
	LookupBindDN       string `json:"lookupBindDN"`
	LookupBindPassword string `json:"lookupBindPassword"`

	stsExpiryDuration time.Duration // contains converted value
	tlsSkipVerify     bool          // allows skipping TLS verification
	serverInsecure    bool          // allows plain text connection to LDAP server
	serverStartTLS    bool          // allows using StartTLS connection to LDAP server
	rootCAs           *x509.CertPool
}

// Clone returns a cloned copy of LDAP config.
func (l *Config) Clone() Config {
	if l == nil {
		return Config{}
	}
	cfg := Config{
		Enabled:                   l.Enabled,
		ServerAddr:                l.ServerAddr,
		UserDNSearchBaseDistName:  l.UserDNSearchBaseDistName,
		UserDNSearchBaseDistNames: l.UserDNSearchBaseDistNames,
		UserDNSearchFilter:        l.UserDNSearchFilter,
		GroupSearchBaseDistName:   l.GroupSearchBaseDistName,
		GroupSearchBaseDistNames:  l.GroupSearchBaseDistNames,
		GroupSearchFilter:         l.GroupSearchFilter,
		LookupBindDN:              l.LookupBindDN,
		LookupBindPassword:        l.LookupBindPassword,
		stsExpiryDuration:         l.stsExpiryDuration,
		tlsSkipVerify:             l.tlsSkipVerify,
		serverInsecure:            l.serverInsecure,
		serverStartTLS:            l.serverStartTLS,
		rootCAs:                   l.rootCAs,
	}
	return cfg
}

// LDAP keys and envs.
const (
	ServerAddr         = "server_addr"
	LookupBindDN       = "lookup_bind_dn"
	LookupBindPassword = "lookup_bind_password"
	UserDNSearchBaseDN = "user_dn_search_base_dn"
	UserDNSearchFilter = "user_dn_search_filter"
	GroupSearchFilter  = "group_search_filter"
	GroupSearchBaseDN  = "group_search_base_dn"
	TLSSkipVerify      = "tls_skip_verify"
	ServerInsecure     = "server_insecure"
	ServerStartTLS     = "server_starttls"

	EnvServerAddr         = "MINIO_IDENTITY_LDAP_SERVER_ADDR"
	EnvTLSSkipVerify      = "MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY"
	EnvServerInsecure     = "MINIO_IDENTITY_LDAP_SERVER_INSECURE"
	EnvServerStartTLS     = "MINIO_IDENTITY_LDAP_SERVER_STARTTLS"
	EnvUsernameFormat     = "MINIO_IDENTITY_LDAP_USERNAME_FORMAT"
	EnvUserDNSearchBaseDN = "MINIO_IDENTITY_LDAP_USER_DN_SEARCH_BASE_DN"
	EnvUserDNSearchFilter = "MINIO_IDENTITY_LDAP_USER_DN_SEARCH_FILTER"
	EnvGroupSearchFilter  = "MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER"
	EnvGroupSearchBaseDN  = "MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN"
	EnvLookupBindDN       = "MINIO_IDENTITY_LDAP_LOOKUP_BIND_DN"
	EnvLookupBindPassword = "MINIO_IDENTITY_LDAP_LOOKUP_BIND_PASSWORD"
)

var removedKeys = []string{
	"sts_expiry",
	"username_format",
	"username_search_filter",
	"username_search_base_dn",
	"group_name_attribute",
}

// DefaultKVS - default config for LDAP config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   ServerAddr,
			Value: "",
		},
		config.KV{
			Key:   UserDNSearchBaseDN,
			Value: "",
		},
		config.KV{
			Key:   UserDNSearchFilter,
			Value: "",
		},
		config.KV{
			Key:   GroupSearchFilter,
			Value: "",
		},
		config.KV{
			Key:   GroupSearchBaseDN,
			Value: "",
		},
		config.KV{
			Key:   TLSSkipVerify,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   ServerInsecure,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   ServerStartTLS,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   LookupBindDN,
			Value: "",
		},
		config.KV{
			Key:   LookupBindPassword,
			Value: "",
		},
	}
)

// Enabled returns if LDAP config is enabled.
func Enabled(kvs config.KVS) bool {
	return kvs.Get(ServerAddr) != ""
}

// Lookup - initializes LDAP config, overrides config, if any ENV values are set.
func Lookup(kvs config.KVS, rootCAs *x509.CertPool) (l Config, err error) {
	l = Config{}

	// Purge all removed keys first
	for _, k := range removedKeys {
		kvs.Delete(k)
	}

	if err = config.CheckValidKeys(config.IdentityLDAPSubSys, kvs, DefaultKVS); err != nil {
		return l, err
	}

	ldapServer := env.Get(EnvServerAddr, kvs.Get(ServerAddr))
	if ldapServer == "" {
		return l, nil
	}
	l.Enabled = true
	l.rootCAs = rootCAs
	l.ServerAddr = ldapServer
	l.stsExpiryDuration = defaultLDAPExpiry

	// LDAP connection configuration
	if v := env.Get(EnvServerInsecure, kvs.Get(ServerInsecure)); v != "" {
		l.serverInsecure, err = config.ParseBool(v)
		if err != nil {
			return l, err
		}
	}
	if v := env.Get(EnvServerStartTLS, kvs.Get(ServerStartTLS)); v != "" {
		l.serverStartTLS, err = config.ParseBool(v)
		if err != nil {
			return l, err
		}
	}
	if v := env.Get(EnvTLSSkipVerify, kvs.Get(TLSSkipVerify)); v != "" {
		l.tlsSkipVerify, err = config.ParseBool(v)
		if err != nil {
			return l, err
		}
	}

	// Lookup bind user configuration
	l.LookupBindDN = env.Get(EnvLookupBindDN, kvs.Get(LookupBindDN))
	l.LookupBindPassword = env.Get(EnvLookupBindPassword, kvs.Get(LookupBindPassword))

	// User DN search configuration
	l.UserDNSearchFilter = env.Get(EnvUserDNSearchFilter, kvs.Get(UserDNSearchFilter))
	l.UserDNSearchBaseDistName = env.Get(EnvUserDNSearchBaseDN, kvs.Get(UserDNSearchBaseDN))

	// Group search params configuration
	l.GroupSearchFilter = env.Get(EnvGroupSearchFilter, kvs.Get(GroupSearchFilter))
	l.GroupSearchBaseDistName = env.Get(EnvGroupSearchBaseDN, kvs.Get(GroupSearchBaseDN))

	// Validate and test configuration.
	valResult := l.Validate()
	if !valResult.IsOk() {
		return l, valResult
	}

	return l, nil
}
