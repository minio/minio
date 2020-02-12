/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package ldap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	ldap "gopkg.in/ldap.v3"
)

const (
	defaultLDAPExpiry = time.Hour * 1
)

// Config contains AD/LDAP server connectivity information.
type Config struct {
	Enabled bool `json:"enabled"`

	// E.g. "ldap.minio.io:636"
	ServerAddr string `json:"serverAddr"`

	// STS credentials expiry duration
	STSExpiryDuration string `json:"stsExpiryDuration"`

	// Format string for usernames
	UsernameFormat string `json:"usernameFormat"`

	GroupSearchBaseDN  string `json:"groupSearchBaseDN"`
	GroupSearchFilter  string `json:"groupSearchFilter"`
	GroupNameAttribute string `json:"groupNameAttribute"`

	stsExpiryDuration time.Duration // contains converted value
	tlsSkipVerify     bool          // allows skipping TLS verification
	rootCAs           *x509.CertPool
}

// LDAP keys and envs.
const (
	ServerAddr         = "server_addr"
	STSExpiry          = "sts_expiry"
	UsernameFormat     = "username_format"
	GroupSearchFilter  = "group_search_filter"
	GroupNameAttribute = "group_name_attribute"
	GroupSearchBaseDN  = "group_search_base_dn"
	TLSSkipVerify      = "tls_skip_verify"

	EnvServerAddr         = "MINIO_IDENTITY_LDAP_SERVER_ADDR"
	EnvSTSExpiry          = "MINIO_IDENTITY_LDAP_STS_EXPIRY"
	EnvTLSSkipVerify      = "MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY"
	EnvUsernameFormat     = "MINIO_IDENTITY_LDAP_USERNAME_FORMAT"
	EnvGroupSearchFilter  = "MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER"
	EnvGroupNameAttribute = "MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE"
	EnvGroupSearchBaseDN  = "MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN"
)

// DefaultKVS - default config for LDAP config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   ServerAddr,
			Value: "",
		},
		config.KV{
			Key:   STSExpiry,
			Value: "1h",
		},
		config.KV{
			Key:   UsernameFormat,
			Value: "",
		},
		config.KV{
			Key:   GroupSearchFilter,
			Value: "",
		},
		config.KV{
			Key:   GroupNameAttribute,
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
	}
)

// Connect connect to ldap server.
func (l *Config) Connect() (ldapConn *ldap.Conn, err error) {
	if l == nil {
		// Happens when LDAP is not configured.
		return
	}
	return ldap.DialTLS("tcp", l.ServerAddr, &tls.Config{
		InsecureSkipVerify: l.tlsSkipVerify,
		RootCAs:            l.rootCAs,
	})
}

// GetExpiryDuration - return parsed expiry duration.
func (l Config) GetExpiryDuration() time.Duration {
	return l.stsExpiryDuration
}

// Enabled returns if jwks is enabled.
func Enabled(kvs config.KVS) bool {
	return kvs.Get(ServerAddr) != ""
}

// Lookup - initializes LDAP config, overrides config, if any ENV values are set.
func Lookup(kvs config.KVS, rootCAs *x509.CertPool) (l Config, err error) {
	l = Config{}
	if err = config.CheckValidKeys(config.IdentityLDAPSubSys, kvs, DefaultKVS); err != nil {
		return l, err
	}
	ldapServer := env.Get(EnvServerAddr, kvs.Get(ServerAddr))
	if ldapServer == "" {
		return l, nil
	}
	l.Enabled = true
	l.ServerAddr = ldapServer
	l.stsExpiryDuration = defaultLDAPExpiry
	if v := env.Get(EnvSTSExpiry, kvs.Get(STSExpiry)); v != "" {
		expDur, err := time.ParseDuration(v)
		if err != nil {
			return l, errors.New("LDAP expiry time err:" + err.Error())
		}
		if expDur <= 0 {
			return l, errors.New("LDAP expiry time has to be positive")
		}
		l.STSExpiryDuration = v
		l.stsExpiryDuration = expDur
	}
	if v := env.Get(EnvTLSSkipVerify, kvs.Get(TLSSkipVerify)); v != "" {
		l.tlsSkipVerify, err = config.ParseBool(v)
		if err != nil {
			return l, err
		}
	}
	if v := env.Get(EnvUsernameFormat, kvs.Get(UsernameFormat)); v != "" {
		subs, err := NewSubstituter("username", "test")
		if err != nil {
			return l, err
		}
		if _, err := subs.Substitute(v); err != nil {
			return l, err
		}
		l.UsernameFormat = v
	} else {
		return l, fmt.Errorf("'%s' cannot be empty and must have a value", UsernameFormat)
	}

	grpSearchFilter := env.Get(EnvGroupSearchFilter, kvs.Get(GroupSearchFilter))
	grpSearchNameAttr := env.Get(EnvGroupNameAttribute, kvs.Get(GroupNameAttribute))
	grpSearchBaseDN := env.Get(EnvGroupSearchBaseDN, kvs.Get(GroupSearchBaseDN))

	// Either all group params must be set or none must be set.
	allNotSet := grpSearchFilter == "" && grpSearchNameAttr == "" && grpSearchBaseDN == ""
	allSet := grpSearchFilter != "" && grpSearchNameAttr != "" && grpSearchBaseDN != ""
	if !allNotSet && !allSet {
		return l, errors.New("All group related parameters must be set")
	}

	if allSet {
		subs, err := NewSubstituter("username", "test", "usernamedn", "test2")
		if err != nil {
			return l, err
		}
		if _, err := subs.Substitute(grpSearchFilter); err != nil {
			return l, fmt.Errorf("Only username and usernamedn may be substituted in the group search filter string: %s", err)
		}
		l.GroupSearchFilter = grpSearchFilter
		l.GroupNameAttribute = grpSearchNameAttr
		subs, err = NewSubstituter("username", "test", "usernamedn", "test2")
		if err != nil {
			return l, err
		}
		if _, err := subs.Substitute(grpSearchBaseDN); err != nil {
			return l, fmt.Errorf("Only username and usernamedn may be substituted in the base DN string: %s", err)
		}
		l.GroupSearchBaseDN = grpSearchBaseDN
	}

	l.rootCAs = rootCAs
	return l, nil
}

// Substituter - This type is to allow restricted runtime
// substitutions of variables in LDAP configuration items during
// runtime.
type Substituter struct {
	vals map[string]string
}

// NewSubstituter - sets up the substituter for usage, for e.g.:
//
//  subber := NewSubstituter("username", "john")
func NewSubstituter(v ...string) (Substituter, error) {
	if len(v)%2 != 0 {
		return Substituter{}, errors.New("Need an even number of arguments")
	}
	vals := make(map[string]string)
	for i := 0; i < len(v); i += 2 {
		vals[v[i]] = v[i+1]
	}
	return Substituter{vals: vals}, nil
}

// Substitute - performs substitution on the given string `t`. Returns
// an error if there are any variables in the input that do not have
// values in the substituter. E.g.:
//
// subber.Substitute("uid=${username},cn=users,dc=example,dc=com")
//
// or
//
// subber.Substitute("uid={username},cn=users,dc=example,dc=com")
//
// returns "uid=john,cn=users,dc=example,dc=com"
//
// whereas:
//
// subber.Substitute("uid=${usernamedn}")
//
// returns an error.
func (s *Substituter) Substitute(t string) (string, error) {
	for k, v := range s.vals {
		reDollar := regexp.MustCompile(fmt.Sprintf(`\$\{%s\}`, k))
		t = reDollar.ReplaceAllLiteralString(t, v)
		reFlower := regexp.MustCompile(fmt.Sprintf(`\{%s\}`, k))
		t = reFlower.ReplaceAllLiteralString(t, v)
	}
	// Check if all requested substitutions have been made.
	re := regexp.MustCompile(`\{.*\}`)
	if re.MatchString(t) {
		return "", errors.New("unsupported substitution requested")
	}
	return t, nil
}
