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
	"net"
	"strings"
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
	UsernameFormat        string   `json:"usernameFormat"`
	UsernameFormats       []string `json:"-"`
	UsernameSearchFilter  string   `json:"-"`
	UsernameSearchBaseDNS []string `json:"-"`

	GroupSearchBaseDN  string   `json:"groupSearchBaseDN"`
	GroupSearchBaseDNS []string `json:"-"`
	GroupSearchFilter  string   `json:"groupSearchFilter"`
	GroupNameAttribute string   `json:"groupNameAttribute"`

	stsExpiryDuration time.Duration // contains converted value
	tlsSkipVerify     bool          // allows skipping TLS verification
	serverInsecure    bool          // allows plain text connection to LDAP Server
	serverStartTLS    bool          // allows plain text connection to LDAP Server
	rootCAs           *x509.CertPool
}

// LDAP keys and envs.
const (
	ServerAddr           = "server_addr"
	STSExpiry            = "sts_expiry"
	UsernameFormat       = "username_format"
	UsernameSearchFilter = "username_search_filter"
	UsernameSearchBaseDN = "username_search_base_dn"
	GroupSearchFilter    = "group_search_filter"
	GroupNameAttribute   = "group_name_attribute"
	GroupSearchBaseDN    = "group_search_base_dn"
	TLSSkipVerify        = "tls_skip_verify"
	ServerInsecure       = "server_insecure"
	ServerStartTLS       = "server_starttls"

	EnvServerAddr           = "MINIO_IDENTITY_LDAP_SERVER_ADDR"
	EnvSTSExpiry            = "MINIO_IDENTITY_LDAP_STS_EXPIRY"
	EnvTLSSkipVerify        = "MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY"
	EnvServerInsecure       = "MINIO_IDENTITY_LDAP_SERVER_INSECURE"
	EnvServerStartTLS       = "MINIO_IDENTITY_LDAP_SERVER_STARTTLS"
	EnvUsernameFormat       = "MINIO_IDENTITY_LDAP_USERNAME_FORMAT"
	EnvUsernameSearchFilter = "MINIO_IDENTITY_LDAP_USERNAME_SEARCH_FILTER"
	EnvUsernameSearchBaseDN = "MINIO_IDENTITY_LDAP_USERNAME_SEARCH_BASE_DN"
	EnvGroupSearchFilter    = "MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER"
	EnvGroupNameAttribute   = "MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE"
	EnvGroupSearchBaseDN    = "MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN"
)

// DefaultKVS - default config for LDAP config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   ServerAddr,
			Value: "",
		},
		config.KV{
			Key:   UsernameFormat,
			Value: "",
		},
		config.KV{
			Key:   UsernameSearchFilter,
			Value: "",
		},
		config.KV{
			Key:   UsernameSearchBaseDN,
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
			Key:   STSExpiry,
			Value: "1h",
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
	}
)

const (
	dnDelimiter = ";"
)

func getGroups(conn *ldap.Conn, sreq *ldap.SearchRequest) ([]string, error) {
	var groups []string
	sres, err := conn.Search(sreq)
	if err != nil {
		return nil, err
	}
	for _, entry := range sres.Entries {
		// We only queried one attribute,
		// so we only look up the first one.
		groups = append(groups, entry.Attributes[0].Values...)
	}
	return groups, nil
}

func (l *Config) bind(conn *ldap.Conn, username, password string) ([]string, error) {
	var bindDNS = make([]string, len(l.UsernameFormats))
	for i, usernameFormat := range l.UsernameFormats {
		bindDN := fmt.Sprintf(usernameFormat, username)
		// Bind with user credentials to validate the password
		if err := conn.Bind(bindDN, password); err != nil {
			return nil, err
		}
		bindDNS[i] = bindDN
	}
	return bindDNS, nil
}

var standardAttributes = []string{
	"givenName",
	"sn",
	"cn",
	"memberOf",
	"email",
}

// Bind - binds to ldap, searches LDAP and returns list of groups.
func (l *Config) Bind(username, password string) ([]string, error) {
	conn, err := l.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	bindDNS, err := l.bind(conn, username, password)
	if err != nil {
		return nil, err
	}

	var groups []string
	if l.UsernameSearchFilter != "" {
		for _, userSearchBase := range l.UsernameSearchBaseDNS {
			filter := strings.Replace(l.UsernameSearchFilter, "%s",
				ldap.EscapeFilter(username), -1)

			searchRequest := ldap.NewSearchRequest(
				userSearchBase,
				ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
				filter,
				standardAttributes,
				nil,
			)

			groups, err = getGroups(conn, searchRequest)
			if err != nil {
				return nil, err
			}
		}
	}

	if l.GroupSearchFilter != "" {
		for _, groupSearchBase := range l.GroupSearchBaseDNS {
			var filters []string
			if l.GroupNameAttribute == "" {
				filters = []string{strings.Replace(l.GroupSearchFilter, "%s",
					ldap.EscapeFilter(username), -1)}
			} else {
				// With group name attribute specified, make sure to
				// include search queries for CN distinguished name
				for _, bindDN := range bindDNS {
					filters = append(filters, strings.Replace(l.GroupSearchFilter, "%s",
						ldap.EscapeFilter(bindDN), -1))
				}
			}
			for _, filter := range filters {
				searchRequest := ldap.NewSearchRequest(
					groupSearchBase,
					ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
					filter,
					standardAttributes,
					nil,
				)

				var newGroups []string
				newGroups, err = getGroups(conn, searchRequest)
				if err != nil {
					return nil, err
				}

				groups = append(groups, newGroups...)
			}
		}
	}

	return groups, nil
}

// Connect connect to ldap server.
func (l *Config) Connect() (ldapConn *ldap.Conn, err error) {
	if l == nil {
		return nil, errors.New("LDAP is not configured")
	}

	if _, _, err = net.SplitHostPort(l.ServerAddr); err != nil {
		// User default LDAP port if none specified "636"
		l.ServerAddr = net.JoinHostPort(l.ServerAddr, "636")
	}

	if l.serverInsecure {
		return ldap.Dial("tcp", l.ServerAddr)
	}

	if l.serverStartTLS {
		conn, err := ldap.Dial("tcp", l.ServerAddr)
		if err != nil {
			return nil, err
		}
		err = conn.StartTLS(&tls.Config{
			InsecureSkipVerify: l.tlsSkipVerify,
			RootCAs:            l.rootCAs,
		})
		return conn, err
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
	if v := env.Get(EnvUsernameFormat, kvs.Get(UsernameFormat)); v != "" {
		if !strings.Contains(v, "%s") {
			return l, errors.New("LDAP username format doesn't have '%s' substitution")
		}
		l.UsernameFormats = strings.Split(v, dnDelimiter)
	} else {
		return l, fmt.Errorf("'%s' cannot be empty and must have a value", UsernameFormat)
	}

	if v := env.Get(EnvUsernameSearchFilter, kvs.Get(UsernameSearchFilter)); v != "" {
		if !strings.Contains(v, "%s") {
			return l, errors.New("LDAP username search filter doesn't have '%s' substitution")
		}
		l.UsernameSearchFilter = v
	}

	if v := env.Get(EnvUsernameSearchBaseDN, kvs.Get(UsernameSearchBaseDN)); v != "" {
		l.UsernameSearchBaseDNS = strings.Split(v, dnDelimiter)
	}

	grpSearchFilter := env.Get(EnvGroupSearchFilter, kvs.Get(GroupSearchFilter))
	grpSearchNameAttr := env.Get(EnvGroupNameAttribute, kvs.Get(GroupNameAttribute))
	grpSearchBaseDN := env.Get(EnvGroupSearchBaseDN, kvs.Get(GroupSearchBaseDN))

	// Either all group params must be set or none must be set.
	var allSet bool
	if grpSearchFilter != "" {
		if grpSearchNameAttr == "" || grpSearchBaseDN == "" {
			return l, errors.New("All group related parameters must be set")
		}
		allSet = true
	}

	if allSet {
		l.GroupSearchFilter = grpSearchFilter
		l.GroupNameAttribute = grpSearchNameAttr
		l.GroupSearchBaseDNS = strings.Split(grpSearchBaseDN, dnDelimiter)
	}

	l.rootCAs = rootCAs
	return l, nil
}
