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

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

const (
	defaultLDAPExpiry = time.Hour * 1

	dnDelimiter = ";"
)

// Config contains AD/LDAP server connectivity information.
type Config struct {
	Enabled bool `json:"enabled"`

	// E.g. "ldap.minio.io:636"
	ServerAddr string `json:"serverAddr"`

	// STS credentials expiry duration
	STSExpiryDuration string `json:"stsExpiryDuration"`

	// Format string for usernames
	UsernameFormat  string   `json:"usernameFormat"`
	UsernameFormats []string `json:"-"`

	// User DN search parameters
	UserDNSearchBaseDN string `json:"userDNSearchBaseDN"`
	UserDNSearchFilter string `json:"userDNSearchFilter"`

	// Group search parameters
	GroupSearchBaseDistName  string   `json:"groupSearchBaseDN"`
	GroupSearchBaseDistNames []string `json:"-"`
	GroupSearchFilter        string   `json:"groupSearchFilter"`

	// Lookup bind LDAP service account
	LookupBindDN       string `json:"lookupBindDN"`
	LookupBindPassword string `json:"lookupBindPassword"`

	stsExpiryDuration time.Duration // contains converted value
	tlsSkipVerify     bool          // allows skipping TLS verification
	serverInsecure    bool          // allows plain text connection to LDAP server
	serverStartTLS    bool          // allows using StartTLS connection to LDAP server
	isUsingLookupBind bool
	rootCAs           *x509.CertPool
}

// LDAP keys and envs.
const (
	ServerAddr         = "server_addr"
	STSExpiry          = "sts_expiry"
	LookupBindDN       = "lookup_bind_dn"
	LookupBindPassword = "lookup_bind_password"
	UserDNSearchBaseDN = "user_dn_search_base_dn"
	UserDNSearchFilter = "user_dn_search_filter"
	UsernameFormat     = "username_format"
	GroupSearchFilter  = "group_search_filter"
	GroupSearchBaseDN  = "group_search_base_dn"
	TLSSkipVerify      = "tls_skip_verify"
	ServerInsecure     = "server_insecure"
	ServerStartTLS     = "server_starttls"

	EnvServerAddr         = "MINIO_IDENTITY_LDAP_SERVER_ADDR"
	EnvSTSExpiry          = "MINIO_IDENTITY_LDAP_STS_EXPIRY"
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
			Key:   UsernameFormat,
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

func getGroups(conn *ldap.Conn, sreq *ldap.SearchRequest) ([]string, error) {
	var groups []string
	sres, err := conn.Search(sreq)
	if err != nil {
		// Check if there is no matching result and return empty slice.
		// Ref: https://ldap.com/ldap-result-code-reference/
		if ldap.IsErrorWithCode(err, 32) {
			return nil, nil
		}
		return nil, err
	}
	for _, entry := range sres.Entries {
		// We only queried one attribute,
		// so we only look up the first one.
		groups = append(groups, entry.DN)
	}
	return groups, nil
}

func (l *Config) lookupBind(conn *ldap.Conn) error {
	return conn.Bind(l.LookupBindDN, l.LookupBindPassword)
}

// usernameFormatsBind - Iterates over all given username formats and expects
// that only one will succeed if the credentials are valid. The succeeding
// bindDN is returned or an error.
//
// In the rare case that multiple username formats succeed, implying that two
// (or more) distinct users in the LDAP directory have the same username and
// password, we return an error as we cannot identify the account intended by
// the user.
func (l *Config) usernameFormatsBind(conn *ldap.Conn, username, password string) (string, error) {
	var bindDistNames []string
	var errs = make([]error, len(l.UsernameFormats))
	var successCount = 0
	for i, usernameFormat := range l.UsernameFormats {
		bindDN := fmt.Sprintf(usernameFormat, username)
		// Bind with user credentials to validate the password
		errs[i] = conn.Bind(bindDN, password)
		if errs[i] == nil {
			bindDistNames = append(bindDistNames, bindDN)
			successCount++
		}
	}
	if successCount == 0 {
		var errStrings []string
		for _, err := range errs {
			if err != nil {
				errStrings = append(errStrings, err.Error())
			}
		}
		outErr := fmt.Sprintf("All username formats failed with: %s", strings.Join(errStrings, "; "))
		return "", errors.New(outErr)
	}
	if successCount > 1 {
		successDistNames := strings.Join(bindDistNames, ", ")
		errMsg := fmt.Sprintf("Multiple username formats succeeded - ambiguous user login (succeeded for: %s)", successDistNames)
		return "", errors.New(errMsg)
	}
	return bindDistNames[0], nil
}

// lookupUserDN searches for the DN of the user given their username. conn is
// assumed to be using the lookup bind service account. It is required that the
// search result in at most one result.
func (l *Config) lookupUserDN(conn *ldap.Conn, username string) (string, error) {
	filter := strings.Replace(l.UserDNSearchFilter, "%s", ldap.EscapeFilter(username), -1)
	searchRequest := ldap.NewSearchRequest(
		l.UserDNSearchBaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		filter,
		[]string{}, // only need DN, so no pass no attributes here
		nil,
	)

	searchResult, err := conn.Search(searchRequest)
	if err != nil {
		return "", err
	}
	if len(searchResult.Entries) == 0 {
		return "", fmt.Errorf("User DN for %s not found", username)
	}
	if len(searchResult.Entries) != 1 {
		return "", fmt.Errorf("Multiple DNs for %s found - please fix the search filter", username)
	}
	return searchResult.Entries[0].DN, nil
}

// Bind - binds to ldap, searches LDAP and returns the distinguished name of the
// user and the list of groups.
func (l *Config) Bind(username, password string) (string, []string, error) {
	conn, err := l.Connect()
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()

	var bindDN string
	if l.isUsingLookupBind {
		// Bind to the lookup user account
		if err = l.lookupBind(conn); err != nil {
			return "", nil, err
		}

		// Lookup user DN
		bindDN, err = l.lookupUserDN(conn, username)
		if err != nil {
			errRet := fmt.Errorf("Unable to find user DN: %s", err)
			return "", nil, errRet
		}

		// Authenticate the user credentials.
		err = conn.Bind(bindDN, password)
		if err != nil {
			errRet := fmt.Errorf("LDAP auth failed for DN %s: %v", bindDN, err)
			return "", nil, errRet
		}

		// Bind to the lookup user account again to perform group search.
		if err = l.lookupBind(conn); err != nil {
			return "", nil, err
		}
	} else {
		// Verify login credentials by checking the username formats.
		bindDN, err = l.usernameFormatsBind(conn, username, password)
		if err != nil {
			return "", nil, err
		}

		// Bind to the successful bindDN again.
		err = conn.Bind(bindDN, password)
		if err != nil {
			errRet := fmt.Errorf("LDAP conn failed though auth for DN %s succeeded: %v", bindDN, err)
			return "", nil, errRet
		}
	}

	// User groups lookup.
	var groups []string
	if l.GroupSearchFilter != "" {
		for _, groupSearchBase := range l.GroupSearchBaseDistNames {
			filter := strings.Replace(l.GroupSearchFilter, "%s", ldap.EscapeFilter(username), -1)
			filter = strings.Replace(filter, "%d", ldap.EscapeFilter(bindDN), -1)
			searchRequest := ldap.NewSearchRequest(
				groupSearchBase,
				ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
				filter,
				nil,
				nil,
			)

			var newGroups []string
			newGroups, err = getGroups(conn, searchRequest)
			if err != nil {
				errRet := fmt.Errorf("Error finding groups of %s: %v", bindDN, err)
				return "", nil, errRet
			}

			groups = append(groups, newGroups...)
		}
	}

	return bindDN, groups, nil
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
	lookupBindDN := env.Get(EnvLookupBindDN, kvs.Get(LookupBindDN))
	lookupBindPassword := env.Get(EnvLookupBindPassword, kvs.Get(LookupBindPassword))
	if lookupBindDN != "" && lookupBindPassword != "" {
		l.LookupBindDN = lookupBindDN
		l.LookupBindPassword = lookupBindPassword
		l.isUsingLookupBind = true

		// User DN search configuration
		userDNSearchBaseDN := env.Get(EnvUserDNSearchBaseDN, kvs.Get(UserDNSearchBaseDN))
		userDNSearchFilter := env.Get(EnvUserDNSearchFilter, kvs.Get(UserDNSearchFilter))
		if userDNSearchFilter == "" || userDNSearchBaseDN == "" {
			return l, errors.New("In lookup bind mode, userDN search base DN and userDN search filter are both required")
		}
		l.UserDNSearchBaseDN = userDNSearchBaseDN
		l.UserDNSearchFilter = userDNSearchFilter
	}

	// Username format configuration.
	if v := env.Get(EnvUsernameFormat, kvs.Get(UsernameFormat)); v != "" {
		if !strings.Contains(v, "%s") {
			return l, errors.New("LDAP username format doesn't have '%s' substitution")
		}
		l.UsernameFormats = strings.Split(v, dnDelimiter)
	}

	// Either lookup bind mode or username format is supported, but not
	// both.
	if l.isUsingLookupBind && len(l.UsernameFormats) > 0 {
		return l, errors.New("Lookup Bind mode and Username Format mode are not supported at the same time")
	}

	// At least one of bind mode or username format must be used.
	if !l.isUsingLookupBind && len(l.UsernameFormats) == 0 {
		return l, errors.New("Either Lookup Bind mode or Username Format mode is required.")
	}

	// Group search params configuration
	grpSearchFilter := env.Get(EnvGroupSearchFilter, kvs.Get(GroupSearchFilter))
	grpSearchBaseDN := env.Get(EnvGroupSearchBaseDN, kvs.Get(GroupSearchBaseDN))

	// Either all group params must be set or none must be set.
	if (grpSearchFilter != "" && grpSearchBaseDN == "") || (grpSearchFilter == "" && grpSearchBaseDN != "") {
		return l, errors.New("All group related parameters must be set")
	}

	if grpSearchFilter != "" {
		l.GroupSearchFilter = grpSearchFilter
		l.GroupSearchBaseDistName = grpSearchBaseDN
		l.GroupSearchBaseDistNames = strings.Split(l.GroupSearchBaseDistName, dnDelimiter)
	}

	l.rootCAs = rootCAs
	return l, nil
}
