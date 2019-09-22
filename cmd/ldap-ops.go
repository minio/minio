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

package cmd

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	ldap "gopkg.in/ldap.v3"
)

const (
	defaultLDAPExpiry = time.Hour * 1
)

// ldapServerConfig contains server connectivity information.
type ldapServerConfig struct {
	IsEnabled bool `json:"enabled"`

	// E.g. "ldap.minio.io:636"
	ServerAddr string `json:"serverAddr"`

	// STS credentials expiry duration
	STSExpiryDuration string        `json:"stsExpiryDuration"`
	stsExpiryDuration time.Duration // contains converted value

	// Skips TLS verification (for testing, not
	// recommended in production).
	SkipTLSVerify bool `json:"skipTLSverify"`

	// Format string for usernames
	UsernameFormat string `json:"usernameFormat"`

	GroupSearchBaseDN  string `json:"groupSearchBaseDN"`
	GroupSearchFilter  string `json:"groupSearchFilter"`
	GroupNameAttribute string `json:"groupNameAttribute"`
}

func (l *ldapServerConfig) Connect() (ldapConn *ldap.Conn, err error) {
	if l == nil {
		// Happens when LDAP is not configured.
		return
	}
	if l.SkipTLSVerify {
		ldapConn, err = ldap.DialTLS("tcp", l.ServerAddr, &tls.Config{InsecureSkipVerify: true})
	} else {
		ldapConn, err = ldap.DialTLS("tcp", l.ServerAddr, &tls.Config{})
	}
	return
}

// newLDAPConfigFromEnv loads configuration from the environment
func newLDAPConfigFromEnv() (l ldapServerConfig, err error) {
	if ldapServer, ok := os.LookupEnv("MINIO_IDENTITY_LDAP_SERVER_ADDR"); ok {
		l.IsEnabled = true
		l.ServerAddr = ldapServer

		if v := os.Getenv("MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY"); v == "true" {
			l.SkipTLSVerify = true
		}

		if v := os.Getenv("MINIO_IDENTITY_LDAP_STS_EXPIRY"); v != "" {
			expDur, err := time.ParseDuration(v)
			if err != nil {
				return l, errors.New("LDAP expiry time err:" + err.Error())
			}
			if expDur <= 0 {
				return l, errors.New("LDAP expiry time has to be positive")
			}
			l.STSExpiryDuration = v
			l.stsExpiryDuration = expDur
		} else {
			l.stsExpiryDuration = defaultLDAPExpiry
		}

		if v := os.Getenv("MINIO_IDENTITY_LDAP_USERNAME_FORMAT"); v != "" {
			subs := newSubstituter("username", "test")
			if _, err := subs.substitute(v); err != nil {
				return l, errors.New("Only username may be substituted in the username format")
			}
			l.UsernameFormat = v
		}

		grpSearchFilter := os.Getenv("MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER")
		grpSearchNameAttr := os.Getenv("MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE")
		grpSearchBaseDN := os.Getenv("MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN")

		// Either all group params must be set or none must be set.
		allNotSet := grpSearchFilter == "" && grpSearchNameAttr == "" && grpSearchBaseDN == ""
		allSet := grpSearchFilter != "" && grpSearchNameAttr != "" && grpSearchBaseDN != ""
		if !allNotSet && !allSet {
			return l, errors.New("All group related parameters must be set")
		}

		if allSet {
			subs := newSubstituter("username", "test", "usernamedn", "test2")
			if _, err := subs.substitute(grpSearchFilter); err != nil {
				return l, errors.New("Only username and usernamedn may be substituted in the group search filter string")
			}
			l.GroupSearchFilter = grpSearchFilter

			l.GroupNameAttribute = grpSearchNameAttr

			subs = newSubstituter("username", "test", "usernamedn", "test2")
			if _, err := subs.substitute(grpSearchBaseDN); err != nil {
				return l, errors.New("Only username and usernamedn may be substituted in the base DN string")
			}
			l.GroupSearchBaseDN = grpSearchBaseDN
		}
	}
	return
}

// substituter - This type is to allow restricted runtime
// substitutions of variables in LDAP configuration items during
// runtime.
type substituter struct {
	vals map[string]string
}

// newSubstituter - sets up the substituter for usage, for e.g.:
//
//  subber := newSubstituter("username", "john")
func newSubstituter(v ...string) substituter {
	if len(v)%2 != 0 {
		log.Fatal("Need an even number of arguments")
	}
	vals := make(map[string]string)
	for i := 0; i < len(v); i += 2 {
		vals[v[i]] = v[i+1]
	}
	return substituter{vals: vals}
}

// substitute - performs substitution on the given string `t`. Returns
// an error if there are any variables in the input that do not have
// values in the substituter. E.g.:
//
// subber.substitute("uid=${username},cn=users,dc=example,dc=com")
//
// returns "uid=john,cn=users,dc=example,dc=com"
//
// whereas:
//
// subber.substitute("uid=${usernamedn}")
//
// returns an error.
func (s *substituter) substitute(t string) (string, error) {
	for k, v := range s.vals {
		re := regexp.MustCompile(fmt.Sprintf(`\$\{%s\}`, k))
		t = re.ReplaceAllLiteralString(t, v)
	}
	// Check if all requested substitutions have been made.
	re := regexp.MustCompile(`\$\{.*\}`)
	if re.MatchString(t) {
		return "", errors.New("unsupported substitution requested")
	}
	return t, nil
}
