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

package ldap

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
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
	var err error
	if l.LookupBindPassword == "" {
		err = conn.UnauthenticatedBind(l.LookupBindDN)
	} else {
		err = conn.Bind(l.LookupBindDN, l.LookupBindPassword)
	}
	if ldap.IsErrorWithCode(err, 49) {
		return fmt.Errorf("LDAP Lookup Bind user invalid credentials error: %w", err)
	}
	return err
}

// lookupUserDN searches for the DN of the user given their username. conn is
// assumed to be using the lookup bind service account. It is required that the
// search result in at most one result.
func (l *Config) lookupUserDN(conn *ldap.Conn, username string) (string, error) {
	filter := strings.ReplaceAll(l.UserDNSearchFilter, "%s", ldap.EscapeFilter(username))
	var foundDistNames []string
	for _, userSearchBase := range l.UserDNSearchBaseDistNames {
		searchRequest := ldap.NewSearchRequest(
			userSearchBase,
			ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
			filter,
			[]string{}, // only need DN, so no pass no attributes here
			nil,
		)

		searchResult, err := conn.Search(searchRequest)
		if err != nil {
			return "", err
		}

		for _, entry := range searchResult.Entries {
			foundDistNames = append(foundDistNames, entry.DN)
		}
	}
	if len(foundDistNames) == 0 {
		return "", fmt.Errorf("User DN for %s not found", username)
	}
	if len(foundDistNames) != 1 {
		return "", fmt.Errorf("Multiple DNs for %s found - please fix the search filter", username)
	}
	return foundDistNames[0], nil
}

func (l *Config) searchForUserGroups(conn *ldap.Conn, username, bindDN string) ([]string, error) {
	// User groups lookup.
	var groups []string
	if l.GroupSearchFilter != "" {
		for _, groupSearchBase := range l.GroupSearchBaseDistNames {
			filter := strings.ReplaceAll(l.GroupSearchFilter, "%s", ldap.EscapeFilter(username))
			filter = strings.ReplaceAll(filter, "%d", ldap.EscapeFilter(bindDN))
			searchRequest := ldap.NewSearchRequest(
				groupSearchBase,
				ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
				filter,
				nil,
				nil,
			)

			var newGroups []string
			newGroups, err := getGroups(conn, searchRequest)
			if err != nil {
				errRet := fmt.Errorf("Error finding groups of %s: %w", bindDN, err)
				return nil, errRet
			}

			groups = append(groups, newGroups...)
		}
	}

	return groups, nil
}

// LookupUserDN searches for the full DN and groups of a given username
func (l *Config) LookupUserDN(username string) (string, []string, error) {
	conn, err := l.Connect()
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.lookupBind(conn); err != nil {
		return "", nil, err
	}

	// Lookup user DN
	bindDN, err := l.lookupUserDN(conn, username)
	if err != nil {
		errRet := fmt.Errorf("Unable to find user DN: %w", err)
		return "", nil, errRet
	}

	groups, err := l.searchForUserGroups(conn, username, bindDN)
	if err != nil {
		return "", nil, err
	}

	return bindDN, groups, nil
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
	// Bind to the lookup user account
	if err = l.lookupBind(conn); err != nil {
		return "", nil, err
	}

	// Lookup user DN
	bindDN, err = l.lookupUserDN(conn, username)
	if err != nil {
		errRet := fmt.Errorf("Unable to find user DN: %w", err)
		return "", nil, errRet
	}

	// Authenticate the user credentials.
	err = conn.Bind(bindDN, password)
	if err != nil {
		errRet := fmt.Errorf("LDAP auth failed for DN %s: %w", bindDN, err)
		return "", nil, errRet
	}

	// Bind to the lookup user account again to perform group search.
	if err = l.lookupBind(conn); err != nil {
		return "", nil, err
	}

	// User groups lookup.
	groups, err := l.searchForUserGroups(conn, username, bindDN)
	if err != nil {
		return "", nil, err
	}

	return bindDN, groups, nil
}

// Connect connect to ldap server.
func (l *Config) Connect() (ldapConn *ldap.Conn, err error) {
	if l == nil {
		return nil, errors.New("LDAP is not configured")
	}

	_, _, err = net.SplitHostPort(l.ServerAddr)
	if err != nil {
		// User default LDAP port if none specified "636"
		l.ServerAddr = net.JoinHostPort(l.ServerAddr, "636")
	}

	if l.serverInsecure {
		return ldap.Dial("tcp", l.ServerAddr)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: l.tlsSkipVerify,
		RootCAs:            l.rootCAs,
	}

	if l.serverStartTLS {
		conn, err := ldap.Dial("tcp", l.ServerAddr)
		if err != nil {
			return nil, err
		}
		err = conn.StartTLS(tlsConfig)
		return conn, err
	}

	return ldap.DialTLS("tcp", l.ServerAddr, tlsConfig)
}

// GetExpiryDuration - return parsed expiry duration.
func (l Config) GetExpiryDuration(dsecs string) (time.Duration, error) {
	if dsecs == "" {
		return l.stsExpiryDuration, nil
	}

	d, err := strconv.Atoi(dsecs)
	if err != nil {
		return 0, auth.ErrInvalidDuration
	}

	dur := time.Duration(d) * time.Second

	if dur < minLDAPExpiry || dur > maxLDAPExpiry {
		return 0, auth.ErrInvalidDuration
	}
	return dur, nil
}

// IsLDAPUserDN determines if the given string could be a user DN from LDAP.
func (l Config) IsLDAPUserDN(user string) bool {
	for _, baseDN := range l.UserDNSearchBaseDistNames {
		if strings.HasSuffix(user, ","+baseDN) {
			return true
		}
	}
	return false
}

// GetNonEligibleUserDistNames - find user accounts (DNs) that are no longer
// present in the LDAP server or do not meet filter criteria anymore
func (l *Config) GetNonEligibleUserDistNames(userDistNames []string) ([]string, error) {
	conn, err := l.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.lookupBind(conn); err != nil {
		return nil, err
	}

	// Evaluate the filter again with generic wildcard instead of  specific values
	filter := strings.ReplaceAll(l.UserDNSearchFilter, "%s", "*")

	nonExistentUsers := []string{}
	for _, dn := range userDistNames {
		searchRequest := ldap.NewSearchRequest(
			dn,
			ldap.ScopeBaseObject, ldap.NeverDerefAliases, 0, 0, false,
			filter,
			[]string{}, // only need DN, so pass no attributes here
			nil,
		)

		searchResult, err := conn.Search(searchRequest)
		if err != nil {
			// Object does not exist error?
			if ldap.IsErrorWithCode(err, 32) {
				nonExistentUsers = append(nonExistentUsers, dn)
				continue
			}
			return nil, err
		}
		if len(searchResult.Entries) == 0 {
			// DN was not found - this means this user account is
			// expired.
			nonExistentUsers = append(nonExistentUsers, dn)
		}
	}
	return nonExistentUsers, nil
}

// LookupGroupMemberships - for each DN finds the set of LDAP groups they are a
// member of.
func (l *Config) LookupGroupMemberships(userDistNames []string, userDNToUsernameMap map[string]string) (map[string]set.StringSet, error) {
	conn, err := l.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.lookupBind(conn); err != nil {
		return nil, err
	}

	res := make(map[string]set.StringSet, len(userDistNames))
	for _, userDistName := range userDistNames {
		username := userDNToUsernameMap[userDistName]
		groups, err := l.searchForUserGroups(conn, username, userDistName)
		if err != nil {
			return nil, err
		}
		res[userDistName] = set.CreateStringSet(groups...)
	}

	return res, nil
}
