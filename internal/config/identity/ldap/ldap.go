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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	xldap "github.com/minio/pkg/v2/ldap"
)

// LookupUserDN searches for the full DN and groups of a given short/login
// username.
func (l *Config) LookupUserDN(username string) (string, []string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", nil, err
	}

	// Lookup user DN
	bindDN, err := l.LDAP.LookupUserDN(conn, username)
	if err != nil {
		errRet := fmt.Errorf("Unable to find user DN: %w", err)
		return "", nil, errRet
	}

	groups, err := l.LDAP.SearchForUserGroups(conn, username, bindDN)
	if err != nil {
		return "", nil, err
	}

	return bindDN, groups, nil
}

// GetValidatedDNForUsername checks if the given username exists in the LDAP directory.
// The given username could be just the short "login" username or the full DN.
//
// When the username/DN is found, the full DN returned by the **server** is
// returned, otherwise the returned string is empty. The value returned here is
// the value sent by the LDAP server and is used in minio as the server performs
// LDAP specific normalization (including Unicode normalization).
//
// If the user is not found, err = nil, otherwise, err != nil.
func (l *Config) GetValidatedDNForUsername(username string) (string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", err
	}

	// Check if the passed in username is a valid DN.
	if !l.ParsesAsDN(username) {
		// We consider it as a login username and attempt to check it exists in
		// the directory.
		bindDN, err := l.LDAP.LookupUserDN(conn, username)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return "", nil
			}
			return "", fmt.Errorf("Unable to find user DN: %w", err)
		}
		return bindDN, nil
	}

	// Since the username is a valid DN, check that it is under a configured
	// base DN in the LDAP directory.

	// Check that userDN exists in the LDAP directory.
	validatedUserDN, err := xldap.LookupDN(conn, username)
	if err != nil {
		return "", fmt.Errorf("Error looking up user DN %s: %w", username, err)
	}
	if validatedUserDN == "" {
		return "", nil
	}

	// This will return an error as the argument is validated to be a DN.
	udn, _ := ldap.ParseDN(validatedUserDN)

	// Check that the user DN is under a configured user base DN in the LDAP
	// directory.
	for _, baseDN := range l.LDAP.UserDNSearchBaseDistNames {
		if baseDN.Parsed.AncestorOf(udn) {
			return validatedUserDN, nil
		}
	}

	return "", fmt.Errorf("User DN %s is not under any configured user base DN", validatedUserDN)
}

// GetValidatedGroupDN checks if the given group DN exists in the LDAP directory
// and returns the group DN sent by the LDAP server. The value returned by the
// server may not be equal to the input group DN, as LDAP equality is not a
// simple Golang string equality. However, we assume the value returned by the
// LDAP server is canonical.
//
// If the group is not found in the LDAP directory, the returned string is empty
// and err = nil.
func (l *Config) GetValidatedGroupDN(groupDN string) (string, error) {
	if len(l.LDAP.GroupSearchBaseDistNames) == 0 {
		return "", errors.New("no group search Base DNs given")
	}

	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", err
	}

	// Check that groupDN exists in the LDAP directory.
	validatedGroupDN, err := xldap.LookupDN(conn, groupDN)
	if err != nil {
		return "", fmt.Errorf("Error looking up group DN %s: %w", groupDN, err)
	}
	if validatedGroupDN == "" {
		return "", nil
	}

	gdn, err := ldap.ParseDN(validatedGroupDN)
	if err != nil {
		return "", fmt.Errorf("Given group DN %s could not be parsed: %w", validatedGroupDN, err)
	}

	// Check that the group DN is under a configured group base DN in the LDAP
	// directory.
	for _, baseDN := range l.LDAP.GroupSearchBaseDistNames {
		if baseDN.Parsed.AncestorOf(gdn) {
			return validatedGroupDN, nil
		}
	}
	return "", fmt.Errorf("Group DN %s is not under any configured group base DN", validatedGroupDN)
}

// Bind - binds to ldap, searches LDAP and returns the distinguished name of the
// user and the list of groups.
func (l *Config) Bind(username, password string) (string, []string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return "", nil, err
	}
	defer conn.Close()

	var bindDN string
	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", nil, err
	}

	// Lookup user DN
	bindDN, err = l.LDAP.LookupUserDN(conn, username)
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
	if err = l.LDAP.LookupBind(conn); err != nil {
		return "", nil, err
	}

	// User groups lookup.
	groups, err := l.LDAP.SearchForUserGroups(conn, username, bindDN)
	if err != nil {
		return "", nil, err
	}

	return bindDN, groups, nil
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

// ParsesAsDN determines if the given string could be a valid DN based on
// parsing alone.
func (l Config) ParsesAsDN(dn string) bool {
	_, err := ldap.ParseDN(dn)
	return err == nil
}

// IsLDAPUserDN determines if the given string could be a user DN from LDAP.
func (l Config) IsLDAPUserDN(user string) bool {
	udn, err := ldap.ParseDN(user)
	if err != nil {
		return false
	}
	for _, baseDN := range l.LDAP.UserDNSearchBaseDistNames {
		if baseDN.Parsed.AncestorOf(udn) {
			return true
		}
	}
	return false
}

// IsLDAPGroupDN determines if the given string could be a group DN from LDAP.
func (l Config) IsLDAPGroupDN(group string) bool {
	gdn, err := ldap.ParseDN(group)
	if err != nil {
		return false
	}
	for _, baseDN := range l.LDAP.GroupSearchBaseDistNames {
		if baseDN.Parsed.AncestorOf(gdn) {
			return true
		}
	}
	return false
}

// GetNonEligibleUserDistNames - find user accounts (DNs) that are no longer
// present in the LDAP server or do not meet filter criteria anymore
func (l *Config) GetNonEligibleUserDistNames(userDistNames []string) ([]string, error) {
	conn, err := l.LDAP.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return nil, err
	}

	// Evaluate the filter again with generic wildcard instead of  specific values
	filter := strings.ReplaceAll(l.LDAP.UserDNSearchFilter, "%s", "*")

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
	conn, err := l.LDAP.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = l.LDAP.LookupBind(conn); err != nil {
		return nil, err
	}

	res := make(map[string]set.StringSet, len(userDistNames))
	for _, userDistName := range userDistNames {
		username := userDNToUsernameMap[userDistName]
		groups, err := l.LDAP.SearchForUserGroups(conn, username, userDistName)
		if err != nil {
			return nil, err
		}
		res[userDistName] = set.CreateStringSet(groups...)
	}

	return res, nil
}
