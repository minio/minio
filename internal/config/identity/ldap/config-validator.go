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
	"fmt"
	"strings"
)

// Result - type for high-level names for the validation status of the config.
type Result string

// Constant values for Result type.
const (
	ConfigOk                       Result = "Config OK"
	ConnectivityError              Result = "LDAP Server Connection Error"
	LookupBindError                Result = "LDAP Lookup Bind Error"
	UserSearchParamsMisconfigured  Result = "User Search Parameters Misconfigured"
	GroupSearchParamsMisconfigured Result = "Group Search Parameters Misconfigured"
	UserDNLookupError              Result = "User DN Lookup Error"
	GroupMembershipsLookupError    Result = "Group Memberships Lookup Error"
)

// Validation returns feedback on the configuration. The `Suggestion` field
// needs to be "printed" for friendly display (it can contain escaped newlines
// `\n`).
type Validation struct {
	Result     Result
	Detail     string
	Suggestion string
	ErrCause   error
}

// Error instance for Validation.
func (v Validation) Error() string {
	if v.Result == ConfigOk {
		return ""
	}
	return fmt.Sprintf("%s: %s", string(v.Result), v.Detail)
}

// IsOk - returns if the validation succeeded.
func (v Validation) IsOk() bool {
	return v.Result == ConfigOk
}

// UserLookupResult returns the DN found for the test user and their group
// memberships.
type UserLookupResult struct {
	DN                 string
	GroupDNMemberships []string
}

// Validate validates the LDAP configuration. It can be called with any subset
// of configuration parameters provided by the user - it will return
// information on what needs to be done to fix the problem if any.
//
// This function updates the UserDNSearchBaseDistNames and
// GroupSearchBaseDistNames fields of the Config - however this an idempotent
// operation. This is done to support configuration validation in Console/mc and
// for tests.
func (l *Config) Validate() Validation {
	if !l.Enabled {
		return Validation{Result: ConfigOk, Detail: "Config is not enabled"}
	}

	if l.ServerAddr == "" {
		return Validation{
			Result:     ConnectivityError,
			Detail:     "Address is empty",
			Suggestion: "Set a server address.",
		}
	}

	conn, err := l.Connect()
	if err != nil {
		return Validation{
			Result:   ConnectivityError,
			Detail:   fmt.Sprintf("Could not connect to LDAP server: %v", err),
			ErrCause: err,
			Suggestion: `Check:
    (1) server address
    (2) TLS parameters, and
    (3) LDAP server's TLS certificate is trusted by MinIO (when using TLS - highly recommended)`,
		}
	}
	defer conn.Close()

	if l.LookupBindDN == "" {
		return Validation{
			Result:     LookupBindError,
			Detail:     "Lookup Bind UserDN not specified",
			Suggestion: "Specify LDAP service account credentials for performing lookups.",
		}
	}
	if err := l.lookupBind(conn); err != nil {
		return Validation{
			Result:     LookupBindError,
			ErrCause:   err,
			Detail:     fmt.Sprintf("Error connecting as LDAP Lookup Bind user: %v", err),
			Suggestion: "Check LDAP Lookup Bind user credentials and if user is allowed to login",
		}
	}

	// Validate User Lookup parameters
	if l.UserDNSearchBaseDistName == "" {
		return Validation{
			Result:     UserSearchParamsMisconfigured,
			Detail:     "UserDN search base is empty",
			Suggestion: "Set the UserDN search base to the DN of the directory subtree where users are present",
		}
	}
	l.UserDNSearchBaseDistNames = strings.Split(l.UserDNSearchBaseDistName, dnDelimiter)

	if l.UserDNSearchFilter == "" {
		return Validation{
			Result: UserSearchParamsMisconfigured,
			Detail: "UserDN search filter is empty",
			Suggestion: `Set the UserDN search filter template:
    Use "%s" - it will be replaced by the login user name and sent to the LDAP server.
    For example: "(uid=%s)"`,
		}
	}
	if strings.Contains(l.UserDNSearchFilter, "%d") {
		return Validation{
			Result: UserSearchParamsMisconfigured,
			Detail: "User DN search filter contains `%d`",
			Suggestion: `User DN search filter is a template where "%s" is replaced by the login username.
    "%d" is not supported here.
    Please provide a search filter containing "%s"`,
		}
	}
	if !strings.Contains(l.UserDNSearchFilter, "%s") {
		return Validation{
			Result: UserSearchParamsMisconfigured,
			Detail: "User DN search filter does not contain `%s`",
			Suggestion: `During login, the user's DN is looked up using the search filter template:
    "%s" gets replaced by the given username - it must be used.
    Enter an LDAP search filter containing "%s"`,
		}
	}

	// If group lookup is not configured, it's ok.
	if l.GroupSearchBaseDistName != "" || l.GroupSearchFilter != "" {

		// Validate Group Search parameters as they are given.
		if l.GroupSearchBaseDistName == "" {
			return Validation{
				Result: GroupSearchParamsMisconfigured,
				Detail: "Group Search Base DN is required.",
				Suggestion: `Since you entered a value for the Group Search Filter - enter a value for the Group Search Base DN too:
    Enter this value as the DN of the subtree where groups will be found.`,
			}
		}
		l.GroupSearchBaseDistNames = strings.Split(l.GroupSearchBaseDistName, dnDelimiter)

		if l.GroupSearchFilter == "" {
			return Validation{
				Result: GroupSearchParamsMisconfigured,
				Detail: "Group Search Filter is required.",
				Suggestion: `Since you entered a value for the Group Search Base DN - enter a value for the Group Search Filter too. This is a template where, before the query is sent to the server:
    "%s" is replaced with the login username;
    "%d" is replaced with the DN of the login user.
    For example: "(&(objectclass=groupOfNames)(memberUid=%s))"`,
			}
		}

		if !strings.Contains(l.GroupSearchFilter, "%d") && !strings.Contains(l.GroupSearchFilter, "%s") {
			return Validation{
				Result: GroupSearchParamsMisconfigured,
				Detail: `GroupSearchFilter must contain at least one of "%s" or "%d"`,
				Suggestion: `During group membership lookup the group search filter template is used:
    "%s" gets replaced by the given username, and
    "%d" gets replaced by the user's DN.
    Either one is needed to find only groups that the user is a member of.
    Enter an LDAP search filter template using at least one of these.`,
			}
		}
	}

	return Validation{
		Result: ConfigOk,
	}
}

// ValidateLookup takes a test username and performs user and group lookup (if
// configured) and returns the result. It is to validate the LDAP configuration.
// The lookup is performed without requiring the password for the test user -
// and so can be used to test any LDAP user intending to use MinIO.
func (l *Config) ValidateLookup(testUsername string) (*UserLookupResult, Validation) {
	if testUsername == "" {
		return nil, Validation{
			Result: UserDNLookupError,
			Detail: "Provided username is empty",
		}
	}

	if r := l.Validate(); !r.IsOk() {
		return nil, r
	}

	conn, err := l.Connect()
	if err != nil {
		return nil, Validation{
			Result:   ConnectivityError,
			Detail:   fmt.Sprintf("Could not connect to LDAP server: %v", err),
			ErrCause: err,
			Suggestion: `Check:
    (1) server address
    (2) TLS parameters, and
    (3) LDAP server's TLS certificate is trusted by MinIO (when using TLS - highly recommended)`,
		}
	}
	defer conn.Close()

	if err := l.lookupBind(conn); err != nil {
		return nil, Validation{
			Result:     LookupBindError,
			ErrCause:   err,
			Detail:     fmt.Sprintf("Error connecting as LDAP Lookup Bind user: %v", err),
			Suggestion: "Check LDAP Lookup Bind user credentials and if user is allowed to login",
		}
	}

	// Lookup the given username.
	dn, err := l.lookupUserDN(conn, testUsername)
	if err != nil {
		return nil, Validation{
			Result:   UserDNLookupError,
			Detail:   fmt.Sprintf("Got an error when looking up user (%s) DN: %v", testUsername, err),
			ErrCause: err,
			Suggestion: `Check if this is a temporary error and try again.
    Perhaps there is an error in the user search filter or user search base DN.`,
		}
	}

	// Lookup groups.
	groups, err := l.searchForUserGroups(conn, testUsername, dn)
	if err != nil {
		return nil, Validation{
			Result:   GroupMembershipsLookupError,
			Detail:   fmt.Sprintf("Got an error when looking up groups for user(=>%s, dn=>%s): %v", testUsername, dn, err),
			ErrCause: err,
			Suggestion: `Check if this is a temporary error and try again.
    Perhaps there is an error in the group search filter or group search base DN.`,
		}
	}

	return &UserLookupResult{
			DN:                 dn,
			GroupDNMemberships: groups,
		}, Validation{
			Result: ConfigOk,
			Detail: "User lookup done.",
		}
}
