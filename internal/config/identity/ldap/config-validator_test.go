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
	"os"
	"testing"

	"github.com/minio/minio-go/v7/pkg/set"
)

const (
	EnvTestLDAPServer = "LDAP_TEST_SERVER"
)

func TestConfigValidator(t *testing.T) {
	ldapServer := os.Getenv(EnvTestLDAPServer)
	if ldapServer == "" {
		t.Skip()
	}
	testCases := []struct {
		cfg            Config
		expectedResult Result
	}{
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				return v
			}(),
			expectedResult: ConnectivityError,
		},
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				return v
			}(),
			expectedResult: ConnectivityError,
		},
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				return v
			}(),
			expectedResult: LookupBindError,
		},
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin1"
				return v
			}(),
			expectedResult: LookupBindError,
		},
		{ // Case 4
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin"
				return v
			}(),
			expectedResult: UserSearchParamsMisconfigured,
		},
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin"
				v.UserDNSearchFilter = "(uid=x)"
				v.UserDNSearchBaseDistName = "dc=min,dc=io"
				return v
			}(),
			expectedResult: UserSearchParamsMisconfigured,
		},
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin"
				v.UserDNSearchFilter = "(uid=%s)"
				v.UserDNSearchBaseDistName = "dc=min,dc=io"
				return v
			}(),
			expectedResult: ConfigOk,
		},
		{ // Case 7
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin"
				v.UserDNSearchFilter = "(uid=%s)"
				v.UserDNSearchBaseDistName = "dc=min,dc=io"
				v.GroupSearchBaseDistName = "ou=swengg,dc=min,dc=io"
				v.GroupSearchFilter = "(&(objectclass=groupofnames)(member=x))"
				return v
			}(),
			expectedResult: GroupSearchParamsMisconfigured,
		},
		{
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin"
				v.UserDNSearchFilter = "(uid=%s)"
				v.UserDNSearchBaseDistName = "dc=min,dc=io"
				v.GroupSearchFilter = "(&(objectclass=groupofnames)(member=x))"
				return v
			}(),
			expectedResult: GroupSearchParamsMisconfigured,
		},
		{ // Case 9
			cfg: func() Config {
				v := Config{Enabled: true}
				v.ServerAddr = ldapServer
				v.serverInsecure = true
				v.LookupBindDN = "cn=admin,dc=min,dc=io"
				v.LookupBindPassword = "admin"
				v.UserDNSearchFilter = "(uid=%s)"
				v.UserDNSearchBaseDistName = "dc=min,dc=io"
				v.GroupSearchBaseDistName = "ou=swengg,dc=min,dc=io"
				v.GroupSearchFilter = "(&(objectclass=groupofnames)(member=%d))"
				return v
			}(),
			expectedResult: ConfigOk,
		},
	}

	expectedDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
	expectedGroups := set.CreateStringSet(
		"cn=projecta,ou=groups,ou=swengg,dc=min,dc=io",
		"cn=projectb,ou=groups,ou=swengg,dc=min,dc=io",
	)

	for i, test := range testCases {
		result := test.cfg.Validate()
		if result.Result != test.expectedResult {
			fmt.Printf("Result: %#v\n", result)
			t.Fatalf("Case %d: Got `%s` expected `%s`", i, result.Result, string(test.expectedResult))
		}
		if result.IsOk() {
			lookupResult, validationResult := test.cfg.ValidateLookup("dillon")
			if !validationResult.IsOk() {
				t.Fatalf("Case %d: Got unexpected validation failure: %#v\n", i, validationResult)
			}
			if lookupResult.DN != expectedDN {
				t.Fatalf("Case %d: Got unexpected DN: %v", i, lookupResult.DN)
			}

			if test.cfg.GroupSearchFilter == "" {
				continue
			}

			if !set.CreateStringSet(lookupResult.GroupDNMemberships...).Equals(expectedGroups) {
				t.Fatalf("Case %d: Got unexpected groups: %v", i, lookupResult.GroupDNMemberships)
			}
		}
	}
}
