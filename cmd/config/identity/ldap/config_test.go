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
	"testing"
)

func TestSubstituter(t *testing.T) {
	tests := []struct {
		KV               []string
		SubstitutableStr string
		SubstitutedStr   string
		ErrExpected      bool
	}{
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "uid=${username},cn=users,dc=example,dc=com",
			SubstitutedStr:   "uid=john,cn=users,dc=example,dc=com",
			ErrExpected:      false,
		},
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "uid={username},cn=users,dc=example,dc=com",
			SubstitutedStr:   "uid=john,cn=users,dc=example,dc=com",
			ErrExpected:      false,
		},
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "(&(objectclass=group)(member=${username}))",
			SubstitutedStr:   "(&(objectclass=group)(member=john))",
			ErrExpected:      false,
		},
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "(&(objectclass=group)(member={username}))",
			SubstitutedStr:   "(&(objectclass=group)(member=john))",
			ErrExpected:      false,
		},
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "uid=${{username}},cn=users,dc=example,dc=com",
			ErrExpected:      true,
		},
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "uid=${usernamedn},cn=users,dc=example,dc=com",
			ErrExpected:      true,
		},
		{
			KV:               []string{"username"},
			SubstitutableStr: "uid=${usernamedn},cn=users,dc=example,dc=com",
			ErrExpected:      true,
		},
		{
			KV:               []string{"username", "john"},
			SubstitutableStr: "(&(objectclass=user)(sAMAccountName={username})(memberOf=CN=myorg,OU=Rialto,OU=Application Managed,OU=Groups,DC=amr,DC=corp,DC=myorg,DC=com))",
			SubstitutedStr:   "(&(objectclass=user)(sAMAccountName=john)(memberOf=CN=myorg,OU=Rialto,OU=Application Managed,OU=Groups,DC=amr,DC=corp,DC=myorg,DC=com))",
			ErrExpected:      false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.SubstitutableStr, func(t *testing.T) {
			subber, err := NewSubstituter(test.KV...)
			if err != nil && !test.ErrExpected {
				t.Errorf("Unexpected failure %s", err)
			}
			gotStr, err := subber.Substitute(test.SubstitutableStr)
			if err != nil && !test.ErrExpected {
				t.Errorf("Unexpected failure %s", err)
			}
			if gotStr != test.SubstitutedStr {
				t.Errorf("Expected %s, got %s", test.SubstitutedStr, gotStr)
			}
		})
	}
}
