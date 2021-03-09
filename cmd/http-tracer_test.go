/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"testing"
)

// Test redactLDAPPwd()
func TestRedactLDAPPwd(t *testing.T) {
	testCases := []struct {
		query         string
		expectedQuery string
	}{
		{"", ""},
		{"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&LDAPPassword=can+youreadthis%3F&Version=2011-06-15",
			"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&LDAPPassword=*REDACTED*&Version=2011-06-15",
		},
		{"LDAPPassword=can+youreadthis%3F&Version=2011-06-15&?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername",
			"LDAPPassword=*REDACTED*&Version=2011-06-15&?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername",
		},
		{"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&Version=2011-06-15&LDAPPassword=can+youreadthis%3F",
			"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&Version=2011-06-15&LDAPPassword=*REDACTED*",
		},
		{
			"?x=y&a=b",
			"?x=y&a=b",
		},
	}
	for i, test := range testCases {
		gotQuery := redactLDAPPwd(test.query)
		if gotQuery != test.expectedQuery {
			t.Fatalf("test %d: expected %s got %s", i+1, test.expectedQuery, gotQuery)
		}
	}
}
