// Copyright (c) 2015-2024 MinIO, Inc.
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

package cmd

import (
	"testing"
)

func TestSplitPath(t *testing.T) {
	cases := []struct {
		path                          string
		secondIndex                   bool
		expectedListKey, expectedItem string
	}{
		{"format.json", false, "format.json", ""},
		{"users/tester.json", false, "users/", "tester.json"},
		{"groups/test/group.json", false, "groups/", "test/group.json"},
		{"policydb/groups/testgroup.json", true, "policydb/groups/", "testgroup.json"},
		{
			"policydb/sts-users/uid=slash/user,ou=people,ou=swengg,dc=min,dc=io.json", true,
			"policydb/sts-users/", "uid=slash/user,ou=people,ou=swengg,dc=min,dc=io.json",
		},
		{
			"policydb/sts-users/uid=slash/user/twice,ou=people,ou=swengg,dc=min,dc=io.json", true,
			"policydb/sts-users/", "uid=slash/user/twice,ou=people,ou=swengg,dc=min,dc=io.json",
		},
		{
			"policydb/groups/cn=project/d,ou=groups,ou=swengg,dc=min,dc=io.json", true,
			"policydb/groups/", "cn=project/d,ou=groups,ou=swengg,dc=min,dc=io.json",
		},
	}
	for i, test := range cases {
		listKey, item := splitPath(test.path, test.secondIndex)
		if listKey != test.expectedListKey || item != test.expectedItem {
			t.Errorf("unexpected result on test[%v]: expected[%s, %s] but got [%s, %s]", i, test.expectedListKey, test.expectedItem, listKey, item)
		}
	}
}
