// Copyright (c) 2015-2021 MinIO, Inc.
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
	"testing"

	ldap "github.com/go-ldap/ldap/v3"
)

func TestLDAPGetDNStr(t *testing.T) {
	mainDNStr := "uid=dillon,ou=itdept,dc=example,dc=com"
	// case-sensitive equal cases
	equalCSVariations := []string{
		"uid=dillon,ou=itdept,dc=example,dc=com",
		"  uid=dillon ,ou=itdept,dc = example , dc=com  ",
		"UID=dillon,ou=itdept,dc=example,dc=com",
		"UID=dillon,OU=itdept,DC=example,DC=com",
		"UID=dillon, OU=itdept  ,  DC = example,DC=   com",
	}
	// case-insensitive equal cases
	equalCISVariations := []string{
		"uid=DILLON,ou=itdept,dc=example,dc=com",
		"uid=Dillon,ou=ITDept,dc=Example,dc=Com",
		"uid=Dillon,ou=ITDept,dc=Example,dc=Com",
		"UID=DILLON,OU=ITDEPT,DC=EXAMPLE,DC=COM",
	}
	// non-equal cases, regardless of casing
	nonEqualVariations := []string{
		"uid=dillon,ou=it dept,dc=example,dc=com",
		"uid=dillon ,ou=itdept,dc = example , dc=in  ",
		"UID=DILLON\\, III.,OU=ITDEPT,DC=EXAMPLE,DC=COM",
		"UID=DILLON\\, III.+CN=xx,OU=ITDEPT,DC=EXAMPLE,DC=COM",
	}

	mainDN, err := ldap.ParseDN(mainDNStr)
	if err != nil {
		t.Fatalf("unexpected parse error")
	}

	mainDNCaseSensitive := GetDNStr(mainDN, true)
	mainDNCaseInsensitive := GetDNStr(mainDN, false)

	for _, s := range equalCSVariations {
		dn, err := ldap.ParseDN(s)
		if err != nil {
			t.Fatalf("error parsing DN %s: %v", s, err)
		}
		if mainDNCaseSensitive != GetDNStr(dn, true) {
			t.Errorf("[CaseSensitiveEquals] %s was not DN equal to %s", s, mainDNStr)
		}
	}

	for _, s := range append(equalCSVariations, equalCISVariations...) {
		dn, err := ldap.ParseDN(s)
		if err != nil {
			t.Fatalf("error parsing DN %s: %v", s, err)
		}
		if mainDNCaseInsensitive != GetDNStr(dn, false) {
			t.Errorf("[CaseInsensitiveEquals] %s was not DN equal to %s", s, mainDNStr)
		}
	}

	for _, s := range nonEqualVariations {
		dn, err := ldap.ParseDN(s)
		if err != nil {
			t.Errorf("error parsing %s: %v", s, err)
			continue
		}
		if mainDNCaseSensitive == GetDNStr(dn, true) {
			t.Errorf("[CaseSensitiveNE] %s was DN equal to %s", s, mainDNStr)
		}
		if mainDNCaseInsensitive == GetDNStr(dn, false) {
			t.Errorf("[CaseInsensitiveNE] %s was DN equal to %s", s, mainDNStr)
		}
	}
}
