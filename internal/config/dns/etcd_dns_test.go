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

package dns

import "testing"

func TestDNSJoin(t *testing.T) {
	tests := []struct {
		in  []string
		out string
	}{
		{[]string{"bla", "bliep", "example", "org"}, "bla.bliep.example.org."},
		{[]string{"example", "."}, "example."},
		{[]string{"example", "org."}, "example.org."}, // technically we should not be called like this.
		{[]string{"."}, "."},
	}

	for i, tc := range tests {
		if x := dnsJoin(tc.in...); x != tc.out {
			t.Errorf("Test %d, expected %s, got %s", i, tc.out, x)
		}
	}
}

func TestPath(t *testing.T) {
	for _, path := range []string{"mydns", "skydns"} {
		result := msgPath("service.staging.skydns.local.", path)
		if result != etcdPathSeparator+path+"/local/skydns/staging/service" {
			t.Errorf("Failure to get domain's path with prefix: %s", result)
		}
	}
}

func TestUnPath(t *testing.T) {
	result1 := msgUnPath("/skydns/local/cluster/staging/service/")
	if result1 != "service.staging.cluster.local.skydns" {
		t.Errorf("Failure to get domain from etcd key (with a trailing '/'), expect: 'service.staging.cluster.local.', actually get: '%s'", result1)
	}

	result2 := msgUnPath("/skydns/local/cluster/staging/service")
	if result2 != "service.staging.cluster.local.skydns" {
		t.Errorf("Failure to get domain from etcd key (without trailing '/'), expect: 'service.staging.cluster.local.' actually get: '%s'", result2)
	}

	result3 := msgUnPath("/singleleveldomain/")
	if result3 != "singleleveldomain" {
		t.Errorf("Failure to get domain from etcd key (with leading and trailing '/'), expect: 'singleleveldomain.' actually get: '%s'", result3)
	}

	result4 := msgUnPath("/singleleveldomain")
	if result4 != "singleleveldomain" {
		t.Errorf("Failure to get domain from etcd key (without trailing '/'), expect: 'singleleveldomain.' actually get: '%s'", result4)
	}

	result5 := msgUnPath("singleleveldomain")
	if result5 != "singleleveldomain" {
		t.Errorf("Failure to get domain from etcd key (without leading and trailing '/'), expect: 'singleleveldomain.' actually get: '%s'", result5)
	}
}
