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

//go:build linux
// +build linux

package kernel

import "testing"

var testData = []struct {
	success       bool
	releaseString string
	kernelVersion uint32
}{
	{true, "4.1.2-3", 262402},
	{true, "4.8.14-200.fc24.x86_64", 264206},
	{true, "4.1.2-3foo", 262402},
	{true, "4.1.2foo-1", 262402},
	{true, "4.1.2-rkt-v1", 262402},
	{true, "4.1.2rkt-v1", 262402},
	{true, "4.1.2-3 foo", 262402},
	{true, "3.10.0-1062.el7.x86_64", 199168},
	{true, "3.0.0", 196608},
	{true, "2.6.32", 132640},
	{true, "5.13.0-30-generic", 331008},
	{true, "5.10.0-1052-oem", 330240},
	{false, "foo 4.1.2-3", 0},
	{true, "4.1.2", 262402},
	{false, ".4.1.2", 0},
	{false, "4.1.", 0},
	{false, "4.1", 0},
}

func TestVersionFromRelease(t *testing.T) {
	for _, test := range testData {
		version, err := VersionFromRelease(test.releaseString)
		if err != nil && test.success {
			t.Errorf("expected %q to success: %s", test.releaseString, err)
		} else if err == nil && !test.success {
			t.Errorf("expected %q to fail", test.releaseString)
		}
		if version != test.kernelVersion {
			t.Errorf("expected kernel version %d, got %d", test.kernelVersion, version)
		}
	}
}

func TestParseDebianVersion(t *testing.T) {
	for _, tc := range []struct {
		success       bool
		releaseString string
		kernelVersion uint32
	}{
		// 4.9.168
		{true, "Linux version 4.9.0-9-amd64 (debian-kernel@lists.debian.org) (gcc version 6.3.0 20170516 (Debian 6.3.0-18+deb9u1) ) #1 SMP Debian 4.9.168-1+deb9u3 (2019-06-16)", 264616},
		// 4.9.88
		{true, "Linux ip-10-0-75-49 4.9.0-6-amd64 #1 SMP Debian 4.9.88-1+deb9u1 (2018-05-07) x86_64 GNU/Linux", 264536},
		// 3.0.4
		{true, "Linux version 3.16.0-9-amd64 (debian-kernel@lists.debian.org) (gcc version 4.9.2 (Debian 4.9.2-10+deb8u2) ) #1 SMP Debian 3.16.68-1 (2019-05-22)", 200772},
		// Invalid
		{false, "Linux version 4.9.125-linuxkit (root@659b6d51c354) (gcc version 6.4.0 (Alpine 6.4.0) ) #1 SMP Fri Sep 7 08:20:28 UTC 2018", 0},
	} {
		version, err := parseDebianVersion(tc.releaseString)
		if err != nil && tc.success {
			t.Errorf("expected %q to success: %s", tc.releaseString, err)
		} else if err == nil && !tc.success {
			t.Errorf("expected %q to fail", tc.releaseString)
		}
		if version != tc.kernelVersion {
			t.Errorf("expected kernel version %d, got %d", tc.kernelVersion, version)
		}
	}
}
