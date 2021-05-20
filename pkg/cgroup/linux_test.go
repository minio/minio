// +build linux

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

package cgroup

import (
	"io/ioutil"
	"os"
	"testing"
)

// Testing parsing correctness for various process cgroup files.
func TestProcCGroup(t *testing.T) {
	tmpPath, err := ioutil.TempFile("", "cgroup")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpPath.Name())

	cgroup := `
11:memory:/user.slice
10:blkio:/user.slice
9:hugetlb:/
8:net_cls,net_prio:/
7:perf_event:/
6:pids:/user.slice/user-1000.slice
5:devices:/user.slice
4:cpuset:/
3:cpu,cpuacct:/user.slice
2:freezer:/
1:name=systemd:/user.slice/user-1000.slice/session-1.scope
`
	_, err = tmpPath.WriteString(cgroup)
	if err != nil {
		t.Fatal(err)
	}

	// Seek back to read from the beginning.
	tmpPath.Seek(0, 0)

	cg, err := parseProcCGroup(tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	path := cg["memory"]
	if len(path) == 0 {
		t.Fatal("Path component cannot be empty")
	}

	if path != "/user.slice" {
		t.Fatal("Path component cannot be empty")
	}

	path = cg["systemd"]
	if path != "/user.slice/user-1000.slice/session-1.scope" {
		t.Fatal("Path component cannot be empty")
	}

	// Mixed cgroups with different group names.
	cgroup = `
11:memory:/newtest/newtest
10:blkio:/user.slice
9:hugetlb:/
8:net_cls,net_prio:/
7:perf_event:/
6:pids:/user.slice/user-1000.slice
5:devices:/user.slice
4:cpuset:/
3:cpu,cpuacct:/newtest/newtest
2:freezer:/
1:name=systemd:/user.slice/user-1000.slice/session-1.scope
`

	// Seek back to read from the beginning.
	tmpPath.Seek(0, 0)

	_, err = tmpPath.WriteString(cgroup)
	if err != nil {
		t.Fatal(err)
	}

	// Seek back to read from the beginning.
	tmpPath.Seek(0, 0)

	cg, err = parseProcCGroup(tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	path = cg["memory"]
	if path != "/newtest/newtest" {
		t.Fatal("Path component cannot be empty")
	}

	path = cg["systemd"]
	if path != "/user.slice/user-1000.slice/session-1.scope" {
		t.Fatal("Path component cannot be empty")
	}

}

// Tests cgroup memory limit path construction.
func TestMemoryLimitPath(t *testing.T) {
	testCases := []struct {
		cgroupPath   string
		expectedPath string
	}{
		{
			cgroupPath:   "/user.slice",
			expectedPath: "/sys/fs/cgroup/memory/user.slice/memory.limit_in_bytes",
		},
		{
			cgroupPath:   "/docker/testing",
			expectedPath: "/sys/fs/cgroup/memory/memory.limit_in_bytes",
		},
	}

	for i, testCase := range testCases {
		actualPath := getMemoryLimitFilePath(testCase.cgroupPath)
		if actualPath != testCase.expectedPath {
			t.Fatalf("Test: %d: Expected: %s, got %s", i+1, testCase.expectedPath, actualPath)
		}
	}
}
