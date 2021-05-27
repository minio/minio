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

// Package cgroup implements parsing for all the cgroup
// categories and functionality in a simple way.
package cgroup

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// DO NOT EDIT following constants are chosen defaults for any kernel
// after 3.x, please open a GitHub issue https://github.com/minio/minio/issues
// and discuss first if you wish to change this.
const (
	// Default string for looking for kernel memory param.
	memoryLimitKernelParam = "memory.limit_in_bytes"

	// Points to sys path memory path.
	cgroupMemSysPath = "/sys/fs/cgroup/memory"

	// Default docker prefix.
	dockerPrefixName = "/docker/"

	// Proc controller group path.
	cgroupFileTemplate = "/proc/%d/cgroup"
)

// CGEntries - represents all the entries in a process cgroup file
// at /proc/<pid>/cgroup as key value pairs.
type CGEntries map[string]string

// GetEntries reads and parses all the cgroup entries for a given process.
func GetEntries(pid int) (CGEntries, error) {
	r, err := os.Open(fmt.Sprintf(cgroupFileTemplate, pid))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return parseProcCGroup(r)
}

// parseProcCGroup - cgroups are always in the following
// format once enabled you need to know the pid of the
// application you are looking for so that the the
// following parsing logic only parses the file located
// at /proc/<pid>/cgroup.
//
// CGROUP entries id, component and path are always in
// the following format. ``ID:COMPONENT:PATH``
//
// Following code block parses this information and
// returns a procCGroup which is a parsed list of all
// the line by line entires from /proc/<pid>/cgroup.
func parseProcCGroup(r io.Reader) (CGEntries, error) {
	var cgEntries = CGEntries{}

	// Start reading cgroup categories line by line
	// and process them into procCGroup structure.
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		tokens := strings.SplitN(line, ":", 3)
		if len(tokens) < 3 {
			continue
		}

		name, path := tokens[1], tokens[2]
		for _, token := range strings.Split(name, ",") {
			name = strings.TrimPrefix(token, "name=")
			cgEntries[name] = path
		}
	}

	// Return upon any error while reading the cgroup categories.
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return cgEntries, nil
}

// Fetch value of the cgroup kernel param from the cgroup manager,
// if cgroup manager is configured we should just rely on `cgm` cli
// to fetch all the values for us.
func getManagerKernValue(cname, path, kernParam string) (limit uint64, err error) {

	cmd := exec.Command("cgm", "getvalue", cname, path, kernParam)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err = cmd.Run(); err != nil {
		return 0, err
	}

	// Parse the cgm output.
	limit, err = strconv.ParseUint(strings.TrimSpace(out.String()), 10, 64)
	return limit, err
}

// Get cgroup memory limit file path.
func getMemoryLimitFilePath(cgPath string) string {
	path := cgroupMemSysPath

	// Docker generates weird cgroup paths that don't
	// really exist on the file system.
	//
	// For example on regular Linux OS :
	// `/user.slice/user-1000.slice/session-1.scope`
	//
	// But they exist as a bind mount on Docker and
	// are not accessible :  `/docker/<hash>`
	//
	// We we will just ignore if there is `/docker` in the
	// path ignore and fall back to :
	// `/sys/fs/cgroup/memory/memory.limit_in_bytes`
	if !strings.HasPrefix(cgPath, dockerPrefixName) {
		path = filepath.Join(path, cgPath)
	}

	// Final path.
	return filepath.Join(path, memoryLimitKernelParam)
}

// GetMemoryLimit - Fetches cgroup memory limit either from
// a file path at '/sys/fs/cgroup/memory', if path fails then
// fallback to querying cgroup manager.
func GetMemoryLimit(pid int) (limit uint64, err error) {
	var cg CGEntries
	cg, err = GetEntries(pid)
	if err != nil {
		return 0, err
	}

	path := cg["memory"]

	limit, err = getManagerKernValue("memory", path, memoryLimitKernelParam)
	if err != nil {

		// Upon any failure returned from `cgm`, on some systems cgm
		// might not be installed. We fallback to using the the sysfs
		// path instead to lookup memory limits.
		var b []byte
		b, err = ioutil.ReadFile(getMemoryLimitFilePath(path))
		if err != nil {
			return 0, err
		}

		limit, err = strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	}

	return limit, err
}
