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

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

var versionRegex = regexp.MustCompile(`^(\d+)\.(\d+).(\d+).*$`)

// VersionFromRelease converts a release string with format
// 4.4.2[-1] to a kernel version number in LINUX_VERSION_CODE format.
// That is, for kernel "a.b.c", the version number will be (a<<16 + b<<8 + c)
func VersionFromRelease(releaseString string) (uint32, error) {
	versionParts := versionRegex.FindStringSubmatch(releaseString)
	if len(versionParts) != 4 {
		return 0, fmt.Errorf("got invalid release version %q (expected format '4.3.2-1')", releaseString)
	}
	major, err := strconv.Atoi(versionParts[1])
	if err != nil {
		return 0, err
	}

	minor, err := strconv.Atoi(versionParts[2])
	if err != nil {
		return 0, err
	}

	patch, err := strconv.Atoi(versionParts[3])
	if err != nil {
		return 0, err
	}
	return Version(major, minor, patch), nil
}

// Version implements KERNEL_VERSION equivalent macro
// #define KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + ((c) > 255 ? 255 : (c)))
func Version(major, minor, patch int) uint32 {
	if patch > 255 {
		patch = 255
	}
	out := major<<16 + minor<<8 + patch
	return uint32(out)
}

func currentVersionUname() (uint32, error) {
	var buf syscall.Utsname
	if err := syscall.Uname(&buf); err != nil {
		return 0, err
	}
	releaseString := strings.Trim(utsnameStr(buf.Release[:]), "\x00")
	return VersionFromRelease(releaseString)
}

func currentVersionUbuntu() (uint32, error) {
	procVersion, err := ioutil.ReadFile("/proc/version_signature")
	if err != nil {
		return 0, err
	}
	var u1, u2, releaseString string
	_, err = fmt.Sscanf(string(procVersion), "%s %s %s", &u1, &u2, &releaseString)
	if err != nil {
		return 0, err
	}
	return VersionFromRelease(releaseString)
}

var debianVersionRegex = regexp.MustCompile(`.* SMP Debian (\d+\.\d+.\d+-\d+)(?:\+[[:alnum:]]*)?.*`)

func parseDebianVersion(str string) (uint32, error) {
	match := debianVersionRegex.FindStringSubmatch(str)
	if len(match) != 2 {
		return 0, fmt.Errorf("failed to parse kernel version from /proc/version: %s", str)
	}
	return VersionFromRelease(match[1])
}

func currentVersionDebian() (uint32, error) {
	procVersion, err := ioutil.ReadFile("/proc/version")
	if err != nil {
		return 0, fmt.Errorf("error reading /proc/version: %s", err)
	}

	return parseDebianVersion(string(procVersion))
}

// CurrentVersion returns the current kernel version in
// LINUX_VERSION_CODE format (see VersionFromRelease())
func CurrentVersion() (uint32, error) {
	// We need extra checks for Debian and Ubuntu as they modify
	// the kernel version patch number for compatibility with
	// out-of-tree modules. Linux perf tools do the same for Ubuntu
	// systems: https://github.com/torvalds/linux/commit/d18acd15c
	//
	// See also:
	// https://kernel-team.pages.debian.net/kernel-handbook/ch-versions.html
	// https://wiki.ubuntu.com/Kernel/FAQ
	version, err := currentVersionUbuntu()
	if err == nil {
		return version, nil
	}
	version, err = currentVersionDebian()
	if err == nil {
		return version, nil
	}
	return currentVersionUname()
}
