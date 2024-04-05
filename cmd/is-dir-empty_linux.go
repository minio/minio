//go:build linux && !appengine
// +build linux,!appengine

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
	"syscall"
)

// Returns true if no error and there is no object or prefix inside this directory
func isDirEmpty(dirname string, legacy bool) bool {
	if legacy {
		// On filesystems such as btrfs, nfs this is not true, so fallback
		// to performing readdir() instead.
		entries, err := readDirN(dirname, 1)
		if err != nil {
			return false
		}
		return len(entries) == 0
	}
	var stat syscall.Stat_t
	if err := syscall.Stat(dirname, &stat); err != nil {
		return false
	}
	return stat.Mode&syscall.S_IFMT == syscall.S_IFDIR && stat.Nlink == 2
}
