//go:build windows
// +build windows

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

package mountinfo

import (
	"path/filepath"
	"sync"

	"golang.org/x/sys/windows"
)

// CheckCrossDevice - check if any input path has multiple sub-mounts.
// this is a dummy function and returns nil for now.
func CheckCrossDevice(paths []string) error {
	return nil
}

// mountPointCache contains results of IsLikelyMountPoint
var mountPointCache sync.Map

// IsLikelyMountPoint determines if a directory is a mountpoint.
func IsLikelyMountPoint(path string) bool {
	path = filepath.Dir(path)
	if v, ok := mountPointCache.Load(path); ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	wpath, _ := windows.UTF16PtrFromString(path)
	wvolume := make([]uint16, len(path)+1)

	if err := windows.GetVolumePathName(wpath, &wvolume[0], uint32(len(wvolume))); err != nil {
		mountPointCache.Store(path, false)
		return false
	}

	switch windows.GetDriveType(&wvolume[0]) {
	case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_REMOTE, windows.DRIVE_RAMDISK:
		// Recognize "fixed", "removable", "remote" and "ramdisk" drives as proper drives
		// which can be treated as an actual mount-point, rest can be ignored.
		// https://docs.microsoft.com/en-us/windows/desktop/api/fileapi/nf-fileapi-getdrivetypew
		mountPointCache.Store(path, true)
		return true
	}
	mountPointCache.Store(path, false)
	return false
}
