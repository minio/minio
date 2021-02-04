// +build windows

/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
		return v.(bool)
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
