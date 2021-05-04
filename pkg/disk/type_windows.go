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

package disk

import (
	"path/filepath"
	"syscall"
	"unsafe"
)

var (
	// GetVolumeInformation provides windows drive volume information.
	GetVolumeInformation = kernel32.NewProc("GetVolumeInformationW")
)

// getFSType returns the filesystem type of the underlying mounted filesystem
func getFSType(path string) string {
	volumeNameSize, nFileSystemNameSize := uint32(260), uint32(260)
	var lpVolumeSerialNumber uint32
	var lpFileSystemFlags, lpMaximumComponentLength uint32
	var lpFileSystemNameBuffer, volumeName [260]uint16
	var ps = syscall.StringToUTF16Ptr(filepath.VolumeName(path))

	// Extract values safely
	// BOOL WINAPI GetVolumeInformation(
	// _In_opt_  LPCTSTR lpRootPathName,
	// _Out_opt_ LPTSTR  lpVolumeNameBuffer,
	// _In_      DWORD   nVolumeNameSize,
	// _Out_opt_ LPDWORD lpVolumeSerialNumber,
	// _Out_opt_ LPDWORD lpMaximumComponentLength,
	// _Out_opt_ LPDWORD lpFileSystemFlags,
	// _Out_opt_ LPTSTR  lpFileSystemNameBuffer,
	// _In_      DWORD   nFileSystemNameSize
	// );

	_, _, _ = GetVolumeInformation.Call(uintptr(unsafe.Pointer(ps)),
		uintptr(unsafe.Pointer(&volumeName)),
		uintptr(volumeNameSize),
		uintptr(unsafe.Pointer(&lpVolumeSerialNumber)),
		uintptr(unsafe.Pointer(&lpMaximumComponentLength)),
		uintptr(unsafe.Pointer(&lpFileSystemFlags)),
		uintptr(unsafe.Pointer(&lpFileSystemNameBuffer)),
		uintptr(nFileSystemNameSize))

	return syscall.UTF16ToString(lpFileSystemNameBuffer[:])
}
