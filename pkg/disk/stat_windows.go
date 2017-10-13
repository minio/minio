// +build windows

/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package disk

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")

	// GetDiskFreeSpaceEx - https://msdn.microsoft.com/en-us/library/windows/desktop/aa364937(v=vs.85).aspx
	// Retrieves information about the amount of space that is available on a disk volume,
	// which is the total amount of space, the total amount of free space, and the total
	// amount of free space available to the user that is associated with the calling thread.
	GetDiskFreeSpaceEx = kernel32.NewProc("GetDiskFreeSpaceExW")
	// GetDiskFreeSpace - https://msdn.microsoft.com/en-us/library/windows/desktop/aa364935(v=vs.85).aspx
	// Retrieves information about the specified disk, including the amount of free space on the disk.
	GetDiskFreeSpace = kernel32.NewProc("GetDiskFreeSpaceW")
)

// GetInfo returns total and free bytes available in a directory, e.g. `C:\`.
// It returns free space available to the user (including quota limitations)
//
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa364937(v=vs.85).aspx
func GetInfo(path string) (info Info, err error) {
	// Stat to know if the path exists.
	if _, err = os.Stat(path); err != nil {
		return Info{}, err
	}

	lpFreeBytesAvailable := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)

	// Extract values safely
	// BOOL WINAPI GetDiskFreeSpaceEx(
	// _In_opt_  LPCTSTR         lpDirectoryName,
	// _Out_opt_ PULARGE_INTEGER lpFreeBytesAvailable,
	// _Out_opt_ PULARGE_INTEGER lpTotalNumberOfBytes,
	// _Out_opt_ PULARGE_INTEGER lpTotalNumberOfFreeBytes
	// );
	_, _, _ = GetDiskFreeSpaceEx.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))
	info = Info{}
	info.Total = uint64(lpTotalNumberOfBytes)
	info.Free = uint64(lpFreeBytesAvailable)
	info.FSType = getFSType(path)

	// Return values of GetDiskFreeSpace()
	lpSectorsPerCluster := uint32(0)
	lpBytesPerSector := uint32(0)
	lpNumberOfFreeClusters := uint32(0)
	lpTotalNumberOfClusters := uint32(0)

	// Extract values safely
	// BOOL WINAPI GetDiskFreeSpace(
	//   _In_  LPCTSTR lpRootPathName,
	//   _Out_ LPDWORD lpSectorsPerCluster,
	//   _Out_ LPDWORD lpBytesPerSector,
	//   _Out_ LPDWORD lpNumberOfFreeClusters,
	//   _Out_ LPDWORD lpTotalNumberOfClusters
	// );
	_, _, _ = GetDiskFreeSpace.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpSectorsPerCluster)),
		uintptr(unsafe.Pointer(&lpBytesPerSector)),
		uintptr(unsafe.Pointer(&lpNumberOfFreeClusters)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfClusters)))

	info.Files = uint64(lpTotalNumberOfClusters)
	info.Ffree = uint64(lpNumberOfFreeClusters)

	return info, nil
}
