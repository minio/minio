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
	"syscall"
	"unsafe"
)

// Stat returns total and free bytes available in a directory, e.g. `C:\`.
// It returns free space available to the user (including quota limitations)
//
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa364937(v=vs.85).aspx
func Stat(path string) (total, free int64, err error) {
	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		return 0, 0, err
	}
	defer syscall.FreeLibrary(kernel32)
	// https://msdn.microsoft.com/en-us/library/windows/desktop/aa364937(v=vs.85).aspx
	// Retrieves information about the amount of space that is available on a disk volume,
	// which is the total amount of space, the total amount of free space, and the total
	// amount of free space available to the user that is associated with the calling thread.
	GetDiskFreeSpaceEx, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceExW")
	if err != nil {
		return 0, 0, err
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
	r1, _, e1 := syscall.Syscall6(uintptr(GetDiskFreeSpaceEx), 4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)), 0, 0)
	if e1 != 0 {
		return 0, 0, error(e1)
	}
	if r1 == 0 {
		return 0, 0, syscall.EINVAL
	}
	total = int64(lpTotalNumberOfBytes)
	free = int64(lpFreeBytesAvailable)
	return total, free, nil
}
