// Copyright 2010-2012 The W32 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package w32

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

var (
	modadvapi32 = syscall.NewLazyDLL("advapi32.dll")

	procRegCreateKeyEx = modadvapi32.NewProc("RegCreateKeyExW")
	procRegOpenKeyEx   = modadvapi32.NewProc("RegOpenKeyExW")
	procRegCloseKey    = modadvapi32.NewProc("RegCloseKey")
	procRegGetValue    = modadvapi32.NewProc("RegGetValueW")
	procRegEnumKeyEx   = modadvapi32.NewProc("RegEnumKeyExW")
	//	procRegSetKeyValue     = modadvapi32.NewProc("RegSetKeyValueW")
	procRegSetValueEx      = modadvapi32.NewProc("RegSetValueExW")
	procOpenEventLog       = modadvapi32.NewProc("OpenEventLogW")
	procReadEventLog       = modadvapi32.NewProc("ReadEventLogW")
	procCloseEventLog      = modadvapi32.NewProc("CloseEventLog")
	procOpenSCManager      = modadvapi32.NewProc("OpenSCManagerW")
	procCloseServiceHandle = modadvapi32.NewProc("CloseServiceHandle")
	procOpenService        = modadvapi32.NewProc("OpenServiceW")
	procStartService       = modadvapi32.NewProc("StartServiceW")
	procControlService     = modadvapi32.NewProc("ControlService")
)

func RegCreateKey(hKey HKEY, subKey string) HKEY {
	var result HKEY
	ret, _, _ := procRegCreateKeyEx.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(0),
		uintptr(0),
		uintptr(0),
		uintptr(KEY_ALL_ACCESS),
		uintptr(0),
		uintptr(unsafe.Pointer(&result)),
		uintptr(0))
	_ = ret
	return result
}

func RegOpenKeyEx(hKey HKEY, subKey string, samDesired uint32) HKEY {
	var result HKEY
	ret, _, _ := procRegOpenKeyEx.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(0),
		uintptr(samDesired),
		uintptr(unsafe.Pointer(&result)))

	if ret != ERROR_SUCCESS {
		panic(fmt.Sprintf("RegOpenKeyEx(%d, %s, %d) failed", hKey, subKey, samDesired))
	}
	return result
}

func RegCloseKey(hKey HKEY) error {
	var err error
	ret, _, _ := procRegCloseKey.Call(
		uintptr(hKey))

	if ret != ERROR_SUCCESS {
		err = errors.New("RegCloseKey failed")
	}
	return err
}

func RegGetRaw(hKey HKEY, subKey string, value string) []byte {
	var bufLen uint32
	var valptr unsafe.Pointer
	if len(value) > 0 {
		valptr = unsafe.Pointer(syscall.StringToUTF16Ptr(value))
	}
	procRegGetValue.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(valptr),
		uintptr(RRF_RT_ANY),
		0,
		0,
		uintptr(unsafe.Pointer(&bufLen)))

	if bufLen == 0 {
		return nil
	}

	buf := make([]byte, bufLen)
	ret, _, _ := procRegGetValue.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(valptr),
		uintptr(RRF_RT_ANY),
		0,
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(&bufLen)))

	if ret != ERROR_SUCCESS {
		return nil
	}

	return buf
}

func RegSetBinary(hKey HKEY, subKey string, value []byte) (errno int) {
	var lptr, vptr unsafe.Pointer
	if len(subKey) > 0 {
		lptr = unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))
	}
	if len(value) > 0 {
		vptr = unsafe.Pointer(&value[0])
	}
	ret, _, _ := procRegSetValueEx.Call(
		uintptr(hKey),
		uintptr(lptr),
		uintptr(0),
		uintptr(REG_BINARY),
		uintptr(vptr),
		uintptr(len(value)))

	return int(ret)
}

func RegGetString(hKey HKEY, subKey string, value string) string {
	var bufLen uint32
	procRegGetValue.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(value))),
		uintptr(RRF_RT_REG_SZ),
		0,
		0,
		uintptr(unsafe.Pointer(&bufLen)))

	if bufLen == 0 {
		return ""
	}

	buf := make([]uint16, bufLen)
	ret, _, _ := procRegGetValue.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(value))),
		uintptr(RRF_RT_REG_SZ),
		0,
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(&bufLen)))

	if ret != ERROR_SUCCESS {
		return ""
	}

	return syscall.UTF16ToString(buf)
}

/*
func RegSetKeyValue(hKey HKEY, subKey string, valueName string, dwType uint32, data uintptr, cbData uint16) (errno int) {
	ret, _, _ := procRegSetKeyValue.Call(
		uintptr(hKey),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(subKey))),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(valueName))),
		uintptr(dwType),
		data,
		uintptr(cbData))

	return int(ret)
}
*/

func RegEnumKeyEx(hKey HKEY, index uint32) string {
	var bufLen uint32 = 255
	buf := make([]uint16, bufLen)
	procRegEnumKeyEx.Call(
		uintptr(hKey),
		uintptr(index),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(&bufLen)),
		0,
		0,
		0,
		0)
	return syscall.UTF16ToString(buf)
}

func OpenEventLog(servername string, sourcename string) HANDLE {
	ret, _, _ := procOpenEventLog.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(servername))),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(sourcename))))

	return HANDLE(ret)
}

func ReadEventLog(eventlog HANDLE, readflags, recordoffset uint32, buffer []byte, numberofbytestoread uint32, bytesread, minnumberofbytesneeded *uint32) bool {
	ret, _, _ := procReadEventLog.Call(
		uintptr(eventlog),
		uintptr(readflags),
		uintptr(recordoffset),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(numberofbytestoread),
		uintptr(unsafe.Pointer(bytesread)),
		uintptr(unsafe.Pointer(minnumberofbytesneeded)))

	return ret != 0
}

func CloseEventLog(eventlog HANDLE) bool {
	ret, _, _ := procCloseEventLog.Call(
		uintptr(eventlog))

	return ret != 0
}

func OpenSCManager(lpMachineName, lpDatabaseName string, dwDesiredAccess uint32) (HANDLE, error) {
	var p1, p2 uintptr
	if len(lpMachineName) > 0 {
		p1 = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpMachineName)))
	}
	if len(lpDatabaseName) > 0 {
		p2 = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpDatabaseName)))
	}
	ret, _, _ := procOpenSCManager.Call(
		p1,
		p2,
		uintptr(dwDesiredAccess))

	if ret == 0 {
		return 0, syscall.GetLastError()
	}

	return HANDLE(ret), nil
}

func CloseServiceHandle(hSCObject HANDLE) error {
	ret, _, _ := procCloseServiceHandle.Call(uintptr(hSCObject))
	if ret == 0 {
		return syscall.GetLastError()
	}
	return nil
}

func OpenService(hSCManager HANDLE, lpServiceName string, dwDesiredAccess uint32) (HANDLE, error) {
	ret, _, _ := procOpenService.Call(
		uintptr(hSCManager),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpServiceName))),
		uintptr(dwDesiredAccess))

	if ret == 0 {
		return 0, syscall.GetLastError()
	}

	return HANDLE(ret), nil
}

func StartService(hService HANDLE, lpServiceArgVectors []string) error {
	l := len(lpServiceArgVectors)
	var ret uintptr
	if l == 0 {
		ret, _, _ = procStartService.Call(
			uintptr(hService),
			0,
			0)
	} else {
		lpArgs := make([]uintptr, l)
		for i := 0; i < l; i++ {
			lpArgs[i] = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpServiceArgVectors[i])))
		}

		ret, _, _ = procStartService.Call(
			uintptr(hService),
			uintptr(l),
			uintptr(unsafe.Pointer(&lpArgs[0])))
	}

	if ret == 0 {
		return syscall.GetLastError()
	}

	return nil
}

func ControlService(hService HANDLE, dwControl uint32, lpServiceStatus *SERVICE_STATUS) bool {
	if lpServiceStatus == nil {
		panic("ControlService:lpServiceStatus cannot be nil")
	}

	ret, _, _ := procControlService.Call(
		uintptr(hService),
		uintptr(dwControl),
		uintptr(unsafe.Pointer(lpServiceStatus)))

	return ret != 0
}
