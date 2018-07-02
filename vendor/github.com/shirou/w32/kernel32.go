// Copyright 2010-2012 The W32 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package w32

import (
	"syscall"
	"unsafe"
)

var (
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")

	procGetModuleHandle            = modkernel32.NewProc("GetModuleHandleW")
	procMulDiv                     = modkernel32.NewProc("MulDiv")
	procGetConsoleWindow           = modkernel32.NewProc("GetConsoleWindow")
	procGetCurrentThread           = modkernel32.NewProc("GetCurrentThread")
	procGetLogicalDrives           = modkernel32.NewProc("GetLogicalDrives")
	procGetUserDefaultLCID         = modkernel32.NewProc("GetUserDefaultLCID")
	procLstrlen                    = modkernel32.NewProc("lstrlenW")
	procLstrcpy                    = modkernel32.NewProc("lstrcpyW")
	procGlobalAlloc                = modkernel32.NewProc("GlobalAlloc")
	procGlobalFree                 = modkernel32.NewProc("GlobalFree")
	procGlobalLock                 = modkernel32.NewProc("GlobalLock")
	procGlobalUnlock               = modkernel32.NewProc("GlobalUnlock")
	procMoveMemory                 = modkernel32.NewProc("RtlMoveMemory")
	procFindResource               = modkernel32.NewProc("FindResourceW")
	procSizeofResource             = modkernel32.NewProc("SizeofResource")
	procLockResource               = modkernel32.NewProc("LockResource")
	procLoadResource               = modkernel32.NewProc("LoadResource")
	procGetLastError               = modkernel32.NewProc("GetLastError")
	procOpenProcess                = modkernel32.NewProc("OpenProcess")
	procTerminateProcess           = modkernel32.NewProc("TerminateProcess")
	procCloseHandle                = modkernel32.NewProc("CloseHandle")
	procCreateToolhelp32Snapshot   = modkernel32.NewProc("CreateToolhelp32Snapshot")
	procModule32First              = modkernel32.NewProc("Module32FirstW")
	procModule32Next               = modkernel32.NewProc("Module32NextW")
	procProcess32First             = modkernel32.NewProc("Process32FirstW")
	procProcess32Next              = modkernel32.NewProc("Process32NextW")
	procGetSystemTimes             = modkernel32.NewProc("GetSystemTimes")
	procGetConsoleScreenBufferInfo = modkernel32.NewProc("GetConsoleScreenBufferInfo")
	procSetConsoleTextAttribute    = modkernel32.NewProc("SetConsoleTextAttribute")
	procGetDiskFreeSpaceEx         = modkernel32.NewProc("GetDiskFreeSpaceExW")
	procGetProcessTimes            = modkernel32.NewProc("GetProcessTimes")
)

func GetModuleHandle(modulename string) HINSTANCE {
	var mn uintptr
	if modulename == "" {
		mn = 0
	} else {
		mn = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(modulename)))
	}
	ret, _, _ := procGetModuleHandle.Call(mn)
	return HINSTANCE(ret)
}

func MulDiv(number, numerator, denominator int) int {
	ret, _, _ := procMulDiv.Call(
		uintptr(number),
		uintptr(numerator),
		uintptr(denominator))

	return int(ret)
}

func GetConsoleWindow() HWND {
	ret, _, _ := procGetConsoleWindow.Call()

	return HWND(ret)
}

func GetCurrentThread() HANDLE {
	ret, _, _ := procGetCurrentThread.Call()

	return HANDLE(ret)
}

func GetLogicalDrives() uint32 {
	ret, _, _ := procGetLogicalDrives.Call()

	return uint32(ret)
}

func GetUserDefaultLCID() uint32 {
	ret, _, _ := procGetUserDefaultLCID.Call()

	return uint32(ret)
}

func Lstrlen(lpString *uint16) int {
	ret, _, _ := procLstrlen.Call(uintptr(unsafe.Pointer(lpString)))

	return int(ret)
}

func Lstrcpy(buf []uint16, lpString *uint16) {
	procLstrcpy.Call(
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(lpString)))
}

func GlobalAlloc(uFlags uint, dwBytes uint32) HGLOBAL {
	ret, _, _ := procGlobalAlloc.Call(
		uintptr(uFlags),
		uintptr(dwBytes))

	if ret == 0 {
		panic("GlobalAlloc failed")
	}

	return HGLOBAL(ret)
}

func GlobalFree(hMem HGLOBAL) {
	ret, _, _ := procGlobalFree.Call(uintptr(hMem))

	if ret != 0 {
		panic("GlobalFree failed")
	}
}

func GlobalLock(hMem HGLOBAL) unsafe.Pointer {
	ret, _, _ := procGlobalLock.Call(uintptr(hMem))

	if ret == 0 {
		panic("GlobalLock failed")
	}

	return unsafe.Pointer(ret)
}

func GlobalUnlock(hMem HGLOBAL) bool {
	ret, _, _ := procGlobalUnlock.Call(uintptr(hMem))

	return ret != 0
}

func MoveMemory(destination, source unsafe.Pointer, length uint32) {
	procMoveMemory.Call(
		uintptr(unsafe.Pointer(destination)),
		uintptr(source),
		uintptr(length))
}

func FindResource(hModule HMODULE, lpName, lpType *uint16) (HRSRC, error) {
	ret, _, _ := procFindResource.Call(
		uintptr(hModule),
		uintptr(unsafe.Pointer(lpName)),
		uintptr(unsafe.Pointer(lpType)))

	if ret == 0 {
		return 0, syscall.GetLastError()
	}

	return HRSRC(ret), nil
}

func SizeofResource(hModule HMODULE, hResInfo HRSRC) uint32 {
	ret, _, _ := procSizeofResource.Call(
		uintptr(hModule),
		uintptr(hResInfo))

	if ret == 0 {
		panic("SizeofResource failed")
	}

	return uint32(ret)
}

func LockResource(hResData HGLOBAL) unsafe.Pointer {
	ret, _, _ := procLockResource.Call(uintptr(hResData))

	if ret == 0 {
		panic("LockResource failed")
	}

	return unsafe.Pointer(ret)
}

func LoadResource(hModule HMODULE, hResInfo HRSRC) HGLOBAL {
	ret, _, _ := procLoadResource.Call(
		uintptr(hModule),
		uintptr(hResInfo))

	if ret == 0 {
		panic("LoadResource failed")
	}

	return HGLOBAL(ret)
}

func GetLastError() uint32 {
	ret, _, _ := procGetLastError.Call()
	return uint32(ret)
}

func OpenProcess(desiredAccess uint32, inheritHandle bool, processId uint32) HANDLE {
	inherit := 0
	if inheritHandle {
		inherit = 1
	}

	ret, _, _ := procOpenProcess.Call(
		uintptr(desiredAccess),
		uintptr(inherit),
		uintptr(processId))
	return HANDLE(ret)
}

func TerminateProcess(hProcess HANDLE, uExitCode uint) bool {
	ret, _, _ := procTerminateProcess.Call(
		uintptr(hProcess),
		uintptr(uExitCode))
	return ret != 0
}

func CloseHandle(object HANDLE) bool {
	ret, _, _ := procCloseHandle.Call(
		uintptr(object))
	return ret != 0
}

func CreateToolhelp32Snapshot(flags, processId uint32) HANDLE {
	ret, _, _ := procCreateToolhelp32Snapshot.Call(
		uintptr(flags),
		uintptr(processId))

	if ret <= 0 {
		return HANDLE(0)
	}

	return HANDLE(ret)
}

func Module32First(snapshot HANDLE, me *MODULEENTRY32) bool {
	ret, _, _ := procModule32First.Call(
		uintptr(snapshot),
		uintptr(unsafe.Pointer(me)))

	return ret != 0
}

func Module32Next(snapshot HANDLE, me *MODULEENTRY32) bool {
	ret, _, _ := procModule32Next.Call(
		uintptr(snapshot),
		uintptr(unsafe.Pointer(me)))

	return ret != 0
}
func Process32First(snapshot HANDLE, pe *PROCESSENTRY32) bool {
	ret, _, _ := procProcess32First.Call(
		uintptr(snapshot),
		uintptr(unsafe.Pointer(pe)))

	return ret != 0
}

func Process32Next(snapshot HANDLE, pe *PROCESSENTRY32) bool {
	ret, _, _ := procProcess32Next.Call(
		uintptr(snapshot),
		uintptr(unsafe.Pointer(pe)))

	return ret != 0
}
func GetSystemTimes(lpIdleTime, lpKernelTime, lpUserTime *FILETIME) bool {
	ret, _, _ := procGetSystemTimes.Call(
		uintptr(unsafe.Pointer(lpIdleTime)),
		uintptr(unsafe.Pointer(lpKernelTime)),
		uintptr(unsafe.Pointer(lpUserTime)))

	return ret != 0
}

func GetProcessTimes(hProcess HANDLE, lpCreationTime, lpExitTime, lpKernelTime, lpUserTime *FILETIME) bool {
	ret, _, _ := procGetProcessTimes.Call(
		uintptr(hProcess),
		uintptr(unsafe.Pointer(lpCreationTime)),
		uintptr(unsafe.Pointer(lpExitTime)),
		uintptr(unsafe.Pointer(lpKernelTime)),
		uintptr(unsafe.Pointer(lpUserTime)))

	return ret != 0
}

func GetConsoleScreenBufferInfo(hConsoleOutput HANDLE) *CONSOLE_SCREEN_BUFFER_INFO {
	var csbi CONSOLE_SCREEN_BUFFER_INFO
	ret, _, _ := procGetConsoleScreenBufferInfo.Call(
		uintptr(hConsoleOutput),
		uintptr(unsafe.Pointer(&csbi)))
	if ret == 0 {
		return nil
	}
	return &csbi
}

func SetConsoleTextAttribute(hConsoleOutput HANDLE, wAttributes uint16) bool {
	ret, _, _ := procSetConsoleTextAttribute.Call(
		uintptr(hConsoleOutput),
		uintptr(wAttributes))
	return ret != 0
}

func GetDiskFreeSpaceEx(dirName string) (r bool,
	freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes uint64) {
	ret, _, _ := procGetDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(dirName))),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)))
	return ret != 0,
		freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes
}
