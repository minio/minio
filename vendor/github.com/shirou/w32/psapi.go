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
	modpsapi = syscall.NewLazyDLL("psapi.dll")

	procEnumProcesses = modpsapi.NewProc("EnumProcesses")
)

func EnumProcesses(processIds []uint32, cb uint32, bytesReturned *uint32) bool {
	ret, _, _ := procEnumProcesses.Call(
		uintptr(unsafe.Pointer(&processIds[0])),
		uintptr(cb),
		uintptr(unsafe.Pointer(bytesReturned)))

	return ret != 0
}
