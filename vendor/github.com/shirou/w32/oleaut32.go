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
	modoleaut32 = syscall.NewLazyDLL("oleaut32")

	procVariantInit        = modoleaut32.NewProc("VariantInit")
	procSysAllocString     = modoleaut32.NewProc("SysAllocString")
	procSysFreeString      = modoleaut32.NewProc("SysFreeString")
	procSysStringLen       = modoleaut32.NewProc("SysStringLen")
	procCreateDispTypeInfo = modoleaut32.NewProc("CreateDispTypeInfo")
	procCreateStdDispatch  = modoleaut32.NewProc("CreateStdDispatch")
)

func VariantInit(v *VARIANT) {
	hr, _, _ := procVariantInit.Call(uintptr(unsafe.Pointer(v)))
	if hr != 0 {
		panic("Invoke VariantInit error.")
	}
	return
}

func SysAllocString(v string) (ss *int16) {
	pss, _, _ := procSysAllocString.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(v))))
	ss = (*int16)(unsafe.Pointer(pss))
	return
}

func SysFreeString(v *int16) {
	hr, _, _ := procSysFreeString.Call(uintptr(unsafe.Pointer(v)))
	if hr != 0 {
		panic("Invoke SysFreeString error.")
	}
	return
}

func SysStringLen(v *int16) uint {
	l, _, _ := procSysStringLen.Call(uintptr(unsafe.Pointer(v)))
	return uint(l)
}
