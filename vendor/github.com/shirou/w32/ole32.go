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
	modole32 = syscall.NewLazyDLL("ole32.dll")

	procCoInitializeEx        = modole32.NewProc("CoInitializeEx")
	procCoInitialize          = modole32.NewProc("CoInitialize")
	procCoUninitialize        = modole32.NewProc("CoUninitialize")
	procCreateStreamOnHGlobal = modole32.NewProc("CreateStreamOnHGlobal")
)

func CoInitializeEx(coInit uintptr) HRESULT {
	ret, _, _ := procCoInitializeEx.Call(
		0,
		coInit)

	switch uint32(ret) {
	case E_INVALIDARG:
		panic("CoInitializeEx failed with E_INVALIDARG")
	case E_OUTOFMEMORY:
		panic("CoInitializeEx failed with E_OUTOFMEMORY")
	case E_UNEXPECTED:
		panic("CoInitializeEx failed with E_UNEXPECTED")
	}

	return HRESULT(ret)
}

func CoInitialize() {
	procCoInitialize.Call(0)
}

func CoUninitialize() {
	procCoUninitialize.Call()
}

func CreateStreamOnHGlobal(hGlobal HGLOBAL, fDeleteOnRelease bool) *IStream {
	stream := new(IStream)
	ret, _, _ := procCreateStreamOnHGlobal.Call(
		uintptr(hGlobal),
		uintptr(BoolToBOOL(fDeleteOnRelease)),
		uintptr(unsafe.Pointer(&stream)))

	switch uint32(ret) {
	case E_INVALIDARG:
		panic("CreateStreamOnHGlobal failed with E_INVALIDARG")
	case E_OUTOFMEMORY:
		panic("CreateStreamOnHGlobal failed with E_OUTOFMEMORY")
	case E_UNEXPECTED:
		panic("CreateStreamOnHGlobal failed with E_UNEXPECTED")
	}

	return stream
}
