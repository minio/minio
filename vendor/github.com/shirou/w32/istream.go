// Copyright 2010-2012 The W32 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package w32

import (
	"unsafe"
)

type pIStreamVtbl struct {
	pQueryInterface uintptr
	pAddRef         uintptr
	pRelease        uintptr
}

type IStream struct {
	lpVtbl *pIStreamVtbl
}

func (this *IStream) QueryInterface(id *GUID) *IDispatch {
	return ComQueryInterface((*IUnknown)(unsafe.Pointer(this)), id)
}

func (this *IStream) AddRef() int32 {
	return ComAddRef((*IUnknown)(unsafe.Pointer(this)))
}

func (this *IStream) Release() int32 {
	return ComRelease((*IUnknown)(unsafe.Pointer(this)))
}
