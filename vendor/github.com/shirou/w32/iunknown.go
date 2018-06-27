// Copyright 2010-2012 The W32 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package w32

type pIUnknownVtbl struct {
	pQueryInterface uintptr
	pAddRef         uintptr
	pRelease        uintptr
}

type IUnknown struct {
	lpVtbl *pIUnknownVtbl
}

func (this *IUnknown) QueryInterface(id *GUID) *IDispatch {
	return ComQueryInterface(this, id)
}

func (this *IUnknown) AddRef() int32 {
	return ComAddRef(this)
}

func (this *IUnknown) Release() int32 {
	return ComRelease(this)
}
