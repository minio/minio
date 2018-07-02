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
	modcomctl32 = syscall.NewLazyDLL("comctl32.dll")

	procInitCommonControlsEx    = modcomctl32.NewProc("InitCommonControlsEx")
	procImageList_Create        = modcomctl32.NewProc("ImageList_Create")
	procImageList_Destroy       = modcomctl32.NewProc("ImageList_Destroy")
	procImageList_GetImageCount = modcomctl32.NewProc("ImageList_GetImageCount")
	procImageList_SetImageCount = modcomctl32.NewProc("ImageList_SetImageCount")
	procImageList_Add           = modcomctl32.NewProc("ImageList_Add")
	procImageList_ReplaceIcon   = modcomctl32.NewProc("ImageList_ReplaceIcon")
	procImageList_Remove        = modcomctl32.NewProc("ImageList_Remove")
	procTrackMouseEvent         = modcomctl32.NewProc("_TrackMouseEvent")
)

func InitCommonControlsEx(lpInitCtrls *INITCOMMONCONTROLSEX) bool {
	ret, _, _ := procInitCommonControlsEx.Call(
		uintptr(unsafe.Pointer(lpInitCtrls)))

	return ret != 0
}

func ImageList_Create(cx, cy int, flags uint, cInitial, cGrow int) HIMAGELIST {
	ret, _, _ := procImageList_Create.Call(
		uintptr(cx),
		uintptr(cy),
		uintptr(flags),
		uintptr(cInitial),
		uintptr(cGrow))

	if ret == 0 {
		panic("Create image list failed")
	}

	return HIMAGELIST(ret)
}

func ImageList_Destroy(himl HIMAGELIST) bool {
	ret, _, _ := procImageList_Destroy.Call(
		uintptr(himl))

	return ret != 0
}

func ImageList_GetImageCount(himl HIMAGELIST) int {
	ret, _, _ := procImageList_GetImageCount.Call(
		uintptr(himl))

	return int(ret)
}

func ImageList_SetImageCount(himl HIMAGELIST, uNewCount uint) bool {
	ret, _, _ := procImageList_SetImageCount.Call(
		uintptr(himl),
		uintptr(uNewCount))

	return ret != 0
}

func ImageList_Add(himl HIMAGELIST, hbmImage, hbmMask HBITMAP) int {
	ret, _, _ := procImageList_Add.Call(
		uintptr(himl),
		uintptr(hbmImage),
		uintptr(hbmMask))

	return int(ret)
}

func ImageList_ReplaceIcon(himl HIMAGELIST, i int, hicon HICON) int {
	ret, _, _ := procImageList_ReplaceIcon.Call(
		uintptr(himl),
		uintptr(i),
		uintptr(hicon))

	return int(ret)
}

func ImageList_AddIcon(himl HIMAGELIST, hicon HICON) int {
	return ImageList_ReplaceIcon(himl, -1, hicon)
}

func ImageList_Remove(himl HIMAGELIST, i int) bool {
	ret, _, _ := procImageList_Remove.Call(
		uintptr(himl),
		uintptr(i))

	return ret != 0
}

func ImageList_RemoveAll(himl HIMAGELIST) bool {
	return ImageList_Remove(himl, -1)
}

func TrackMouseEvent(tme *TRACKMOUSEEVENT) bool {
	ret, _, _ := procTrackMouseEvent.Call(
		uintptr(unsafe.Pointer(tme)))

	return ret != 0
}
