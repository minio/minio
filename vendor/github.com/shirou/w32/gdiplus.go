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

const (
	Ok                        = 0
	GenericError              = 1
	InvalidParameter          = 2
	OutOfMemory               = 3
	ObjectBusy                = 4
	InsufficientBuffer        = 5
	NotImplemented            = 6
	Win32Error                = 7
	WrongState                = 8
	Aborted                   = 9
	FileNotFound              = 10
	ValueOverflow             = 11
	AccessDenied              = 12
	UnknownImageFormat        = 13
	FontFamilyNotFound        = 14
	FontStyleNotFound         = 15
	NotTrueTypeFont           = 16
	UnsupportedGdiplusVersion = 17
	GdiplusNotInitialized     = 18
	PropertyNotFound          = 19
	PropertyNotSupported      = 20
	ProfileNotFound           = 21
)

func GetGpStatus(s int32) string {
	switch s {
	case Ok:
		return "Ok"
	case GenericError:
		return "GenericError"
	case InvalidParameter:
		return "InvalidParameter"
	case OutOfMemory:
		return "OutOfMemory"
	case ObjectBusy:
		return "ObjectBusy"
	case InsufficientBuffer:
		return "InsufficientBuffer"
	case NotImplemented:
		return "NotImplemented"
	case Win32Error:
		return "Win32Error"
	case WrongState:
		return "WrongState"
	case Aborted:
		return "Aborted"
	case FileNotFound:
		return "FileNotFound"
	case ValueOverflow:
		return "ValueOverflow"
	case AccessDenied:
		return "AccessDenied"
	case UnknownImageFormat:
		return "UnknownImageFormat"
	case FontFamilyNotFound:
		return "FontFamilyNotFound"
	case FontStyleNotFound:
		return "FontStyleNotFound"
	case NotTrueTypeFont:
		return "NotTrueTypeFont"
	case UnsupportedGdiplusVersion:
		return "UnsupportedGdiplusVersion"
	case GdiplusNotInitialized:
		return "GdiplusNotInitialized"
	case PropertyNotFound:
		return "PropertyNotFound"
	case PropertyNotSupported:
		return "PropertyNotSupported"
	case ProfileNotFound:
		return "ProfileNotFound"
	}
	return "Unknown Status Value"
}

var (
	token uintptr

	modgdiplus = syscall.NewLazyDLL("gdiplus.dll")

	procGdipCreateBitmapFromFile     = modgdiplus.NewProc("GdipCreateBitmapFromFile")
	procGdipCreateBitmapFromHBITMAP  = modgdiplus.NewProc("GdipCreateBitmapFromHBITMAP")
	procGdipCreateHBITMAPFromBitmap  = modgdiplus.NewProc("GdipCreateHBITMAPFromBitmap")
	procGdipCreateBitmapFromResource = modgdiplus.NewProc("GdipCreateBitmapFromResource")
	procGdipCreateBitmapFromStream   = modgdiplus.NewProc("GdipCreateBitmapFromStream")
	procGdipDisposeImage             = modgdiplus.NewProc("GdipDisposeImage")
	procGdiplusShutdown              = modgdiplus.NewProc("GdiplusShutdown")
	procGdiplusStartup               = modgdiplus.NewProc("GdiplusStartup")
)

func GdipCreateBitmapFromFile(filename string) (*uintptr, error) {
	var bitmap *uintptr
	ret, _, _ := procGdipCreateBitmapFromFile.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(filename))),
		uintptr(unsafe.Pointer(&bitmap)))

	if ret != Ok {
		return nil, errors.New(fmt.Sprintf("GdipCreateBitmapFromFile failed with status '%s' for file '%s'", GetGpStatus(int32(ret)), filename))
	}

	return bitmap, nil
}

func GdipCreateBitmapFromResource(instance HINSTANCE, resId *uint16) (*uintptr, error) {
	var bitmap *uintptr
	ret, _, _ := procGdipCreateBitmapFromResource.Call(
		uintptr(instance),
		uintptr(unsafe.Pointer(resId)),
		uintptr(unsafe.Pointer(&bitmap)))

	if ret != Ok {
		return nil, errors.New(fmt.Sprintf("GdiCreateBitmapFromResource failed with status '%s'", GetGpStatus(int32(ret))))
	}

	return bitmap, nil
}

func GdipCreateBitmapFromStream(stream *IStream) (*uintptr, error) {
	var bitmap *uintptr
	ret, _, _ := procGdipCreateBitmapFromStream.Call(
		uintptr(unsafe.Pointer(stream)),
		uintptr(unsafe.Pointer(&bitmap)))

	if ret != Ok {
		return nil, errors.New(fmt.Sprintf("GdipCreateBitmapFromStream failed with status '%s'", GetGpStatus(int32(ret))))
	}

	return bitmap, nil
}

func GdipCreateHBITMAPFromBitmap(bitmap *uintptr, background uint32) (HBITMAP, error) {
	var hbitmap HBITMAP
	ret, _, _ := procGdipCreateHBITMAPFromBitmap.Call(
		uintptr(unsafe.Pointer(bitmap)),
		uintptr(unsafe.Pointer(&hbitmap)),
		uintptr(background))

	if ret != Ok {
		return 0, errors.New(fmt.Sprintf("GdipCreateHBITMAPFromBitmap failed with status '%s'", GetGpStatus(int32(ret))))
	}

	return hbitmap, nil
}

func GdipDisposeImage(image *uintptr) {
	procGdipDisposeImage.Call(uintptr(unsafe.Pointer(image)))
}

func GdiplusShutdown() {
	procGdiplusShutdown.Call(token)
}

func GdiplusStartup(input *GdiplusStartupInput, output *GdiplusStartupOutput) {
	ret, _, _ := procGdiplusStartup.Call(
		uintptr(unsafe.Pointer(&token)),
		uintptr(unsafe.Pointer(input)),
		uintptr(unsafe.Pointer(output)))

	if ret != Ok {
		panic("GdiplusStartup failed with status " + GetGpStatus(int32(ret)))
	}
}
