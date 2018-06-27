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
	modshell32 = syscall.NewLazyDLL("shell32.dll")

	procSHBrowseForFolder   = modshell32.NewProc("SHBrowseForFolderW")
	procSHGetPathFromIDList = modshell32.NewProc("SHGetPathFromIDListW")
	procDragAcceptFiles     = modshell32.NewProc("DragAcceptFiles")
	procDragQueryFile       = modshell32.NewProc("DragQueryFileW")
	procDragQueryPoint      = modshell32.NewProc("DragQueryPoint")
	procDragFinish          = modshell32.NewProc("DragFinish")
	procShellExecute        = modshell32.NewProc("ShellExecuteW")
	procExtractIcon         = modshell32.NewProc("ExtractIconW")
)

func SHBrowseForFolder(bi *BROWSEINFO) uintptr {
	ret, _, _ := procSHBrowseForFolder.Call(uintptr(unsafe.Pointer(bi)))

	return ret
}

func SHGetPathFromIDList(idl uintptr) string {
	buf := make([]uint16, 1024)
	procSHGetPathFromIDList.Call(
		idl,
		uintptr(unsafe.Pointer(&buf[0])))

	return syscall.UTF16ToString(buf)
}

func DragAcceptFiles(hwnd HWND, accept bool) {
	procDragAcceptFiles.Call(
		uintptr(hwnd),
		uintptr(BoolToBOOL(accept)))
}

func DragQueryFile(hDrop HDROP, iFile uint) (fileName string, fileCount uint) {
	ret, _, _ := procDragQueryFile.Call(
		uintptr(hDrop),
		uintptr(iFile),
		0,
		0)

	fileCount = uint(ret)

	if iFile != 0xFFFFFFFF {
		buf := make([]uint16, fileCount+1)

		ret, _, _ := procDragQueryFile.Call(
			uintptr(hDrop),
			uintptr(iFile),
			uintptr(unsafe.Pointer(&buf[0])),
			uintptr(fileCount+1))

		if ret == 0 {
			panic("Invoke DragQueryFile error.")
		}

		fileName = syscall.UTF16ToString(buf)
	}

	return
}

func DragQueryPoint(hDrop HDROP) (x, y int, isClientArea bool) {
	var pt POINT
	ret, _, _ := procDragQueryPoint.Call(
		uintptr(hDrop),
		uintptr(unsafe.Pointer(&pt)))

	return int(pt.X), int(pt.Y), (ret == 1)
}

func DragFinish(hDrop HDROP) {
	procDragFinish.Call(uintptr(hDrop))
}

func ShellExecute(hwnd HWND, lpOperation, lpFile, lpParameters, lpDirectory string, nShowCmd int) error {
	var op, param, directory uintptr
	if len(lpOperation) != 0 {
		op = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpOperation)))
	}
	if len(lpParameters) != 0 {
		param = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpParameters)))
	}
	if len(lpDirectory) != 0 {
		directory = uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpDirectory)))
	}

	ret, _, _ := procShellExecute.Call(
		uintptr(hwnd),
		op,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpFile))),
		param,
		directory,
		uintptr(nShowCmd))

	errorMsg := ""
	if ret != 0 && ret <= 32 {
		switch int(ret) {
		case ERROR_FILE_NOT_FOUND:
			errorMsg = "The specified file was not found."
		case ERROR_PATH_NOT_FOUND:
			errorMsg = "The specified path was not found."
		case ERROR_BAD_FORMAT:
			errorMsg = "The .exe file is invalid (non-Win32 .exe or error in .exe image)."
		case SE_ERR_ACCESSDENIED:
			errorMsg = "The operating system denied access to the specified file."
		case SE_ERR_ASSOCINCOMPLETE:
			errorMsg = "The file name association is incomplete or invalid."
		case SE_ERR_DDEBUSY:
			errorMsg = "The DDE transaction could not be completed because other DDE transactions were being processed."
		case SE_ERR_DDEFAIL:
			errorMsg = "The DDE transaction failed."
		case SE_ERR_DDETIMEOUT:
			errorMsg = "The DDE transaction could not be completed because the request timed out."
		case SE_ERR_DLLNOTFOUND:
			errorMsg = "The specified DLL was not found."
		case SE_ERR_NOASSOC:
			errorMsg = "There is no application associated with the given file name extension. This error will also be returned if you attempt to print a file that is not printable."
		case SE_ERR_OOM:
			errorMsg = "There was not enough memory to complete the operation."
		case SE_ERR_SHARE:
			errorMsg = "A sharing violation occurred."
		default:
			errorMsg = fmt.Sprintf("Unknown error occurred with error code %v", ret)
		}
	} else {
		return nil
	}

	return errors.New(errorMsg)
}

func ExtractIcon(lpszExeFileName string, nIconIndex int) HICON {
	ret, _, _ := procExtractIcon.Call(
		0,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(lpszExeFileName))),
		uintptr(nIconIndex))

	return HICON(ret)
}
