// Copyright 2010-2012 The W32 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package w32

import (
	"fmt"
	"syscall"
	"unsafe"
)

// DEFINED IN THE DWM API BUT NOT IMPLEMENTED BY MS:
// DwmAttachMilContent
// DwmDetachMilContent
// DwmEnableComposition
// DwmGetGraphicsStreamClient
// DwmGetGraphicsStreamTransformHint

var (
	moddwmapi = syscall.NewLazyDLL("dwmapi.dll")

	procDwmDefWindowProc                 = moddwmapi.NewProc("DwmDefWindowProc")
	procDwmEnableBlurBehindWindow        = moddwmapi.NewProc("DwmEnableBlurBehindWindow")
	procDwmEnableMMCSS                   = moddwmapi.NewProc("DwmEnableMMCSS")
	procDwmExtendFrameIntoClientArea     = moddwmapi.NewProc("DwmExtendFrameIntoClientArea")
	procDwmFlush                         = moddwmapi.NewProc("DwmFlush")
	procDwmGetColorizationColor          = moddwmapi.NewProc("DwmGetColorizationColor")
	procDwmGetCompositionTimingInfo      = moddwmapi.NewProc("DwmGetCompositionTimingInfo")
	procDwmGetTransportAttributes        = moddwmapi.NewProc("DwmGetTransportAttributes")
	procDwmGetWindowAttribute            = moddwmapi.NewProc("DwmGetWindowAttribute")
	procDwmInvalidateIconicBitmaps       = moddwmapi.NewProc("DwmInvalidateIconicBitmaps")
	procDwmIsCompositionEnabled          = moddwmapi.NewProc("DwmIsCompositionEnabled")
	procDwmModifyPreviousDxFrameDuration = moddwmapi.NewProc("DwmModifyPreviousDxFrameDuration")
	procDwmQueryThumbnailSourceSize      = moddwmapi.NewProc("DwmQueryThumbnailSourceSize")
	procDwmRegisterThumbnail             = moddwmapi.NewProc("DwmRegisterThumbnail")
	procDwmRenderGesture                 = moddwmapi.NewProc("DwmRenderGesture")
	procDwmSetDxFrameDuration            = moddwmapi.NewProc("DwmSetDxFrameDuration")
	procDwmSetIconicLivePreviewBitmap    = moddwmapi.NewProc("DwmSetIconicLivePreviewBitmap")
	procDwmSetIconicThumbnail            = moddwmapi.NewProc("DwmSetIconicThumbnail")
	procDwmSetPresentParameters          = moddwmapi.NewProc("DwmSetPresentParameters")
	procDwmSetWindowAttribute            = moddwmapi.NewProc("DwmSetWindowAttribute")
	procDwmShowContact                   = moddwmapi.NewProc("DwmShowContact")
	procDwmTetherContact                 = moddwmapi.NewProc("DwmTetherContact")
	procDwmTransitionOwnedWindow         = moddwmapi.NewProc("DwmTransitionOwnedWindow")
	procDwmUnregisterThumbnail           = moddwmapi.NewProc("DwmUnregisterThumbnail")
	procDwmUpdateThumbnailProperties     = moddwmapi.NewProc("DwmUpdateThumbnailProperties")
)

func DwmDefWindowProc(hWnd HWND, msg uint, wParam, lParam uintptr) (bool, uint) {
	var result uint
	ret, _, _ := procDwmDefWindowProc.Call(
		uintptr(hWnd),
		uintptr(msg),
		wParam,
		lParam,
		uintptr(unsafe.Pointer(&result)))
	return ret != 0, result
}

func DwmEnableBlurBehindWindow(hWnd HWND, pBlurBehind *DWM_BLURBEHIND) HRESULT {
	ret, _, _ := procDwmEnableBlurBehindWindow.Call(
		uintptr(hWnd),
		uintptr(unsafe.Pointer(pBlurBehind)))
	return HRESULT(ret)
}

func DwmEnableMMCSS(fEnableMMCSS bool) HRESULT {
	ret, _, _ := procDwmEnableMMCSS.Call(
		uintptr(BoolToBOOL(fEnableMMCSS)))
	return HRESULT(ret)
}

func DwmExtendFrameIntoClientArea(hWnd HWND, pMarInset *MARGINS) HRESULT {
	ret, _, _ := procDwmExtendFrameIntoClientArea.Call(
		uintptr(hWnd),
		uintptr(unsafe.Pointer(pMarInset)))
	return HRESULT(ret)
}

func DwmFlush() HRESULT {
	ret, _, _ := procDwmFlush.Call()
	return HRESULT(ret)
}

func DwmGetColorizationColor(pcrColorization *uint32, pfOpaqueBlend *BOOL) HRESULT {
	ret, _, _ := procDwmGetColorizationColor.Call(
		uintptr(unsafe.Pointer(pcrColorization)),
		uintptr(unsafe.Pointer(pfOpaqueBlend)))
	return HRESULT(ret)
}

func DwmGetCompositionTimingInfo(hWnd HWND, pTimingInfo *DWM_TIMING_INFO) HRESULT {
	ret, _, _ := procDwmGetCompositionTimingInfo.Call(
		uintptr(hWnd),
		uintptr(unsafe.Pointer(pTimingInfo)))
	return HRESULT(ret)
}

func DwmGetTransportAttributes(pfIsRemoting *BOOL, pfIsConnected *BOOL, pDwGeneration *uint32) HRESULT {
	ret, _, _ := procDwmGetTransportAttributes.Call(
		uintptr(unsafe.Pointer(pfIsRemoting)),
		uintptr(unsafe.Pointer(pfIsConnected)),
		uintptr(unsafe.Pointer(pDwGeneration)))
	return HRESULT(ret)
}

// TODO: verify handling of variable arguments
func DwmGetWindowAttribute(hWnd HWND, dwAttribute uint32) (pAttribute interface{}, result HRESULT) {
	var pvAttribute, pvAttrSize uintptr
	switch dwAttribute {
	case DWMWA_NCRENDERING_ENABLED:
		v := new(BOOL)
		pAttribute = v
		pvAttribute = uintptr(unsafe.Pointer(v))
		pvAttrSize = unsafe.Sizeof(*v)
	case DWMWA_CAPTION_BUTTON_BOUNDS, DWMWA_EXTENDED_FRAME_BOUNDS:
		v := new(RECT)
		pAttribute = v
		pvAttribute = uintptr(unsafe.Pointer(v))
		pvAttrSize = unsafe.Sizeof(*v)
	case DWMWA_CLOAKED:
		panic(fmt.Sprintf("DwmGetWindowAttribute(%d) is not currently supported.", dwAttribute))
	default:
		panic(fmt.Sprintf("DwmGetWindowAttribute(%d) is not valid.", dwAttribute))
	}

	ret, _, _ := procDwmGetWindowAttribute.Call(
		uintptr(hWnd),
		uintptr(dwAttribute),
		pvAttribute,
		pvAttrSize)
	result = HRESULT(ret)
	return
}

func DwmInvalidateIconicBitmaps(hWnd HWND) HRESULT {
	ret, _, _ := procDwmInvalidateIconicBitmaps.Call(
		uintptr(hWnd))
	return HRESULT(ret)
}

func DwmIsCompositionEnabled(pfEnabled *BOOL) HRESULT {
	ret, _, _ := procDwmIsCompositionEnabled.Call(
		uintptr(unsafe.Pointer(pfEnabled)))
	return HRESULT(ret)
}

func DwmModifyPreviousDxFrameDuration(hWnd HWND, cRefreshes int, fRelative bool) HRESULT {
	ret, _, _ := procDwmModifyPreviousDxFrameDuration.Call(
		uintptr(hWnd),
		uintptr(cRefreshes),
		uintptr(BoolToBOOL(fRelative)))
	return HRESULT(ret)
}

func DwmQueryThumbnailSourceSize(hThumbnail HTHUMBNAIL, pSize *SIZE) HRESULT {
	ret, _, _ := procDwmQueryThumbnailSourceSize.Call(
		uintptr(hThumbnail),
		uintptr(unsafe.Pointer(pSize)))
	return HRESULT(ret)
}

func DwmRegisterThumbnail(hWndDestination HWND, hWndSource HWND, phThumbnailId *HTHUMBNAIL) HRESULT {
	ret, _, _ := procDwmRegisterThumbnail.Call(
		uintptr(hWndDestination),
		uintptr(hWndSource),
		uintptr(unsafe.Pointer(phThumbnailId)))
	return HRESULT(ret)
}

func DwmRenderGesture(gt GESTURE_TYPE, cContacts uint, pdwPointerID *uint32, pPoints *POINT) {
	procDwmRenderGesture.Call(
		uintptr(gt),
		uintptr(cContacts),
		uintptr(unsafe.Pointer(pdwPointerID)),
		uintptr(unsafe.Pointer(pPoints)))
	return
}

func DwmSetDxFrameDuration(hWnd HWND, cRefreshes int) HRESULT {
	ret, _, _ := procDwmSetDxFrameDuration.Call(
		uintptr(hWnd),
		uintptr(cRefreshes))
	return HRESULT(ret)
}

func DwmSetIconicLivePreviewBitmap(hWnd HWND, hbmp HBITMAP, pptClient *POINT, dwSITFlags uint32) HRESULT {
	ret, _, _ := procDwmSetIconicLivePreviewBitmap.Call(
		uintptr(hWnd),
		uintptr(hbmp),
		uintptr(unsafe.Pointer(pptClient)),
		uintptr(dwSITFlags))
	return HRESULT(ret)
}

func DwmSetIconicThumbnail(hWnd HWND, hbmp HBITMAP, dwSITFlags uint32) HRESULT {
	ret, _, _ := procDwmSetIconicThumbnail.Call(
		uintptr(hWnd),
		uintptr(hbmp),
		uintptr(dwSITFlags))
	return HRESULT(ret)
}

func DwmSetPresentParameters(hWnd HWND, pPresentParams *DWM_PRESENT_PARAMETERS) HRESULT {
	ret, _, _ := procDwmSetPresentParameters.Call(
		uintptr(hWnd),
		uintptr(unsafe.Pointer(pPresentParams)))
	return HRESULT(ret)
}

func DwmSetWindowAttribute(hWnd HWND, dwAttribute uint32, pvAttribute LPCVOID, cbAttribute uint32) HRESULT {
	ret, _, _ := procDwmSetWindowAttribute.Call(
		uintptr(hWnd),
		uintptr(dwAttribute),
		uintptr(pvAttribute),
		uintptr(cbAttribute))
	return HRESULT(ret)
}

func DwmShowContact(dwPointerID uint32, eShowContact DWM_SHOWCONTACT) {
	procDwmShowContact.Call(
		uintptr(dwPointerID),
		uintptr(eShowContact))
	return
}

func DwmTetherContact(dwPointerID uint32, fEnable bool, ptTether POINT) {
	procDwmTetherContact.Call(
		uintptr(dwPointerID),
		uintptr(BoolToBOOL(fEnable)),
		uintptr(unsafe.Pointer(&ptTether)))
	return
}

func DwmTransitionOwnedWindow(hWnd HWND, target DWMTRANSITION_OWNEDWINDOW_TARGET) {
	procDwmTransitionOwnedWindow.Call(
		uintptr(hWnd),
		uintptr(target))
	return
}

func DwmUnregisterThumbnail(hThumbnailId HTHUMBNAIL) HRESULT {
	ret, _, _ := procDwmUnregisterThumbnail.Call(
		uintptr(hThumbnailId))
	return HRESULT(ret)
}

func DwmUpdateThumbnailProperties(hThumbnailId HTHUMBNAIL, ptnProperties *DWM_THUMBNAIL_PROPERTIES) HRESULT {
	ret, _, _ := procDwmUpdateThumbnailProperties.Call(
		uintptr(hThumbnailId),
		uintptr(unsafe.Pointer(ptnProperties)))
	return HRESULT(ret)
}
