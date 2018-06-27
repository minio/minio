// +build windows

package memory

import (
	"syscall"
	"unsafe"
)

// omitting a few fields for brevity...
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa366589(v=vs.85).aspx
type memStatusEx struct {
	dwLength     uint32
	dwMemoryLoad uint32
	ullTotalPhys uint64
	unused       [6]uint64
}

func sysTotalMemory() uint64 {
	kernel32, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		return 0
	}
	// GetPhysicallyInstalledSystemMemory is simpler, but broken on
	// older versions of windows (and uses this under the hood anyway).
	globalMemoryStatusEx, err := kernel32.FindProc("GlobalMemoryStatusEx")
	if err != nil {
		return 0
	}
	msx := &memStatusEx{
		dwLength: 64,
	}
	r, _, _ := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(msx)))
	if r == 0 {
		return 0
	}
	return msx.ullTotalPhys
}
