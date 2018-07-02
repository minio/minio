// +build windows

package mem

import (
	"context"
	"unsafe"

	"github.com/shirou/gopsutil/internal/common"
	"golang.org/x/sys/windows"
)

var (
	procGlobalMemoryStatusEx = common.Modkernel32.NewProc("GlobalMemoryStatusEx")
	procGetPerformanceInfo   = common.ModPsapi.NewProc("GetPerformanceInfo")
)

type memoryStatusEx struct {
	cbSize                  uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64 // in bytes
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

func VirtualMemory() (*VirtualMemoryStat, error) {
	return VirtualMemoryWithContext(context.Background())
}

func VirtualMemoryWithContext(ctx context.Context) (*VirtualMemoryStat, error) {
	var memInfo memoryStatusEx
	memInfo.cbSize = uint32(unsafe.Sizeof(memInfo))
	mem, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memInfo)))
	if mem == 0 {
		return nil, windows.GetLastError()
	}

	ret := &VirtualMemoryStat{
		Total:       memInfo.ullTotalPhys,
		Available:   memInfo.ullAvailPhys,
		UsedPercent: float64(memInfo.dwMemoryLoad),
	}

	ret.Used = ret.Total - ret.Available
	return ret, nil
}

type performanceInformation struct {
	cb                uint32
	commitTotal       uint64
	commitLimit       uint64
	commitPeak        uint64
	physicalTotal     uint64
	physicalAvailable uint64
	systemCache       uint64
	kernelTotal       uint64
	kernelPaged       uint64
	kernelNonpaged    uint64
	pageSize          uint64
	handleCount       uint32
	processCount      uint32
	threadCount       uint32
}

func SwapMemory() (*SwapMemoryStat, error) {
	return SwapMemoryWithContext(context.Background())
}

func SwapMemoryWithContext(ctx context.Context) (*SwapMemoryStat, error) {
	var perfInfo performanceInformation
	perfInfo.cb = uint32(unsafe.Sizeof(perfInfo))
	mem, _, _ := procGetPerformanceInfo.Call(uintptr(unsafe.Pointer(&perfInfo)), uintptr(perfInfo.cb))
	if mem == 0 {
		return nil, windows.GetLastError()
	}
	tot := perfInfo.commitLimit * perfInfo.pageSize
	used := perfInfo.commitTotal * perfInfo.pageSize
	free := tot - used
	ret := &SwapMemoryStat{
		Total:       tot,
		Used:        used,
		Free:        free,
		UsedPercent: float64(used) / float64(tot),
	}

	return ret, nil
}
