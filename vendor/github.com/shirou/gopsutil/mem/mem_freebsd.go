// +build freebsd

package mem

import (
	"context"
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

func VirtualMemory() (*VirtualMemoryStat, error) {
	return VirtualMemoryWithContext(context.Background())
}

func VirtualMemoryWithContext(ctx context.Context) (*VirtualMemoryStat, error) {
	pageSize, err := unix.SysctlUint32("vm.stats.vm.v_page_size")
	if err != nil {
		return nil, err
	}
	pageCount, err := unix.SysctlUint32("vm.stats.vm.v_page_count")
	if err != nil {
		return nil, err
	}
	free, err := unix.SysctlUint32("vm.stats.vm.v_free_count")
	if err != nil {
		return nil, err
	}
	active, err := unix.SysctlUint32("vm.stats.vm.v_active_count")
	if err != nil {
		return nil, err
	}
	inactive, err := unix.SysctlUint32("vm.stats.vm.v_inactive_count")
	if err != nil {
		return nil, err
	}
	cached, err := unix.SysctlUint32("vm.stats.vm.v_cache_count")
	if err != nil {
		return nil, err
	}
	buffers, err := unix.SysctlUint64("vfs.bufspace")
	if err != nil {
		return nil, err
	}
	wired, err := unix.SysctlUint32("vm.stats.vm.v_wire_count")
	if err != nil {
		return nil, err
	}

	p := uint64(pageSize)
	ret := &VirtualMemoryStat{
		Total:    uint64(pageCount) * p,
		Free:     uint64(free) * p,
		Active:   uint64(active) * p,
		Inactive: uint64(inactive) * p,
		Cached:   uint64(cached) * p,
		Buffers:  uint64(buffers),
		Wired:    uint64(wired) * p,
	}

	ret.Available = ret.Inactive + ret.Cached + ret.Free
	ret.Used = ret.Total - ret.Available
	ret.UsedPercent = float64(ret.Used) / float64(ret.Total) * 100.0

	return ret, nil
}

// Return swapinfo
func SwapMemory() (*SwapMemoryStat, error) {
	return SwapMemoryWithContext(context.Background())
}

// Constants from vm/vm_param.h
// nolint: golint
const (
	XSWDEV_VERSION = 1
)

// Types from vm/vm_param.h
type xswdev struct {
	Version uint32 // Version is the version
	Dev     uint32 // Dev is the device identifier
	Flags   int32  // Flags is the swap flags applied to the device
	NBlks   int32  // NBlks is the total number of blocks
	Used    int32  // Used is the number of blocks used
}

func SwapMemoryWithContext(ctx context.Context) (*SwapMemoryStat, error) {
	// FreeBSD can have multiple swap devices so we total them up
	i, err := unix.SysctlUint32("vm.nswapdev")
	if err != nil {
		return nil, err
	}

	if i == 0 {
		return nil, errors.New("no swap devices found")
	}

	c := int(i)

	i, err = unix.SysctlUint32("vm.stats.vm.v_page_size")
	if err != nil {
		return nil, err
	}
	pageSize := uint64(i)

	var buf []byte
	s := &SwapMemoryStat{}
	for n := 0; n < c; n++ {
		buf, err = unix.SysctlRaw("vm.swap_info", n)
		if err != nil {
			return nil, err
		}

		xsw := (*xswdev)(unsafe.Pointer(&buf[0]))
		if xsw.Version != XSWDEV_VERSION {
			return nil, errors.New("xswdev version mismatch")
		}
		s.Total += uint64(xsw.NBlks)
		s.Used += uint64(xsw.Used)
	}

	if s.Total != 0 {
		s.UsedPercent = float64(s.Used) / float64(s.Total) * 100
	}
	s.Total *= pageSize
	s.Used *= pageSize
	s.Free = s.Total - s.Used

	return s, nil
}
