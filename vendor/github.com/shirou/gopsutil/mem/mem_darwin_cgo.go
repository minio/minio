// +build darwin
// +build cgo

package mem

/*
#include <mach/mach_host.h>
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// VirtualMemory returns VirtualmemoryStat.
func VirtualMemory() (*VirtualMemoryStat, error) {
	return VirtualMemoryWithContext(context.Background())
}

func VirtualMemoryWithContext(ctx context.Context) (*VirtualMemoryStat, error) {
	count := C.mach_msg_type_number_t(C.HOST_VM_INFO_COUNT)
	var vmstat C.vm_statistics_data_t

	status := C.host_statistics(C.host_t(C.mach_host_self()),
		C.HOST_VM_INFO,
		C.host_info_t(unsafe.Pointer(&vmstat)),
		&count)

	if status != C.KERN_SUCCESS {
		return nil, fmt.Errorf("host_statistics error=%d", status)
	}

	pageSize := uint64(unix.Getpagesize())
	total, err := getHwMemsize()
	if err != nil {
		return nil, err
	}
	totalCount := C.natural_t(total / pageSize)

	availableCount := vmstat.inactive_count + vmstat.free_count
	usedPercent := 100 * float64(totalCount-availableCount) / float64(totalCount)

	usedCount := totalCount - availableCount

	return &VirtualMemoryStat{
		Total:       total,
		Available:   pageSize * uint64(availableCount),
		Used:        pageSize * uint64(usedCount),
		UsedPercent: usedPercent,
		Free:        pageSize * uint64(vmstat.free_count),
		Active:      pageSize * uint64(vmstat.active_count),
		Inactive:    pageSize * uint64(vmstat.inactive_count),
		Wired:       pageSize * uint64(vmstat.wire_count),
	}, nil
}
