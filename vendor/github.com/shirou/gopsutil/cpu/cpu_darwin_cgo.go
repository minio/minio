// +build darwin
// +build cgo

package cpu

/*
#include <stdlib.h>
#include <sys/sysctl.h>
#include <sys/mount.h>
#include <mach/mach_init.h>
#include <mach/mach_host.h>
#include <mach/host_info.h>
#if TARGET_OS_MAC
#include <libproc.h>
#endif
#include <mach/processor_info.h>
#include <mach/vm_map.h>
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

// these CPU times for darwin is borrowed from influxdb/telegraf.

func perCPUTimes() ([]TimesStat, error) {
	var (
		count   C.mach_msg_type_number_t
		cpuload *C.processor_cpu_load_info_data_t
		ncpu    C.natural_t
	)

	status := C.host_processor_info(C.host_t(C.mach_host_self()),
		C.PROCESSOR_CPU_LOAD_INFO,
		&ncpu,
		(*C.processor_info_array_t)(unsafe.Pointer(&cpuload)),
		&count)

	if status != C.KERN_SUCCESS {
		return nil, fmt.Errorf("host_processor_info error=%d", status)
	}

	// jump through some cgo casting hoops and ensure we properly free
	// the memory that cpuload points to
	target := C.vm_map_t(C.mach_task_self_)
	address := C.vm_address_t(uintptr(unsafe.Pointer(cpuload)))
	defer C.vm_deallocate(target, address, C.vm_size_t(ncpu))

	// the body of struct processor_cpu_load_info
	// aka processor_cpu_load_info_data_t
	var cpu_ticks [C.CPU_STATE_MAX]uint32

	// copy the cpuload array to a []byte buffer
	// where we can binary.Read the data
	size := int(ncpu) * binary.Size(cpu_ticks)
	buf := (*[1 << 30]byte)(unsafe.Pointer(cpuload))[:size:size]

	bbuf := bytes.NewBuffer(buf)

	var ret []TimesStat

	for i := 0; i < int(ncpu); i++ {
		err := binary.Read(bbuf, binary.LittleEndian, &cpu_ticks)
		if err != nil {
			return nil, err
		}

		c := TimesStat{
			CPU:    fmt.Sprintf("cpu%d", i),
			User:   float64(cpu_ticks[C.CPU_STATE_USER]) / ClocksPerSec,
			System: float64(cpu_ticks[C.CPU_STATE_SYSTEM]) / ClocksPerSec,
			Nice:   float64(cpu_ticks[C.CPU_STATE_NICE]) / ClocksPerSec,
			Idle:   float64(cpu_ticks[C.CPU_STATE_IDLE]) / ClocksPerSec,
		}

		ret = append(ret, c)
	}

	return ret, nil
}

func allCPUTimes() ([]TimesStat, error) {
	var count C.mach_msg_type_number_t
	var cpuload C.host_cpu_load_info_data_t

	count = C.HOST_CPU_LOAD_INFO_COUNT

	status := C.host_statistics(C.host_t(C.mach_host_self()),
		C.HOST_CPU_LOAD_INFO,
		C.host_info_t(unsafe.Pointer(&cpuload)),
		&count)

	if status != C.KERN_SUCCESS {
		return nil, fmt.Errorf("host_statistics error=%d", status)
	}

	c := TimesStat{
		CPU:    "cpu-total",
		User:   float64(cpuload.cpu_ticks[C.CPU_STATE_USER]) / ClocksPerSec,
		System: float64(cpuload.cpu_ticks[C.CPU_STATE_SYSTEM]) / ClocksPerSec,
		Nice:   float64(cpuload.cpu_ticks[C.CPU_STATE_NICE]) / ClocksPerSec,
		Idle:   float64(cpuload.cpu_ticks[C.CPU_STATE_IDLE]) / ClocksPerSec,
	}

	return []TimesStat{c}, nil

}
