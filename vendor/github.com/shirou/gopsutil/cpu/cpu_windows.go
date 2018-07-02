// +build windows

package cpu

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/StackExchange/wmi"
	"github.com/shirou/gopsutil/internal/common"
	"golang.org/x/sys/windows"
)

type Win32_Processor struct {
	LoadPercentage            *uint16
	Family                    uint16
	Manufacturer              string
	Name                      string
	NumberOfLogicalProcessors uint32
	ProcessorID               *string
	Stepping                  *string
	MaxClockSpeed             uint32
}

// Win32_PerfFormattedData_Counters_ProcessorInformation stores instance value of the perf counters
type Win32_PerfFormattedData_Counters_ProcessorInformation struct {
	Name                  string
	PercentDPCTime        uint64
	PercentIdleTime       uint64
	PercentUserTime       uint64
	PercentProcessorTime  uint64
	PercentInterruptTime  uint64
	PercentPriorityTime   uint64
	PercentPrivilegedTime uint64
	InterruptsPerSec      uint32
	ProcessorFrequency    uint32
	DPCRate               uint32
}

// Win32_PerfFormattedData_PerfOS_System struct to have count of processes and processor queue length
type Win32_PerfFormattedData_PerfOS_System struct {
	Processes            uint32
	ProcessorQueueLength uint32
}

// Times returns times stat per cpu and combined for all CPUs
func Times(percpu bool) ([]TimesStat, error) {
	return TimesWithContext(context.Background(), percpu)
}

func TimesWithContext(ctx context.Context, percpu bool) ([]TimesStat, error) {
	if percpu {
		return perCPUTimes()
	}

	var ret []TimesStat
	var lpIdleTime common.FILETIME
	var lpKernelTime common.FILETIME
	var lpUserTime common.FILETIME
	r, _, _ := common.ProcGetSystemTimes.Call(
		uintptr(unsafe.Pointer(&lpIdleTime)),
		uintptr(unsafe.Pointer(&lpKernelTime)),
		uintptr(unsafe.Pointer(&lpUserTime)))
	if r == 0 {
		return ret, windows.GetLastError()
	}

	LOT := float64(0.0000001)
	HIT := (LOT * 4294967296.0)
	idle := ((HIT * float64(lpIdleTime.DwHighDateTime)) + (LOT * float64(lpIdleTime.DwLowDateTime)))
	user := ((HIT * float64(lpUserTime.DwHighDateTime)) + (LOT * float64(lpUserTime.DwLowDateTime)))
	kernel := ((HIT * float64(lpKernelTime.DwHighDateTime)) + (LOT * float64(lpKernelTime.DwLowDateTime)))
	system := (kernel - idle)

	ret = append(ret, TimesStat{
		CPU:    "cpu-total",
		Idle:   float64(idle),
		User:   float64(user),
		System: float64(system),
	})
	return ret, nil
}

func Info() ([]InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) ([]InfoStat, error) {
	var ret []InfoStat
	var dst []Win32_Processor
	q := wmi.CreateQuery(&dst, "")
	if err := common.WMIQueryWithContext(ctx, q, &dst); err != nil {
		return ret, err
	}

	var procID string
	for i, l := range dst {
		procID = ""
		if l.ProcessorID != nil {
			procID = *l.ProcessorID
		}

		cpu := InfoStat{
			CPU:        int32(i),
			Family:     fmt.Sprintf("%d", l.Family),
			VendorID:   l.Manufacturer,
			ModelName:  l.Name,
			Cores:      int32(l.NumberOfLogicalProcessors),
			PhysicalID: procID,
			Mhz:        float64(l.MaxClockSpeed),
			Flags:      []string{},
		}
		ret = append(ret, cpu)
	}

	return ret, nil
}

// PerfInfo returns the performance counter's instance value for ProcessorInformation.
// Name property is the key by which overall, per cpu and per core metric is known.
func PerfInfo() ([]Win32_PerfFormattedData_Counters_ProcessorInformation, error) {
	return PerfInfoWithContext(context.Background())
}

func PerfInfoWithContext(ctx context.Context) ([]Win32_PerfFormattedData_Counters_ProcessorInformation, error) {
	var ret []Win32_PerfFormattedData_Counters_ProcessorInformation

	q := wmi.CreateQuery(&ret, "")
	err := common.WMIQueryWithContext(ctx, q, &ret)
	if err != nil {
		return []Win32_PerfFormattedData_Counters_ProcessorInformation{}, err
	}

	return ret, err
}

// ProcInfo returns processes count and processor queue length in the system.
// There is a single queue for processor even on multiprocessors systems.
func ProcInfo() ([]Win32_PerfFormattedData_PerfOS_System, error) {
	return ProcInfoWithContext(context.Background())
}

func ProcInfoWithContext(ctx context.Context) ([]Win32_PerfFormattedData_PerfOS_System, error) {
	var ret []Win32_PerfFormattedData_PerfOS_System
	q := wmi.CreateQuery(&ret, "")
	err := common.WMIQueryWithContext(ctx, q, &ret)
	if err != nil {
		return []Win32_PerfFormattedData_PerfOS_System{}, err
	}
	return ret, err
}

// perCPUTimes returns times stat per cpu, per core and overall for all CPUs
func perCPUTimes() ([]TimesStat, error) {
	var ret []TimesStat
	stats, err := PerfInfo()
	if err != nil {
		return nil, err
	}
	for _, v := range stats {
		c := TimesStat{
			CPU:    v.Name,
			User:   float64(v.PercentUserTime),
			System: float64(v.PercentPrivilegedTime),
			Idle:   float64(v.PercentIdleTime),
			Irq:    float64(v.PercentInterruptTime),
		}
		ret = append(ret, c)
	}
	return ret, nil
}
