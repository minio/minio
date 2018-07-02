// +build linux

package mem

import (
	"context"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
	"golang.org/x/sys/unix"
)

func VirtualMemory() (*VirtualMemoryStat, error) {
	return VirtualMemoryWithContext(context.Background())
}

func VirtualMemoryWithContext(ctx context.Context) (*VirtualMemoryStat, error) {
	filename := common.HostProc("meminfo")
	lines, _ := common.ReadLines(filename)
	// flag if MemAvailable is in /proc/meminfo (kernel 3.14+)
	memavail := false

	ret := &VirtualMemoryStat{}
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		key := strings.TrimSpace(fields[0])
		value := strings.TrimSpace(fields[1])
		value = strings.Replace(value, " kB", "", -1)

		t, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return ret, err
		}
		switch key {
		case "MemTotal":
			ret.Total = t * 1024
		case "MemFree":
			ret.Free = t * 1024
		case "MemAvailable":
			memavail = true
			ret.Available = t * 1024
		case "Buffers":
			ret.Buffers = t * 1024
		case "Cached":
			ret.Cached = t * 1024
		case "Active":
			ret.Active = t * 1024
		case "Inactive":
			ret.Inactive = t * 1024
		case "Writeback":
			ret.Writeback = t * 1024
		case "WritebackTmp":
			ret.WritebackTmp = t * 1024
		case "Dirty":
			ret.Dirty = t * 1024
		case "Shmem":
			ret.Shared = t * 1024
		case "Slab":
			ret.Slab = t * 1024
		case "PageTables":
			ret.PageTables = t * 1024
		case "SwapCached":
			ret.SwapCached = t * 1024
		case "CommitLimit":
			ret.CommitLimit = t * 1024
		case "Committed_AS":
			ret.CommittedAS = t * 1024
		}
	}
	if !memavail {
		ret.Available = ret.Free + ret.Buffers + ret.Cached
	}
	ret.Used = ret.Total - ret.Free - ret.Buffers - ret.Cached
	ret.UsedPercent = float64(ret.Used) / float64(ret.Total) * 100.0

	return ret, nil
}

func SwapMemory() (*SwapMemoryStat, error) {
	return SwapMemoryWithContext(context.Background())
}

func SwapMemoryWithContext(ctx context.Context) (*SwapMemoryStat, error) {
	sysinfo := &unix.Sysinfo_t{}

	if err := unix.Sysinfo(sysinfo); err != nil {
		return nil, err
	}
	ret := &SwapMemoryStat{
		Total: uint64(sysinfo.Totalswap) * uint64(sysinfo.Unit),
		Free:  uint64(sysinfo.Freeswap) * uint64(sysinfo.Unit),
	}
	ret.Used = ret.Total - ret.Free
	//check Infinity
	if ret.Total != 0 {
		ret.UsedPercent = float64(ret.Total-ret.Free) / float64(ret.Total) * 100.0
	} else {
		ret.UsedPercent = 0
	}
	filename := common.HostProc("vmstat")
	lines, _ := common.ReadLines(filename)
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "pswpin":
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				continue
			}
			ret.Sin = value * 4 * 1024
		case "pswpout":
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				continue
			}
			ret.Sout = value * 4 * 1024
		}
	}
	return ret, nil
}
