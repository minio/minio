// +build darwin
// +build !cgo

package mem

import (
	"context"
	"os/exec"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Runs vm_stat and returns Free and inactive pages
func getVMStat(vms *VirtualMemoryStat) error {
	vm_stat, err := exec.LookPath("vm_stat")
	if err != nil {
		return err
	}
	out, err := invoke.Command(vm_stat)
	if err != nil {
		return err
	}
	return parseVMStat(string(out), vms)
}

func parseVMStat(out string, vms *VirtualMemoryStat) error {
	var err error

	lines := strings.Split(out, "\n")
	pagesize := uint64(unix.Getpagesize())
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSpace(fields[0])
		value := strings.Trim(fields[1], " .")
		switch key {
		case "Pages free":
			free, e := strconv.ParseUint(value, 10, 64)
			if e != nil {
				err = e
			}
			vms.Free = free * pagesize
		case "Pages inactive":
			inactive, e := strconv.ParseUint(value, 10, 64)
			if e != nil {
				err = e
			}
			vms.Inactive = inactive * pagesize
		case "Pages active":
			active, e := strconv.ParseUint(value, 10, 64)
			if e != nil {
				err = e
			}
			vms.Active = active * pagesize
		case "Pages wired down":
			wired, e := strconv.ParseUint(value, 10, 64)
			if e != nil {
				err = e
			}
			vms.Wired = wired * pagesize
		}
	}
	return err
}

// VirtualMemory returns VirtualmemoryStat.
func VirtualMemory() (*VirtualMemoryStat, error) {
	return VirtualMemoryWithContext(context.Background())
}

func VirtualMemoryWithContext(ctx context.Context) (*VirtualMemoryStat, error) {
	ret := &VirtualMemoryStat{}

	total, err := getHwMemsize()
	if err != nil {
		return nil, err
	}
	err = getVMStat(ret)
	if err != nil {
		return nil, err
	}

	ret.Available = ret.Free + ret.Inactive
	ret.Total = total

	ret.Used = ret.Total - ret.Available
	ret.UsedPercent = 100 * float64(ret.Used) / float64(ret.Total)

	return ret, nil
}
