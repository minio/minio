package mem

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
)

// VirtualMemory for Solaris is a minimal implementation which only returns
// what Nomad needs. It does take into account global vs zone, however.
func VirtualMemory() (*VirtualMemoryStat, error) {
	return VirtualMemoryWithContext(context.Background())
}

func VirtualMemoryWithContext(ctx context.Context) (*VirtualMemoryStat, error) {
	result := &VirtualMemoryStat{}

	zoneName, err := zoneName()
	if err != nil {
		return nil, err
	}

	if zoneName == "global" {
		cap, err := globalZoneMemoryCapacity()
		if err != nil {
			return nil, err
		}
		result.Total = cap
	} else {
		cap, err := nonGlobalZoneMemoryCapacity()
		if err != nil {
			return nil, err
		}
		result.Total = cap
	}

	return result, nil
}

func SwapMemory() (*SwapMemoryStat, error) {
	return SwapMemoryWithContext(context.Background())
}

func SwapMemoryWithContext(ctx context.Context) (*SwapMemoryStat, error) {
	return nil, common.ErrNotImplementedError
}

func zoneName() (string, error) {
	zonename, err := exec.LookPath("/usr/bin/zonename")
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	out, err := invoke.CommandWithContext(ctx, zonename)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

var globalZoneMemoryCapacityMatch = regexp.MustCompile(`memory size: ([\d]+) Megabytes`)

func globalZoneMemoryCapacity() (uint64, error) {
	prtconf, err := exec.LookPath("/usr/sbin/prtconf")
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	out, err := invoke.CommandWithContext(ctx, prtconf)
	if err != nil {
		return 0, err
	}

	match := globalZoneMemoryCapacityMatch.FindAllStringSubmatch(string(out), -1)
	if len(match) != 1 {
		return 0, errors.New("memory size not contained in output of /usr/sbin/prtconf")
	}

	totalMB, err := strconv.ParseUint(match[0][1], 10, 64)
	if err != nil {
		return 0, err
	}

	return totalMB * 1024 * 1024, nil
}

var kstatMatch = regexp.MustCompile(`([^\s]+)[\s]+([^\s]*)`)

func nonGlobalZoneMemoryCapacity() (uint64, error) {
	kstat, err := exec.LookPath("/usr/bin/kstat")
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	out, err := invoke.CommandWithContext(ctx, kstat, "-p", "-c", "zone_memory_cap", "memory_cap:*:*:physcap")
	if err != nil {
		return 0, err
	}

	kstats := kstatMatch.FindAllStringSubmatch(string(out), -1)
	if len(kstats) != 1 {
		return 0, fmt.Errorf("expected 1 kstat, found %d", len(kstats))
	}

	memSizeBytes, err := strconv.ParseUint(kstats[0][2], 10, 64)
	if err != nil {
		return 0, err
	}

	return memSizeBytes, nil
}
