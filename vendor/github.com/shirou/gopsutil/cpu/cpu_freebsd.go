package cpu

import (
	"context"
	"fmt"
	"os/exec"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	"github.com/shirou/gopsutil/internal/common"
	"golang.org/x/sys/unix"
)

var ClocksPerSec = float64(128)
var cpuMatch = regexp.MustCompile(`^CPU:`)
var originMatch = regexp.MustCompile(`Origin\s*=\s*"(.+)"\s+Id\s*=\s*(.+)\s+Family\s*=\s*(.+)\s+Model\s*=\s*(.+)\s+Stepping\s*=\s*(.+)`)
var featuresMatch = regexp.MustCompile(`Features=.+<(.+)>`)
var featuresMatch2 = regexp.MustCompile(`Features2=[a-f\dx]+<(.+)>`)
var cpuEnd = regexp.MustCompile(`^Trying to mount root`)
var cpuCores = regexp.MustCompile(`FreeBSD/SMP: (\d*) package\(s\) x (\d*) core\(s\)`)
var cpuTimesSize int
var emptyTimes cpuTimes

func init() {
	getconf, err := exec.LookPath("/usr/bin/getconf")
	if err != nil {
		return
	}
	out, err := invoke.Command(getconf, "CLK_TCK")
	// ignore errors
	if err == nil {
		i, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if err == nil {
			ClocksPerSec = float64(i)
		}
	}
}

func timeStat(name string, t *cpuTimes) *TimesStat {
	return &TimesStat{
		User:   float64(t.User) / ClocksPerSec,
		Nice:   float64(t.Nice) / ClocksPerSec,
		System: float64(t.Sys) / ClocksPerSec,
		Idle:   float64(t.Idle) / ClocksPerSec,
		Irq:    float64(t.Intr) / ClocksPerSec,
		CPU:    name,
	}
}

func Times(percpu bool) ([]TimesStat, error) {
	return TimesWithContext(context.Background(), percpu)
}

func TimesWithContext(ctx context.Context, percpu bool) ([]TimesStat, error) {
	if percpu {
		buf, err := unix.SysctlRaw("kern.cp_times")
		if err != nil {
			return nil, err
		}

		// We can't do this in init due to the conflict with cpu.init()
		if cpuTimesSize == 0 {
			cpuTimesSize = int(reflect.TypeOf(cpuTimes{}).Size())
		}

		ncpus := len(buf) / cpuTimesSize
		ret := make([]TimesStat, 0, ncpus)
		for i := 0; i < ncpus; i++ {
			times := (*cpuTimes)(unsafe.Pointer(&buf[i*cpuTimesSize]))
			if *times == emptyTimes {
				// CPU not present
				continue
			}
			ret = append(ret, *timeStat(fmt.Sprintf("cpu%d", len(ret)), times))
		}
		return ret, nil
	}

	buf, err := unix.SysctlRaw("kern.cp_time")
	if err != nil {
		return nil, err
	}

	times := (*cpuTimes)(unsafe.Pointer(&buf[0]))
	return []TimesStat{*timeStat("cpu-total", times)}, nil
}

// Returns only one InfoStat on FreeBSD.  The information regarding core
// count, however is accurate and it is assumed that all InfoStat attributes
// are the same across CPUs.
func Info() ([]InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) ([]InfoStat, error) {
	const dmesgBoot = "/var/run/dmesg.boot"

	c, num, err := parseDmesgBoot(dmesgBoot)
	if err != nil {
		return nil, err
	}

	var u32 uint32
	if u32, err = unix.SysctlUint32("hw.clockrate"); err != nil {
		return nil, err
	}
	c.Mhz = float64(u32)

	if u32, err = unix.SysctlUint32("hw.ncpu"); err != nil {
		return nil, err
	}
	c.Cores = int32(u32)

	if c.ModelName, err = unix.Sysctl("hw.model"); err != nil {
		return nil, err
	}

	ret := make([]InfoStat, num)
	for i := 0; i < num; i++ {
		ret[i] = c
	}

	return ret, nil
}

func parseDmesgBoot(fileName string) (InfoStat, int, error) {
	c := InfoStat{}
	lines, _ := common.ReadLines(fileName)
	cpuNum := 1 // default cpu num is 1
	for _, line := range lines {
		if matches := cpuEnd.FindStringSubmatch(line); matches != nil {
			break
		} else if matches := originMatch.FindStringSubmatch(line); matches != nil {
			c.VendorID = matches[1]
			c.Family = matches[3]
			c.Model = matches[4]
			t, err := strconv.ParseInt(matches[5], 10, 32)
			if err != nil {
				return c, 0, fmt.Errorf("unable to parse FreeBSD CPU stepping information from %q: %v", line, err)
			}
			c.Stepping = int32(t)
		} else if matches := featuresMatch.FindStringSubmatch(line); matches != nil {
			for _, v := range strings.Split(matches[1], ",") {
				c.Flags = append(c.Flags, strings.ToLower(v))
			}
		} else if matches := featuresMatch2.FindStringSubmatch(line); matches != nil {
			for _, v := range strings.Split(matches[1], ",") {
				c.Flags = append(c.Flags, strings.ToLower(v))
			}
		} else if matches := cpuCores.FindStringSubmatch(line); matches != nil {
			t, err := strconv.ParseInt(matches[1], 10, 32)
			if err != nil {
				return c, 0, fmt.Errorf("unable to parse FreeBSD CPU Nums from %q: %v", line, err)
			}
			cpuNum = int(t)
			t2, err := strconv.ParseInt(matches[2], 10, 32)
			if err != nil {
				return c, 0, fmt.Errorf("unable to parse FreeBSD CPU cores from %q: %v", line, err)
			}
			c.Cores = int32(t2)
		}
	}

	return c, cpuNum, nil
}
