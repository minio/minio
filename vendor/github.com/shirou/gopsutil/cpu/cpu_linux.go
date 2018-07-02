// +build linux

package cpu

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
)

var cpu_tick = float64(100)

func init() {
	getconf, err := exec.LookPath("/usr/bin/getconf")
	if err != nil {
		return
	}
	out, err := invoke.CommandWithContext(context.Background(), getconf, "CLK_TCK")
	// ignore errors
	if err == nil {
		i, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if err == nil {
			cpu_tick = float64(i)
		}
	}
}

func Times(percpu bool) ([]TimesStat, error) {
	return TimesWithContext(context.Background(), percpu)
}

func TimesWithContext(ctx context.Context, percpu bool) ([]TimesStat, error) {
	filename := common.HostProc("stat")
	var lines = []string{}
	if percpu {
		statlines, err := common.ReadLines(filename)
		if err != nil || len(statlines) < 2 {
			return []TimesStat{}, nil
		}
		for _, line := range statlines[1:] {
			if !strings.HasPrefix(line, "cpu") {
				break
			}
			lines = append(lines, line)
		}
	} else {
		lines, _ = common.ReadLinesOffsetN(filename, 0, 1)
	}

	ret := make([]TimesStat, 0, len(lines))

	for _, line := range lines {
		ct, err := parseStatLine(line)
		if err != nil {
			continue
		}
		ret = append(ret, *ct)

	}
	return ret, nil
}

func sysCPUPath(cpu int32, relPath string) string {
	return common.HostSys(fmt.Sprintf("devices/system/cpu/cpu%d", cpu), relPath)
}

func finishCPUInfo(c *InfoStat) error {
	var lines []string
	var err error
	var value float64

	if len(c.CoreID) == 0 {
		lines, err = common.ReadLines(sysCPUPath(c.CPU, "topology/core_id"))
		if err == nil {
			c.CoreID = lines[0]
		}
	}

	// override the value of c.Mhz with cpufreq/cpuinfo_max_freq regardless
	// of the value from /proc/cpuinfo because we want to report the maximum
	// clock-speed of the CPU for c.Mhz, matching the behaviour of Windows
	lines, err = common.ReadLines(sysCPUPath(c.CPU, "cpufreq/cpuinfo_max_freq"))
	// if we encounter errors below such as there are no cpuinfo_max_freq file,
	// we just ignore. so let Mhz is 0.
	if err != nil {
		return nil
	}
	value, err = strconv.ParseFloat(lines[0], 64)
	if err != nil {
		return nil
	}
	c.Mhz = value / 1000.0 // value is in kHz
	if c.Mhz > 9999 {
		c.Mhz = c.Mhz / 1000.0 // value in Hz
	}
	return nil
}

// CPUInfo on linux will return 1 item per physical thread.
//
// CPUs have three levels of counting: sockets, cores, threads.
// Cores with HyperThreading count as having 2 threads per core.
// Sockets often come with many physical CPU cores.
// For example a single socket board with two cores each with HT will
// return 4 CPUInfoStat structs on Linux and the "Cores" field set to 1.
func Info() ([]InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) ([]InfoStat, error) {
	filename := common.HostProc("cpuinfo")
	lines, _ := common.ReadLines(filename)

	var ret []InfoStat
	var processorName string

	c := InfoStat{CPU: -1, Cores: 1}
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSpace(fields[0])
		value := strings.TrimSpace(fields[1])

		switch key {
		case "Processor":
			processorName = value
		case "processor":
			if c.CPU >= 0 {
				err := finishCPUInfo(&c)
				if err != nil {
					return ret, err
				}
				ret = append(ret, c)
			}
			c = InfoStat{Cores: 1, ModelName: processorName}
			t, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ret, err
			}
			c.CPU = int32(t)
		case "vendorId", "vendor_id":
			c.VendorID = value
		case "cpu family":
			c.Family = value
		case "model":
			c.Model = value
		case "model name", "cpu":
			c.ModelName = value
			if strings.Contains(value, "POWER8") ||
				strings.Contains(value, "POWER7") {
				c.Model = strings.Split(value, " ")[0]
				c.Family = "POWER"
				c.VendorID = "IBM"
			}
		case "stepping", "revision":
			val := value

			if key == "revision" {
				val = strings.Split(value, ".")[0]
			}

			t, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return ret, err
			}
			c.Stepping = int32(t)
		case "cpu MHz", "clock":
			// treat this as the fallback value, thus we ignore error
			if t, err := strconv.ParseFloat(strings.Replace(value, "MHz", "", 1), 64); err == nil {
				c.Mhz = t
			}
		case "cache size":
			t, err := strconv.ParseInt(strings.Replace(value, " KB", "", 1), 10, 64)
			if err != nil {
				return ret, err
			}
			c.CacheSize = int32(t)
		case "physical id":
			c.PhysicalID = value
		case "core id":
			c.CoreID = value
		case "flags", "Features":
			c.Flags = strings.FieldsFunc(value, func(r rune) bool {
				return r == ',' || r == ' '
			})
		case "microcode":
			c.Microcode = value
		}
	}
	if c.CPU >= 0 {
		err := finishCPUInfo(&c)
		if err != nil {
			return ret, err
		}
		ret = append(ret, c)
	}
	return ret, nil
}

func parseStatLine(line string) (*TimesStat, error) {
	fields := strings.Fields(line)

	if len(fields) == 0 {
		return nil, errors.New("stat does not contain cpu info")
	}

	if strings.HasPrefix(fields[0], "cpu") == false {
		//		return CPUTimesStat{}, e
		return nil, errors.New("not contain cpu")
	}

	cpu := fields[0]
	if cpu == "cpu" {
		cpu = "cpu-total"
	}
	user, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}
	nice, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return nil, err
	}
	system, err := strconv.ParseFloat(fields[3], 64)
	if err != nil {
		return nil, err
	}
	idle, err := strconv.ParseFloat(fields[4], 64)
	if err != nil {
		return nil, err
	}
	iowait, err := strconv.ParseFloat(fields[5], 64)
	if err != nil {
		return nil, err
	}
	irq, err := strconv.ParseFloat(fields[6], 64)
	if err != nil {
		return nil, err
	}
	softirq, err := strconv.ParseFloat(fields[7], 64)
	if err != nil {
		return nil, err
	}

	ct := &TimesStat{
		CPU:     cpu,
		User:    float64(user) / cpu_tick,
		Nice:    float64(nice) / cpu_tick,
		System:  float64(system) / cpu_tick,
		Idle:    float64(idle) / cpu_tick,
		Iowait:  float64(iowait) / cpu_tick,
		Irq:     float64(irq) / cpu_tick,
		Softirq: float64(softirq) / cpu_tick,
	}
	if len(fields) > 8 { // Linux >= 2.6.11
		steal, err := strconv.ParseFloat(fields[8], 64)
		if err != nil {
			return nil, err
		}
		ct.Steal = float64(steal) / cpu_tick
	}
	if len(fields) > 9 { // Linux >= 2.6.24
		guest, err := strconv.ParseFloat(fields[9], 64)
		if err != nil {
			return nil, err
		}
		ct.Guest = float64(guest) / cpu_tick
	}
	if len(fields) > 10 { // Linux >= 3.2.0
		guestNice, err := strconv.ParseFloat(fields[10], 64)
		if err != nil {
			return nil, err
		}
		ct.GuestNice = float64(guestNice) / cpu_tick
	}

	return ct, nil
}
