package cpu

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
)

var ClocksPerSec = float64(128)

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

func Times(percpu bool) ([]TimesStat, error) {
	return TimesWithContext(context.Background(), percpu)
}

func TimesWithContext(ctx context.Context, percpu bool) ([]TimesStat, error) {
	return []TimesStat{}, common.ErrNotImplementedError
}

func Info() ([]InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) ([]InfoStat, error) {
	psrInfo, err := exec.LookPath("/usr/sbin/psrinfo")
	if err != nil {
		return nil, fmt.Errorf("cannot find psrinfo: %s", err)
	}
	psrInfoOut, err := invoke.CommandWithContext(ctx, psrInfo, "-p", "-v")
	if err != nil {
		return nil, fmt.Errorf("cannot execute psrinfo: %s", err)
	}

	isaInfo, err := exec.LookPath("/usr/bin/isainfo")
	if err != nil {
		return nil, fmt.Errorf("cannot find isainfo: %s", err)
	}
	isaInfoOut, err := invoke.CommandWithContext(ctx, isaInfo, "-b", "-v")
	if err != nil {
		return nil, fmt.Errorf("cannot execute isainfo: %s", err)
	}

	procs, err := parseProcessorInfo(string(psrInfoOut))
	if err != nil {
		return nil, fmt.Errorf("error parsing psrinfo output: %s", err)
	}

	flags, err := parseISAInfo(string(isaInfoOut))
	if err != nil {
		return nil, fmt.Errorf("error parsing isainfo output: %s", err)
	}

	result := make([]InfoStat, 0, len(flags))
	for _, proc := range procs {
		procWithFlags := proc
		procWithFlags.Flags = flags
		result = append(result, procWithFlags)
	}

	return result, nil
}

var flagsMatch = regexp.MustCompile(`[\w\.]+`)

func parseISAInfo(cmdOutput string) ([]string, error) {
	words := flagsMatch.FindAllString(cmdOutput, -1)

	// Sanity check the output
	if len(words) < 4 || words[1] != "bit" || words[3] != "applications" {
		return nil, errors.New("attempted to parse invalid isainfo output")
	}

	flags := make([]string, len(words)-4)
	for i, val := range words[4:] {
		flags[i] = val
	}
	sort.Strings(flags)

	return flags, nil
}

var psrInfoMatch = regexp.MustCompile(`The physical processor has (?:([\d]+) virtual processor \(([\d]+)\)|([\d]+) cores and ([\d]+) virtual processors[^\n]+)\n(?:\s+ The core has.+\n)*\s+.+ \((\w+) ([\S]+) family (.+) model (.+) step (.+) clock (.+) MHz\)\n[\s]*(.*)`)

const (
	psrNumCoresOffset   = 1
	psrNumCoresHTOffset = 3
	psrNumHTOffset      = 4
	psrVendorIDOffset   = 5
	psrFamilyOffset     = 7
	psrModelOffset      = 8
	psrStepOffset       = 9
	psrClockOffset      = 10
	psrModelNameOffset  = 11
)

func parseProcessorInfo(cmdOutput string) ([]InfoStat, error) {
	matches := psrInfoMatch.FindAllStringSubmatch(cmdOutput, -1)

	var infoStatCount int32
	result := make([]InfoStat, 0, len(matches))
	for physicalIndex, physicalCPU := range matches {
		var step int32
		var clock float64

		if physicalCPU[psrStepOffset] != "" {
			stepParsed, err := strconv.ParseInt(physicalCPU[psrStepOffset], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot parse value %q for step as 32-bit integer: %s", physicalCPU[9], err)
			}
			step = int32(stepParsed)
		}

		if physicalCPU[psrClockOffset] != "" {
			clockParsed, err := strconv.ParseInt(physicalCPU[psrClockOffset], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse value %q for clock as 32-bit integer: %s", physicalCPU[10], err)
			}
			clock = float64(clockParsed)
		}

		var err error
		var numCores int64
		var numHT int64
		switch {
		case physicalCPU[psrNumCoresOffset] != "":
			numCores, err = strconv.ParseInt(physicalCPU[psrNumCoresOffset], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot parse value %q for core count as 32-bit integer: %s", physicalCPU[1], err)
			}

			for i := 0; i < int(numCores); i++ {
				result = append(result, InfoStat{
					CPU:        infoStatCount,
					PhysicalID: strconv.Itoa(physicalIndex),
					CoreID:     strconv.Itoa(i),
					Cores:      1,
					VendorID:   physicalCPU[psrVendorIDOffset],
					ModelName:  physicalCPU[psrModelNameOffset],
					Family:     physicalCPU[psrFamilyOffset],
					Model:      physicalCPU[psrModelOffset],
					Stepping:   step,
					Mhz:        clock,
				})
				infoStatCount++
			}
		case physicalCPU[psrNumCoresHTOffset] != "":
			numCores, err = strconv.ParseInt(physicalCPU[psrNumCoresHTOffset], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot parse value %q for core count as 32-bit integer: %s", physicalCPU[3], err)
			}

			numHT, err = strconv.ParseInt(physicalCPU[psrNumHTOffset], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot parse value %q for hyperthread count as 32-bit integer: %s", physicalCPU[4], err)
			}

			for i := 0; i < int(numCores); i++ {
				result = append(result, InfoStat{
					CPU:        infoStatCount,
					PhysicalID: strconv.Itoa(physicalIndex),
					CoreID:     strconv.Itoa(i),
					Cores:      int32(numHT) / int32(numCores),
					VendorID:   physicalCPU[psrVendorIDOffset],
					ModelName:  physicalCPU[psrModelNameOffset],
					Family:     physicalCPU[psrFamilyOffset],
					Model:      physicalCPU[psrModelOffset],
					Stepping:   step,
					Mhz:        clock,
				})
				infoStatCount++
			}
		default:
			return nil, errors.New("values for cores with and without hyperthreading are both set")
		}
	}
	return result, nil
}
