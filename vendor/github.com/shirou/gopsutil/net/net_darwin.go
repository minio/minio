// +build darwin

package net

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var (
	errNetstatHeader  = errors.New("Can't parse header of netstat output")
	netstatLinkRegexp = regexp.MustCompile(`^<Link#(\d+)>$`)
)

const endOfLine = "\n"

func parseNetstatLine(line string) (stat *IOCountersStat, linkID *uint, err error) {
	var (
		numericValue uint64
		columns      = strings.Fields(line)
	)

	if columns[0] == "Name" {
		err = errNetstatHeader
		return
	}

	// try to extract the numeric value from <Link#123>
	if subMatch := netstatLinkRegexp.FindStringSubmatch(columns[2]); len(subMatch) == 2 {
		numericValue, err = strconv.ParseUint(subMatch[1], 10, 64)
		if err != nil {
			return
		}
		linkIDUint := uint(numericValue)
		linkID = &linkIDUint
	}

	base := 1
	numberColumns := len(columns)
	// sometimes Address is ommitted
	if numberColumns < 12 {
		base = 0
	}
	if numberColumns < 11 || numberColumns > 13 {
		err = fmt.Errorf("Line %q do have an invalid number of columns %d", line, numberColumns)
		return
	}

	parsed := make([]uint64, 0, 7)
	vv := []string{
		columns[base+3], // Ipkts == PacketsRecv
		columns[base+4], // Ierrs == Errin
		columns[base+5], // Ibytes == BytesRecv
		columns[base+6], // Opkts == PacketsSent
		columns[base+7], // Oerrs == Errout
		columns[base+8], // Obytes == BytesSent
	}
	if len(columns) == 12 {
		vv = append(vv, columns[base+10])
	}

	for _, target := range vv {
		if target == "-" {
			parsed = append(parsed, 0)
			continue
		}

		if numericValue, err = strconv.ParseUint(target, 10, 64); err != nil {
			return
		}
		parsed = append(parsed, numericValue)
	}

	stat = &IOCountersStat{
		Name:        strings.Trim(columns[0], "*"), // remove the * that sometimes is on right on interface
		PacketsRecv: parsed[0],
		Errin:       parsed[1],
		BytesRecv:   parsed[2],
		PacketsSent: parsed[3],
		Errout:      parsed[4],
		BytesSent:   parsed[5],
	}
	if len(parsed) == 7 {
		stat.Dropout = parsed[6]
	}
	return
}

type netstatInterface struct {
	linkID *uint
	stat   *IOCountersStat
}

func parseNetstatOutput(output string) ([]netstatInterface, error) {
	var (
		err   error
		lines = strings.Split(strings.Trim(output, endOfLine), endOfLine)
	)

	// number of interfaces is number of lines less one for the header
	numberInterfaces := len(lines) - 1

	interfaces := make([]netstatInterface, numberInterfaces)
	// no output beside header
	if numberInterfaces == 0 {
		return interfaces, nil
	}

	for index := 0; index < numberInterfaces; index++ {
		nsIface := netstatInterface{}
		if nsIface.stat, nsIface.linkID, err = parseNetstatLine(lines[index+1]); err != nil {
			return nil, err
		}
		interfaces[index] = nsIface
	}
	return interfaces, nil
}

// map that hold the name of a network interface and the number of usage
type mapInterfaceNameUsage map[string]uint

func newMapInterfaceNameUsage(ifaces []netstatInterface) mapInterfaceNameUsage {
	output := make(mapInterfaceNameUsage)
	for index := range ifaces {
		if ifaces[index].linkID != nil {
			ifaceName := ifaces[index].stat.Name
			usage, ok := output[ifaceName]
			if ok {
				output[ifaceName] = usage + 1
			} else {
				output[ifaceName] = 1
			}
		}
	}
	return output
}

func (min mapInterfaceNameUsage) isTruncated() bool {
	for _, usage := range min {
		if usage > 1 {
			return true
		}
	}
	return false
}

func (min mapInterfaceNameUsage) notTruncated() []string {
	output := make([]string, 0)
	for ifaceName, usage := range min {
		if usage == 1 {
			output = append(output, ifaceName)
		}
	}
	return output
}

// example of `netstat -ibdnW` output on yosemite
// Name  Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll Drop
// lo0   16384 <Link#1>                        869107     0  169411755   869107     0  169411755     0   0
// lo0   16384 ::1/128     ::1                 869107     -  169411755   869107     -  169411755     -   -
// lo0   16384 127           127.0.0.1         869107     -  169411755   869107     -  169411755     -   -
func IOCounters(pernic bool) ([]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), pernic)
}

func IOCountersWithContext(ctx context.Context, pernic bool) ([]IOCountersStat, error) {
	var (
		ret      []IOCountersStat
		retIndex int
	)

	netstat, err := exec.LookPath("/usr/sbin/netstat")
	if err != nil {
		return nil, err
	}

	// try to get all interface metrics, and hope there won't be any truncated
	out, err := invoke.CommandWithContext(ctx, netstat, "-ibdnW")
	if err != nil {
		return nil, err
	}

	nsInterfaces, err := parseNetstatOutput(string(out))
	if err != nil {
		return nil, err
	}

	ifaceUsage := newMapInterfaceNameUsage(nsInterfaces)
	notTruncated := ifaceUsage.notTruncated()
	ret = make([]IOCountersStat, len(notTruncated))

	if !ifaceUsage.isTruncated() {
		// no truncated interface name, return stats of all interface with <Link#...>
		for index := range nsInterfaces {
			if nsInterfaces[index].linkID != nil {
				ret[retIndex] = *nsInterfaces[index].stat
				retIndex++
			}
		}
	} else {
		// duplicated interface, list all interfaces
		ifconfig, err := exec.LookPath("/sbin/ifconfig")
		if err != nil {
			return nil, err
		}
		if out, err = invoke.CommandWithContext(ctx, ifconfig, "-l"); err != nil {
			return nil, err
		}
		interfaceNames := strings.Fields(strings.TrimRight(string(out), endOfLine))

		// for each of the interface name, run netstat if we don't have any stats yet
		for _, interfaceName := range interfaceNames {
			truncated := true
			for index := range nsInterfaces {
				if nsInterfaces[index].linkID != nil && nsInterfaces[index].stat.Name == interfaceName {
					// handle the non truncated name to avoid execute netstat for them again
					ret[retIndex] = *nsInterfaces[index].stat
					retIndex++
					truncated = false
					break
				}
			}
			if truncated {
				// run netstat with -I$ifacename
				if out, err = invoke.CommandWithContext(ctx, netstat, "-ibdnWI"+interfaceName); err != nil {
					return nil, err
				}
				parsedIfaces, err := parseNetstatOutput(string(out))
				if err != nil {
					return nil, err
				}
				if len(parsedIfaces) == 0 {
					// interface had been removed since `ifconfig -l` had been executed
					continue
				}
				for index := range parsedIfaces {
					if parsedIfaces[index].linkID != nil {
						ret = append(ret, *parsedIfaces[index].stat)
						break
					}
				}
			}
		}
	}

	if pernic == false {
		return getIOCountersAll(ret)
	}
	return ret, nil
}

// NetIOCountersByFile is an method which is added just a compatibility for linux.
func IOCountersByFile(pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCountersByFileWithContext(context.Background(), pernic, filename)
}

func IOCountersByFileWithContext(ctx context.Context, pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCounters(pernic)
}

func FilterCounters() ([]FilterStat, error) {
	return FilterCountersWithContext(context.Background())
}

func FilterCountersWithContext(ctx context.Context) ([]FilterStat, error) {
	return nil, errors.New("NetFilterCounters not implemented for darwin")
}

// NetProtoCounters returns network statistics for the entire system
// If protocols is empty then all protocols are returned, otherwise
// just the protocols in the list are returned.
// Not Implemented for Darwin
func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return ProtoCountersWithContext(context.Background(), protocols)
}

func ProtoCountersWithContext(ctx context.Context, protocols []string) ([]ProtoCountersStat, error) {
	return nil, errors.New("NetProtoCounters not implemented for darwin")
}
