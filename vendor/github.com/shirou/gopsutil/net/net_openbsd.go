// +build openbsd

package net

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/shirou/gopsutil/internal/common"
)

var portMatch = regexp.MustCompile(`(.*)\.(\d+)$`)

func ParseNetstat(output string, mode string,
	iocs map[string]IOCountersStat) error {
	lines := strings.Split(output, "\n")

	exists := make([]string, 0, len(lines)-1)

	columns := 6
	if mode == "ind" {
		columns = 10
	}
	for _, line := range lines {
		values := strings.Fields(line)
		if len(values) < 1 || values[0] == "Name" {
			continue
		}
		if common.StringsHas(exists, values[0]) {
			// skip if already get
			continue
		}

		if len(values) < columns {
			continue
		}
		base := 1
		// sometimes Address is ommitted
		if len(values) < columns {
			base = 0
		}

		parsed := make([]uint64, 0, 8)
		var vv []string
		if mode == "inb" {
			vv = []string{
				values[base+3], // BytesRecv
				values[base+4], // BytesSent
			}
		} else {
			vv = []string{
				values[base+3], // Ipkts
				values[base+4], // Ierrs
				values[base+5], // Opkts
				values[base+6], // Oerrs
				values[base+8], // Drops
			}
		}
		for _, target := range vv {
			if target == "-" {
				parsed = append(parsed, 0)
				continue
			}

			t, err := strconv.ParseUint(target, 10, 64)
			if err != nil {
				return err
			}
			parsed = append(parsed, t)
		}
		exists = append(exists, values[0])

		n, present := iocs[values[0]]
		if !present {
			n = IOCountersStat{Name: values[0]}
		}
		if mode == "inb" {
			n.BytesRecv = parsed[0]
			n.BytesSent = parsed[1]
		} else {
			n.PacketsRecv = parsed[0]
			n.Errin = parsed[1]
			n.PacketsSent = parsed[2]
			n.Errout = parsed[3]
			n.Dropin = parsed[4]
			n.Dropout = parsed[4]
		}

		iocs[n.Name] = n
	}
	return nil
}

func IOCounters(pernic bool) ([]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), pernic)
}

func IOCountersWithContext(ctx context.Context, pernic bool) ([]IOCountersStat, error) {
	netstat, err := exec.LookPath("netstat")
	if err != nil {
		return nil, err
	}
	out, err := invoke.CommandWithContext(ctx, netstat, "-inb")
	if err != nil {
		return nil, err
	}
	out2, err := invoke.CommandWithContext(ctx, netstat, "-ind")
	if err != nil {
		return nil, err
	}
	iocs := make(map[string]IOCountersStat)

	lines := strings.Split(string(out), "\n")
	ret := make([]IOCountersStat, 0, len(lines)-1)

	err = ParseNetstat(string(out), "inb", iocs)
	if err != nil {
		return nil, err
	}
	err = ParseNetstat(string(out2), "ind", iocs)
	if err != nil {
		return nil, err
	}

	for _, ioc := range iocs {
		ret = append(ret, ioc)
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
	return nil, errors.New("NetFilterCounters not implemented for openbsd")
}

// NetProtoCounters returns network statistics for the entire system
// If protocols is empty then all protocols are returned, otherwise
// just the protocols in the list are returned.
// Not Implemented for OpenBSD
func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return ProtoCountersWithContext(context.Background(), protocols)
}

func ProtoCountersWithContext(ctx context.Context, protocols []string) ([]ProtoCountersStat, error) {
	return nil, errors.New("NetProtoCounters not implemented for openbsd")
}

func parseNetstatLine(line string) (ConnectionStat, error) {
	f := strings.Fields(line)
	if len(f) < 5 {
		return ConnectionStat{}, fmt.Errorf("wrong line,%s", line)
	}

	var netType, netFamily uint32
	switch f[0] {
	case "tcp":
		netType = syscall.SOCK_STREAM
		netFamily = syscall.AF_INET
	case "udp":
		netType = syscall.SOCK_DGRAM
		netFamily = syscall.AF_INET
	case "tcp6":
		netType = syscall.SOCK_STREAM
		netFamily = syscall.AF_INET6
	case "udp6":
		netType = syscall.SOCK_DGRAM
		netFamily = syscall.AF_INET6
	default:
		return ConnectionStat{}, fmt.Errorf("unknown type, %s", f[0])
	}

	laddr, raddr, err := parseNetstatAddr(f[3], f[4], netFamily)
	if err != nil {
		return ConnectionStat{}, fmt.Errorf("failed to parse netaddr, %s %s", f[3], f[4])
	}

	n := ConnectionStat{
		Fd:     uint32(0), // not supported
		Family: uint32(netFamily),
		Type:   uint32(netType),
		Laddr:  laddr,
		Raddr:  raddr,
		Pid:    int32(0), // not supported
	}
	if len(f) == 6 {
		n.Status = f[5]
	}

	return n, nil
}

func parseNetstatAddr(local string, remote string, family uint32) (laddr Addr, raddr Addr, err error) {
	parse := func(l string) (Addr, error) {
		matches := portMatch.FindStringSubmatch(l)
		if matches == nil {
			return Addr{}, fmt.Errorf("wrong addr, %s", l)
		}
		host := matches[1]
		port := matches[2]
		if host == "*" {
			switch family {
			case syscall.AF_INET:
				host = "0.0.0.0"
			case syscall.AF_INET6:
				host = "::"
			default:
				return Addr{}, fmt.Errorf("unknown family, %d", family)
			}
		}
		lport, err := strconv.Atoi(port)
		if err != nil {
			return Addr{}, err
		}
		return Addr{IP: host, Port: uint32(lport)}, nil
	}

	laddr, err = parse(local)
	if remote != "*.*" { // remote addr exists
		raddr, err = parse(remote)
		if err != nil {
			return laddr, raddr, err
		}
	}

	return laddr, raddr, err
}

// Return a list of network connections opened.
func Connections(kind string) ([]ConnectionStat, error) {
	return ConnectionsWithContext(context.Background(), kind)
}

func ConnectionsWithContext(ctx context.Context, kind string) ([]ConnectionStat, error) {
	var ret []ConnectionStat

	args := []string{"-na"}
	switch strings.ToLower(kind) {
	default:
		fallthrough
	case "":
		fallthrough
	case "all":
		fallthrough
	case "inet":
		// nothing to add
	case "inet4":
		args = append(args, "-finet")
	case "inet6":
		args = append(args, "-finet6")
	case "tcp":
		args = append(args, "-ptcp")
	case "tcp4":
		args = append(args, "-ptcp", "-finet")
	case "tcp6":
		args = append(args, "-ptcp", "-finet6")
	case "udp":
		args = append(args, "-pudp")
	case "udp4":
		args = append(args, "-pudp", "-finet")
	case "udp6":
		args = append(args, "-pudp", "-finet6")
	case "unix":
		return ret, common.ErrNotImplementedError
	}

	netstat, err := exec.LookPath("netstat")
	if err != nil {
		return nil, err
	}
	out, err := invoke.CommandWithContext(ctx, netstat, args...)

	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if !(strings.HasPrefix(line, "tcp") || strings.HasPrefix(line, "udp")) {
			continue
		}
		n, err := parseNetstatLine(line)
		if err != nil {
			continue
		}

		ret = append(ret, n)
	}

	return ret, nil
}
