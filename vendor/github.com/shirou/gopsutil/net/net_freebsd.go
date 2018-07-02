// +build freebsd

package net

import (
	"context"
	"errors"
	"os/exec"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
)

func IOCounters(pernic bool) ([]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), pernic)
}

func IOCountersWithContext(ctx context.Context, pernic bool) ([]IOCountersStat, error) {
	netstat, err := exec.LookPath("/usr/bin/netstat")
	if err != nil {
		return nil, err
	}
	out, err := invoke.CommandWithContext(ctx, netstat, "-ibdnW")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(out), "\n")
	ret := make([]IOCountersStat, 0, len(lines)-1)
	exists := make([]string, 0, len(ret))

	for _, line := range lines {
		values := strings.Fields(line)
		if len(values) < 1 || values[0] == "Name" {
			continue
		}
		if common.StringsHas(exists, values[0]) {
			// skip if already get
			continue
		}
		exists = append(exists, values[0])

		if len(values) < 12 {
			continue
		}
		base := 1
		// sometimes Address is ommitted
		if len(values) < 13 {
			base = 0
		}

		parsed := make([]uint64, 0, 8)
		vv := []string{
			values[base+3],  // PacketsRecv
			values[base+4],  // Errin
			values[base+5],  // Dropin
			values[base+6],  // BytesRecvn
			values[base+7],  // PacketSent
			values[base+8],  // Errout
			values[base+9],  // BytesSent
			values[base+11], // Dropout
		}
		for _, target := range vv {
			if target == "-" {
				parsed = append(parsed, 0)
				continue
			}

			t, err := strconv.ParseUint(target, 10, 64)
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, t)
		}

		n := IOCountersStat{
			Name:        values[0],
			PacketsRecv: parsed[0],
			Errin:       parsed[1],
			Dropin:      parsed[2],
			BytesRecv:   parsed[3],
			PacketsSent: parsed[4],
			Errout:      parsed[5],
			BytesSent:   parsed[6],
			Dropout:     parsed[7],
		}
		ret = append(ret, n)
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
	return nil, errors.New("NetFilterCounters not implemented for freebsd")
}

// NetProtoCounters returns network statistics for the entire system
// If protocols is empty then all protocols are returned, otherwise
// just the protocols in the list are returned.
// Not Implemented for FreeBSD
func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return ProtoCountersWithContext(context.Background(), protocols)
}

func ProtoCountersWithContext(ctx context.Context, protocols []string) ([]ProtoCountersStat, error) {
	return nil, errors.New("NetProtoCounters not implemented for freebsd")
}
