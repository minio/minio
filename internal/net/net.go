// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package net

import (
	"bufio"
	"strconv"
	"strings"
)

// Inter - is for NIC Name
type Inter string

// AllNetStats - is for parse the /proc/net/dev result
type AllNetStats map[Inter]NetStats

// NetStats - is for net stats, include Receive and Transmit
type NetStats struct {
	Receive  *ReceiveNetStats
	Transmit *TransmitNetStats
}

// ReceiveNetStats - is for net stats for Receive
type ReceiveNetStats struct {
	Bytes      uint64
	Packets    uint64
	Errs       uint64
	Drop       uint64
	Fifo       uint64
	Frame      uint64
	Compressed uint64
	Multicast  uint64
}

// TransmitNetStats - is for net stats for Transmit
type TransmitNetStats struct {
	Bytes      uint64
	Packets    uint64
	Errs       uint64
	Drop       uint64
	Fifo       uint64
	Colls      uint64
	Carrier    uint64
	Compressed uint64
}

const (
	statsPath = "/proc/net/dev"
)

// GetAllNetStats - parse the /proc/net/dev
func GetAllNetStats() (AllNetStats, error) {
	//if runtime.GOOS == "linux" {
	//	netStats, err := os.Open(statsPath)
	//	if err != nil {
	//		return nil, err
	//	}
	//	defer netStats.Close()

	netStats := strings.NewReader(`Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0
 bond0:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0
dummy0:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0
 tunl0:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0
  sit0:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0
  eth0:   21000     119    0    0    0     0          0       119     1146      15    0    0    0     0       0          0`)

	ret := make(AllNetStats)
	sc := bufio.NewScanner(netStats)
	for sc.Scan() {
		line := sc.Text()
		// info line
		if strings.Contains(line, "|") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 17 {
			ret[parseInter(fields[0])] = NetStats{
				Receive: &ReceiveNetStats{
					Bytes:      fastParseNumber(fields[1]),
					Packets:    fastParseNumber(fields[2]),
					Errs:       fastParseNumber(fields[3]),
					Drop:       fastParseNumber(fields[4]),
					Fifo:       fastParseNumber(fields[5]),
					Frame:      fastParseNumber(fields[6]),
					Compressed: fastParseNumber(fields[7]),
					Multicast:  fastParseNumber(fields[8]),
				},
				Transmit: &TransmitNetStats{
					Bytes:      fastParseNumber(fields[9]),
					Packets:    fastParseNumber(fields[10]),
					Errs:       fastParseNumber(fields[11]),
					Drop:       fastParseNumber(fields[12]),
					Fifo:       fastParseNumber(fields[13]),
					Colls:      fastParseNumber(fields[14]),
					Carrier:    fastParseNumber(fields[15]),
					Compressed: fastParseNumber(fields[16]),
				},
			}
		}
	}
	return ret, nil
	//}
}

func fastParseNumber(s string) uint64 {
	out, _ := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	return out
}

func parseInter(s string) Inter {
	return Inter(strings.SplitN(strings.TrimSpace(s), ":", 2)[0])
}
