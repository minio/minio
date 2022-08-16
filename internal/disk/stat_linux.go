//go:build linux && !s390x && !arm && !386
// +build linux,!s390x,!arm,!386

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

package disk

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// GetInfo returns total and free bytes available in a directory, e.g. `/`.
func GetInfo(path string) (info Info, err error) {
	s := syscall.Statfs_t{}
	err = syscall.Statfs(path, &s)
	if err != nil {
		return Info{}, err
	}
	reservedBlocks := s.Bfree - s.Bavail
	info = Info{
		Total: uint64(s.Frsize) * (s.Blocks - reservedBlocks),
		Free:  uint64(s.Frsize) * s.Bavail,
		Files: s.Files,
		Ffree: s.Ffree,
		//nolint:unconvert
		FSType: getFSType(int64(s.Type)),
	}
	// Check for overflows.
	// https://github.com/minio/minio/issues/8035
	// XFS can show wrong values at times error out
	// in such scenarios.
	if info.Free > info.Total {
		return info, fmt.Errorf("detected free space (%d) > total drive space (%d), fs corruption at (%s). please run 'fsck'", info.Free, info.Total, path)
	}
	info.Used = info.Total - info.Free

	st := syscall.Stat_t{}
	err = syscall.Stat(path, &st)
	if err != nil {
		return Info{}, err
	}
	//nolint:unconvert
	devID := uint64(st.Dev) // Needed to support multiple GOARCHs
	info.Major = unix.Major(devID)
	info.Minor = unix.Minor(devID)
	return info, nil
}

const (
	statsPath = "/proc/diskstats"
)

// GetAllDrivesIOStats returns IO stats of all drives found in the machine
func GetAllDrivesIOStats() (info AllDrivesIOStats, err error) {
	proc, err := os.Open(statsPath)
	if err != nil {
		return nil, err
	}
	defer proc.Close()

	ret := make(AllDrivesIOStats)

	sc := bufio.NewScanner(proc)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) < 11 {
			continue
		}

		var err error
		var ds IOStats

		ds.ReadIOs, err = strconv.ParseUint((fields[3]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.ReadMerges, err = strconv.ParseUint((fields[4]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.ReadSectors, err = strconv.ParseUint((fields[5]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.ReadTicks, err = strconv.ParseUint((fields[6]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.WriteIOs, err = strconv.ParseUint((fields[7]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.WriteMerges, err = strconv.ParseUint((fields[8]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.WriteSectors, err = strconv.ParseUint((fields[9]), 10, 64)
		if err != nil {
			return ret, err
		}
		ds.WriteTicks, err = strconv.ParseUint((fields[10]), 10, 64)
		if err != nil {
			return ret, err
		}

		if len(fields) > 11 {
			ds.CurrentIOs, err = strconv.ParseUint((fields[11]), 10, 64)
			if err != nil {
				return ret, err
			}

			ds.TotalTicks, err = strconv.ParseUint((fields[12]), 10, 64)
			if err != nil {
				return ret, err
			}
			ds.ReqTicks, err = strconv.ParseUint((fields[13]), 10, 64)
			if err != nil {
				return ret, err
			}
		}

		if len(fields) > 14 {
			ds.DiscardIOs, err = strconv.ParseUint((fields[14]), 10, 64)
			if err != nil {
				return ret, err
			}
			ds.DiscardMerges, err = strconv.ParseUint((fields[15]), 10, 64)
			if err != nil {
				return ret, err
			}
			ds.DiscardSectors, err = strconv.ParseUint((fields[16]), 10, 64)
			if err != nil {
				return ret, err
			}
			ds.DiscardTicks, err = strconv.ParseUint((fields[17]), 10, 64)
			if err != nil {
				return ret, err
			}
		}

		if len(fields) > 18 {
			ds.FlushIOs, err = strconv.ParseUint((fields[18]), 10, 64)
			if err != nil {
				return ret, err
			}
			ds.FlushTicks, err = strconv.ParseUint((fields[19]), 10, 64)
			if err != nil {
				return ret, err
			}
		}

		major, err := strconv.ParseUint((fields[0]), 10, 32)
		if err != nil {
			return ret, err
		}

		minor, err := strconv.ParseUint((fields[1]), 10, 32)
		if err != nil {
			return ret, err
		}
		ret[DevID{uint32(major), uint32(minor)}] = ds
	}

	if err := sc.Err(); err != nil {
		return nil, err
	}

	return ret, nil
}
