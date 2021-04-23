// +build netbsd

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

package sys

import (
	"encoding/binary"
	"syscall"
)

func getHwPhysmem() (uint64, error) {
	totalString, err := syscall.Sysctl("hw.physmem64")
	if err != nil {
		return 0, err
	}

	// syscall.sysctl() helpfully assumes the result is a null-terminated string and
	// removes the last byte of the result if it's 0 :/
	totalString += "\x00"

	total := uint64(binary.LittleEndian.Uint64([]byte(totalString)))

	return total, nil
}

// GetStats - return system statistics for bsd.
func GetStats() (stats Stats, err error) {
	stats.TotalRAM, err = getHwPhysmem()
	return stats, err
}
