//go:build solaris
// +build solaris

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
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// GetInfo returns total and free bytes available in a directory, e.g. `/`.
func GetInfo(path string, _ bool) (info Info, err error) {
	s := unix.Statvfs_t{}
	if err = unix.Statvfs(path, &s); err != nil {
		return Info{}, err
	}
	reservedBlocks := uint64(s.Bfree) - uint64(s.Bavail)
	info = Info{
		Total:  uint64(s.Frsize) * (uint64(s.Blocks) - reservedBlocks),
		Free:   uint64(s.Frsize) * uint64(s.Bavail),
		Files:  uint64(s.Files),
		Ffree:  uint64(s.Ffree),
		FSType: getFSType(s.Fstr[:]),
	}
	if info.Free > info.Total {
		return info, fmt.Errorf("detected free space (%d) > total drive space (%d), fs corruption at (%s). please run 'fsck'", info.Free, info.Total, path)
	}
	info.Used = info.Total - info.Free
	return info, nil
}

// GetDriveStats returns IO stats of the drive by its major:minor
func GetDriveStats(major, minor uint32) (iostats IOStats, err error) {
	return IOStats{}, errors.New("operation unsupported")
}
