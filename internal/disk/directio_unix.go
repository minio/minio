//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

// Copyright (c) 2015-2023 MinIO, Inc.
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
	"os"
	"syscall"

	"github.com/ncw/directio"
	"golang.org/x/sys/unix"
)

// ODirectPlatform indicates if the platform supports O_DIRECT
const ODirectPlatform = true

// OpenFileDirectIO - bypass kernel cache.
func OpenFileDirectIO(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	return directio.OpenFile(filePath, flag, perm)
}

// DisableDirectIO - disables directio mode.
func DisableDirectIO(f *os.File) error {
	fd := f.Fd()
	flag, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	flag &= ^(syscall.O_DIRECT)
	_, err = unix.FcntlInt(fd, unix.F_SETFL, flag)
	return err
}

// AlignedBlock - pass through to directio implementation.
func AlignedBlock(blockSize int) []byte {
	return directio.AlignedBlock(blockSize)
}
