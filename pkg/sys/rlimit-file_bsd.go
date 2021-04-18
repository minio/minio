// +build freebsd dragonfly

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
	"syscall"
)

// GetMaxOpenFileLimit - returns maximum file descriptor number that can be opened by this process.
func GetMaxOpenFileLimit() (curLimit, maxLimit uint64, err error) {
	var rlimit syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err == nil {
		curLimit = uint64(rlimit.Cur)
		maxLimit = uint64(rlimit.Max)
	}

	return curLimit, maxLimit, err
}

// SetMaxOpenFileLimit - sets maximum file descriptor number that can be opened by this process.
func SetMaxOpenFileLimit(curLimit, maxLimit uint64) error {
	rlimit := syscall.Rlimit{Cur: int64(curLimit), Max: int64(curLimit)}
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit)
}
