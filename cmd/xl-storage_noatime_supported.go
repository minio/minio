//go:build unix && !darwin && !freebsd

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

package cmd

import (
	"os"
	"syscall"
)

var (
	// Disallow updating access times
	// Add non-block to avoid syscall to attempt to set epoll on files.
	readMode = os.O_RDONLY | 0x40000 | syscall.O_NONBLOCK // O_NOATIME

	// Write with data sync only used only for `xl.meta` writes
	writeMode = 0x1000 | syscall.O_NONBLOCK // O_DSYNC
)
