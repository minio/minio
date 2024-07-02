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
	"runtime"
	"runtime/debug"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3/kernel"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/sys"
)

func oldLinux() bool {
	currentKernel, err := kernel.CurrentVersion()
	if err != nil {
		// Could not probe the kernel version
		return false
	}

	if currentKernel == 0 {
		// We could not get any valid value return false
		return false
	}

	// legacy linux indicator for printing warnings
	// about older Linux kernels and Go runtime.
	return currentKernel < kernel.Version(4, 0, 0)
}

func setMaxResources(ctx serverCtxt) (err error) {
	// Set the Go runtime max threads threshold to 90% of kernel setting.
	sysMaxThreads, err := sys.GetMaxThreads()
	if err == nil {
		minioMaxThreads := (sysMaxThreads * 90) / 100
		// Only set max threads if it is greater than the default one
		if minioMaxThreads > 10000 {
			debug.SetMaxThreads(minioMaxThreads)
		}
	}

	var maxLimit uint64

	// Set open files limit to maximum.
	if _, maxLimit, err = sys.GetMaxOpenFileLimit(); err != nil {
		return err
	}

	if maxLimit < 4096 && runtime.GOOS != globalWindowsOSName {
		logger.Info("WARNING: maximum file descriptor limit %d is too low for production servers. At least 4096 is recommended. Fix with \"ulimit -n 4096\"",
			maxLimit)
	}

	if err = sys.SetMaxOpenFileLimit(maxLimit, maxLimit); err != nil {
		return err
	}

	_, vssLimit, err := sys.GetMaxMemoryLimit()
	if err != nil {
		return err
	}

	if vssLimit > 0 && vssLimit < humanize.GiByte {
		logger.Info("WARNING: maximum virtual memory limit (%s) is too small for 'go runtime', please consider setting `ulimit -v` to unlimited",
			humanize.IBytes(vssLimit))
	}

	if ctx.MemLimit > 0 {
		debug.SetMemoryLimit(int64(ctx.MemLimit))
	}

	// Do not use RLIMIT_AS as that is not useful and at times on systems < 4Gi
	// this can crash the Go runtime if the value is smaller refer
	// - https://github.com/golang/go/issues/38010
	// - https://github.com/golang/go/issues/43699
	// So do not add `sys.SetMaxMemoryLimit()` this is not useful for any practical purposes.
	return nil
}
