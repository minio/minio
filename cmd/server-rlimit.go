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
	"runtime/debug"

	"github.com/minio/pkg/sys"
)

func setMaxResources() (err error) {
	// Set the Go runtime max threads threshold to 90% of kernel setting.
	sysMaxThreads, mErr := sys.GetMaxThreads()
	if mErr == nil {
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

	if err = sys.SetMaxOpenFileLimit(maxLimit, maxLimit); err != nil {
		return err
	}

	// Set max memory limit as current memory limit.
	if _, maxLimit, err = sys.GetMaxMemoryLimit(); err != nil {
		return err
	}

	err = sys.SetMaxMemoryLimit(maxLimit, maxLimit)
	return err
}
