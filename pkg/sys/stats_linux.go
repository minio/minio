// +build linux

/*
 * Minio Cloud Storage, (C) 2016,2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sys

import (
	"os"
	"syscall"

	"github.com/minio/minio/pkg/cgroup"
)

// Get the final system memory limit chosen by the user.
// by default without any configuration on a vanilla Linux
// system you would see physical RAM limit. If cgroup
// is configured at some point in time this function
// would return the memory limit chosen for the given pid.
func getMemoryLimit() (sysLimit uint64, err error) {
	if sysLimit, err = getSysinfoMemoryLimit(); err != nil {
		// Physical memory info is not accessible, just exit here.
		return 0, err
	}

	// Following code is deliberately ignoring the error.
	cGroupLimit, gerr := cgroup.GetMemoryLimit(os.Getpid())
	if gerr != nil {
		// Upon error just return system limit.
		return sysLimit, nil
	}

	// cgroup limit is lesser than system limit means
	// user wants to limit the memory usage further
	// treat cgroup limit as the system limit.
	if cGroupLimit <= sysLimit {
		sysLimit = cGroupLimit
	}

	// Final system limit.
	return sysLimit, nil

}

// Get physical RAM size of the node.
func getSysinfoMemoryLimit() (limit uint64, err error) {
	var si syscall.Sysinfo_t
	if err = syscall.Sysinfo(&si); err != nil {
		return 0, err
	}

	// Some fields in syscall.Sysinfo_t have different  integer sizes
	// in different platform architectures. Cast all fields to uint64.
	totalRAM := uint64(si.Totalram)
	unit := uint64(si.Unit)

	// Total RAM is always the multiplicative value
	// of unit size and total ram.
	limit = unit * totalRAM
	return limit, nil
}

// GetStats - return system statistics, currently only
// supported value is TotalRAM.
func GetStats() (stats Stats, err error) {
	var limit uint64
	limit, err = getMemoryLimit()
	if err != nil {
		return Stats{}, err
	}

	stats.TotalRAM = limit
	return stats, nil
}
