//  +build !windows,!plan9

/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"syscall"

	"github.com/minio/minio/pkg/sys"
)

// For all unixes we need to bump allowed number of open files to a
// higher value than its usual default of '1024'. The reasoning is
// that this value is too small for a server.
func setMaxOpenFiles() error {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}
	// Set the current limit to Max, it is usually around 4096.
	// TO increase this limit further user has to manually edit
	// `/etc/security/limits.conf`
	rLimit.Cur = rLimit.Max
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
}

// Set max memory used by minio as a process, this value is usually
// set to 'unlimited' but we need to validate additionally to verify
// if any hard limit is set by the user, in such a scenario would need
// to reset the global max cache size to be 80% of the hardlimit set
// by the user. This is done to honor the system limits and not crash.
func setMaxMemory() error {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_AS, &rLimit)
	if err != nil {
		return err
	}
	// Set the current limit to Max, it is default 'unlimited'.
	// TO decrease this limit further user has to manually edit
	// `/etc/security/limits.conf`
	rLimit.Cur = rLimit.Max
	err = syscall.Setrlimit(syscall.RLIMIT_AS, &rLimit)
	if err != nil {
		return err
	}
	err = syscall.Getrlimit(syscall.RLIMIT_AS, &rLimit)
	if err != nil {
		return err
	}
	// Validate if rlimit memory is set to lower
	// than max cache size. Then we should use such value.
	if uint64(rLimit.Cur) < globalMaxCacheSize {
		globalMaxCacheSize = (80 / 100) * uint64(rLimit.Cur)
	}

	// Make sure globalMaxCacheSize is less than RAM size.
	stats, err := sys.GetStats()
	if err != nil && err != sys.ErrNotImplemented {
		// sys.GetStats() is implemented only on linux. Ignore errors
		// from other OSes.
		return err
	}
	if err == nil && stats.TotalRAM < globalMaxCacheSize {
		globalMaxCacheSize = (80 / 100) * stats.TotalRAM
	}
	return nil
}
