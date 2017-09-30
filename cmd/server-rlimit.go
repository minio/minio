/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"runtime/debug"

	"github.com/minio/minio/pkg/sys"
)

func setMaxResources() (err error) {
	// Set the Go runtime max threads threshold to 90% of kernel setting.
	// Do not return when an error when encountered since it is not a crucial task.
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

func getMaxCacheSize(curLimit, totalRAM uint64) (cacheSize uint64) {
	// Return zero if current limit or totalTAM is less than minRAMSize.
	if curLimit < minRAMSize || totalRAM < minRAMSize {
		return cacheSize
	}

	// Return 50% of current rlimit or total RAM as cache size.
	if curLimit < totalRAM {
		cacheSize = curLimit / 2
	} else {
		cacheSize = totalRAM / 2
	}

	return cacheSize
}

// GetMaxCacheSize returns maximum cache size based on current RAM size and memory limit.
func GetMaxCacheSize() (cacheSize uint64, err error) {
	// Get max memory limit
	var curLimit uint64
	if curLimit, _, err = sys.GetMaxMemoryLimit(); err != nil {
		return cacheSize, err
	}

	// Get total RAM.
	var stats sys.Stats
	if stats, err = sys.GetStats(); err != nil {
		return cacheSize, err
	}

	// In some OS like windows, maxLimit is zero.  Set total RAM as maxLimit.
	if curLimit == 0 {
		curLimit = stats.TotalRAM
	}

	cacheSize = getMaxCacheSize(curLimit, stats.TotalRAM)
	return cacheSize, err
}
