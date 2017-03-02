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
	"github.com/minio/minio/pkg/sys"
)

func setMaxOpenFiles() error {
	_, maxLimit, err := sys.GetMaxOpenFileLimit()
	if err != nil {
		return err
	}

	return sys.SetMaxOpenFileLimit(maxLimit, maxLimit)
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

func setMaxMemory() error {
	// Get max memory limit
	_, maxLimit, err := sys.GetMaxMemoryLimit()
	if err != nil {
		return err
	}

	// Set max memory limit as current memory limit.
	if err = sys.SetMaxMemoryLimit(maxLimit, maxLimit); err != nil {
		return err
	}

	// Get total RAM.
	stats, err := sys.GetStats()
	if err != nil {
		return err
	}

	// In some OS like windows, maxLimit is zero.  Set total RAM as maxLimit.
	if maxLimit == 0 {
		maxLimit = stats.TotalRAM
	}

	globalMaxCacheSize = getMaxCacheSize(maxLimit, stats.TotalRAM)
	return nil
}
