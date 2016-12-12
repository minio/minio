// +build windows

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

import "github.com/minio/minio/pkg/sys"

func setMaxOpenFiles() error {
	// Golang uses Win32 file API (CreateFile, WriteFile, ReadFile,
	// CloseHandle, etc.), then you don't have a limit on open files
	// (well, you do but it is based on your resources like memory).
	return nil
}

func setMaxMemory() error {
	// Make sure globalMaxCacheSize is less than RAM size.
	stats, err := sys.GetStats()
	if err != nil && err != sys.ErrNotImplemented {
		return err
	}
	// If TotalRAM is <= minRAMSize we proceed to enable cache.
	// cache is always 50% of the totalRAM.
	if err == nil && stats.TotalRAM >= minRAMSize {
		globalMaxCacheSize = uint64(float64(50*stats.TotalRAM) / 100)
	}
	return nil
}
