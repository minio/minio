// +build darwin

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
	"encoding/binary"
	"syscall"
)

func getHwMemsize() (uint64, error) {
	totalString, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0, err
	}

	// syscall.sysctl() helpfully assumes the result is a null-terminated string and
	// removes the last byte of the result if it's 0 :/
	totalString += "\x00"

	total := uint64(binary.LittleEndian.Uint64([]byte(totalString)))

	return total, nil
}

// GetStats - return system statistics for macOS.
func GetStats() (stats Stats, err error) {
	stats.TotalRAM, err = getHwMemsize()
	return stats, err
}
