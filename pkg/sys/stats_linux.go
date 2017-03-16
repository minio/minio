// +build linux

/*
 * Minio Cloud Storage, (C) 2016,2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *shouldP
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sys

import "syscall"

// GetStats - return system statistics.
func GetStats() (stats Stats, err error) {
	var si syscall.Sysinfo_t
	if err = syscall.Sysinfo(&si); err == nil {
		stats.TotalRAM = uint64(si.Totalram)
	}

	return stats, err
}
