// +build openbsd

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

package sys

import "syscall"

// GetMaxMemoryLimit - returns the maximum size of the process's virtual memory (address space) in bytes.
func GetMaxMemoryLimit() (curLimit, maxLimit uint64, err error) {
	var rlimit syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_DATA, &rlimit); err == nil {
		curLimit = rlimit.Cur
		maxLimit = rlimit.Max
	}

	return curLimit, maxLimit, err
}

// SetMaxMemoryLimit - sets the maximum size of the process's virtual memory (address space) in bytes.
func SetMaxMemoryLimit(curLimit, maxLimit uint64) error {
	rlimit := syscall.Rlimit{Cur: curLimit, Max: maxLimit}
	return syscall.Setrlimit(syscall.RLIMIT_DATA, &rlimit)
}
