/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package cpu

import (
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func newCounter() (counter, error) {
	return counter{}, nil
}

func (c counter) now() time.Time {
	var ts syscall.Timespec
	// Retrieve Per-process CPU-time clock
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, unix.CLOCK_PROCESS_CPUTIME_ID, uintptr(unsafe.Pointer(&ts)), 0)
	sec, nsec := ts.Unix()
	return time.Unix(sec, nsec)
}
