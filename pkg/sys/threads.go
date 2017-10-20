// +build linux

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

import (
	"io/ioutil"
	"strconv"
	"strings"
)

// GetMaxThreads returns the maximum number of threads that the system can create.
func GetMaxThreads() (int, error) {
	sysMaxThreadsStr, err := ioutil.ReadFile("/proc/sys/kernel/threads-max")
	if err != nil {
		return 0, err
	}
	sysMaxThreads, err := strconv.Atoi(strings.TrimSpace(string(sysMaxThreadsStr)))
	if err != nil {
		return 0, err
	}
	return sysMaxThreads, nil
}
