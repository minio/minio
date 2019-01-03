/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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

package mem

import (
	"fmt"
)

const (
	BYTES     = 1
	KILOBYTES = BYTES << 10
	MEGABYTES = KILOBYTES << 10
	GIGABYTES = MEGABYTES << 10
	TERABYTES = GIGABYTES << 10
)

func fmtBytes(bytes uint64) string {
	unit := ""
	value := float64(0)
	if bytes >= TERABYTES {
		unit = "T"
		value = float64(bytes) / float64(TERABYTES)
	} else if bytes >= GIGABYTES {
		unit = "G"
		value = float64(bytes) / float64(GIGABYTES)
	} else if bytes >= MEGABYTES {
		unit = "M"
		value = float64(bytes) / float64(MEGABYTES)
	} else if bytes >= KILOBYTES {
		unit = "K"
		value = float64(bytes) / float64(KILOBYTES)
	}
	return fmt.Sprintf("%.2f%s", value, unit)
}
