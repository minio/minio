/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package crc32c

// #include <stdint.h>
// uint32_t crc32c_pcl(uint8_t *buf, int32_t len, uint32_t prev_crc);
import "C"
import (
	"unsafe"
)

func updateCastanagoliPCL(crc uint32, p []byte) uint32 {
	if len(p) == 0 {
		return 0
	}
	return uint32(C.crc32c_pcl((*C.uint8_t)(unsafe.Pointer(&p[0])), C.int32_t(len(p)), C.uint32_t(crc)))
}
