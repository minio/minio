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
	"errors"
	"unsafe"
)

func Crc32c(buffer []byte) (uint32, error) {
	var length = len(buffer)
	if length == 0 {
		return 0, errors.New("Invalid input")
	}

	var cbuf *C.uint8_t
	cbuf = (*C.uint8_t)(unsafe.Pointer(&buffer[0]))
	crc := C.crc32c_pcl(cbuf, C.int32_t(length), C.uint32_t(0))

	return uint32(crc), nil
}
