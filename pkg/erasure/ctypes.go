/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

package erasure

// #include <stdint.h>
import "C"
import (
	"fmt"
	"unsafe"
)

// intSlice2CIntArray converts Go int slice to C int array
func intSlice2CIntArray(srcErrList []int) *C.int32_t {
	if len(srcErrList) == 0 {
		return (*C.int32_t)(unsafe.Pointer(nil))
	}
	var sizeErrInt = int(unsafe.Sizeof(srcErrList[0]))
	switch sizeInt {
	case sizeErrInt:
		return (*C.int32_t)(unsafe.Pointer(&srcErrList[0]))
	case sizeInt8:
		int8Array := make([]int8, len(srcErrList))
		for i, v := range srcErrList {
			int8Array[i] = int8(v)
		}
		return (*C.int32_t)(unsafe.Pointer(&int8Array[0]))
	case sizeInt16:
		int16Array := make([]int16, len(srcErrList))
		for i, v := range srcErrList {
			int16Array[i] = int16(v)
		}
		return (*C.int32_t)(unsafe.Pointer(&int16Array[0]))
	case sizeInt32:
		int32Array := make([]int32, len(srcErrList))
		for i, v := range srcErrList {
			int32Array[i] = int32(v)
		}
		return (*C.int32_t)(unsafe.Pointer(&int32Array[0]))
	case sizeInt64:
		int64Array := make([]int64, len(srcErrList))
		for i, v := range srcErrList {
			int64Array[i] = int64(v)
		}
		return (*C.int32_t)(unsafe.Pointer(&int64Array[0]))
	default:
		panic(fmt.Sprintf("Unsupported: %d", sizeInt))
	}
}
