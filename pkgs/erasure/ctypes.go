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

package erasure

import "C"
import (
	"fmt"
	"unsafe"
)

// Integer to Int conversion
func int2cInt(src_err_list []int) *C.int {
	var SrcErrInt = int(unsafe.Sizeof(src_err_list[0]))
	switch SizeInt {
	case SrcErrInt:
		return (*C.int)(unsafe.Pointer(&src_err_list[0]))
	case SizeInt8:
		Int8Array := make([]int8, len(src_err_list))
		for i, v := range src_err_list {
			Int8Array[i] = int8(v)
		}
		return (*C.int)(unsafe.Pointer(&Int8Array[0]))
	case SizeInt16:
		Int16Array := make([]int16, len(src_err_list))
		for i, v := range src_err_list {
			Int16Array[i] = int16(v)
		}
		return (*C.int)(unsafe.Pointer(&Int16Array[0]))
	case SizeInt32:
		Int32Array := make([]int32, len(src_err_list))
		for i, v := range src_err_list {
			Int32Array[i] = int32(v)
		}
		return (*C.int)(unsafe.Pointer(&Int32Array[0]))
	case SizeInt64:
		Int64Array := make([]int64, len(src_err_list))
		for i, v := range src_err_list {
			Int64Array[i] = int64(v)
		}
		return (*C.int)(unsafe.Pointer(&Int64Array[0]))
	default:
		panic(fmt.Sprintf("Unsupported: %d", SizeInt))
	}
}
