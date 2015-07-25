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

//
// int sizeInt()
// {
//      return sizeof(int);
// }
import "C"
import "unsafe"

var (
	// See http://golang.org/ref/spec#Numeric_types
	sizeInt = int(C.sizeInt())
	// SizeInt8  is the byte size of a int8.
	sizeInt8 = int(unsafe.Sizeof(int8(0)))
	// SizeInt16 is the byte size of a int16.
	sizeInt16 = int(unsafe.Sizeof(int16(0)))
	// SizeInt32 is the byte size of a int32.
	sizeInt32 = int(unsafe.Sizeof(int32(0)))
	// SizeInt64 is the byte size of a int64.
	sizeInt64 = int(unsafe.Sizeof(int64(0)))
)
