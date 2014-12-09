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

// +build amd64

package cpu

// #include "cpu.h"
import "C"

func HasSSE41() bool {
	return int(C.has_sse41()) == 1
}

func HasAVX() bool {
	return int(C.has_avx()) == 1
}

func HasAVX2() bool {
	return int(C.has_avx2()) == 1
}
