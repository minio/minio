/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

import "fmt"

// Flags cpu flags
type Flags uint64

//
const (
	SSE3  = iota // Prescott SSE3 functions
	SSE42        // Nehalem SSE4.2 functions
	AVX          // AVX functions
	AVX2         // AVX2 functions
)

// cpu get CPU flags
func cpu() Flags {
	rval := Flags(0)
	a, _, _, _ := cpuid(0)
	_, _, c, _ := cpuid(1)
	if (c & 1) != 0 {
		rval |= SSE3
	}
	if (c & 0x00100000) != 0 {
		rval |= SSE42
	}
	// Check OXSAVE and AVX bits
	if (c & 0x18000000) == 0x18000000 {
		// Check for OS support
		eax, _ := xgetbv(0)
		if (eax & 0x6) == 0x6 {
			rval |= AVX
		}
	}
	fmt.Println(a)
	if a >= 7 {
		_, ebx, _, _ := cpuidex(7, 0)
		if (rval&AVX) != 0 && (ebx&0x00000020) != 0 {
			rval |= AVX2
		}
	}
	return rval
}

// HasSSE3 check if sse3 available
func HasSSE3() bool {
	return cpu()&SSE3 != 0
}

// HasSSE42 check if sse4.2 available
func HasSSE42() bool {
	return cpu()&SSE42 != 0
}

// HasAVX check if avx available
func HasAVX() bool {
	return cpu()&AVX != 0
}

// HasAVX2 check if avx2 available
func HasAVX2() bool {
	return cpu()&AVX2 != 0
}
