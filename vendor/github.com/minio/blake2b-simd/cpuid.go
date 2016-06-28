// +build 386,!gccgo amd64,!gccgo

// Copyright 2016 Frank Wessels <fwessels@xs4all.nl>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package blake2b

func cpuid(op uint32) (eax, ebx, ecx, edx uint32)
func xgetbv(index uint32) (eax, edx uint32)

// True when SIMD instructions are available.
var avx = haveAVX()

// haveSSE returns true if we have streaming SIMD instructions.
func haveAVX() bool {
	_, _, c, _ := cpuid(1)

	// Check XGETBV, OXSAVE and AVX bits
	if c&(1<<26) != 0 && c&(1<<27) != 0 && c&(1<<28) != 0 {
		// Check for OS support
		eax, _ := xgetbv(0)
		return (eax & 0x6) == 0x6
	}
	return false
}
