/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

// cpuid, cpuidex
func cpuid(op uint32) (eax, ebx, ecx, edx uint32)
func cpuidex(op, op2 uint32) (eax, ebx, ecx, edx uint32)

// HasSSE41 - CPUID instruction verification wrapper for SSE41 extensions
func HasSSE41() bool {
	_, _, c, _ := cpuid(1)
	return ((c & (1 << 19)) != 0)
}

// HasAVX - CPUID instruction verification wrapper for AVX extensions
func HasAVX() bool {
	_, _, c, _ := cpuid(1)
	return ((c & (1 << 28)) != 0)
}

// HasAVX2 - CPUID instruction verification wrapper for AVX2 extensions
func HasAVX2() bool {
	_, b, _, _ := cpuidex(7, 0)
	return ((b & (1 << 5)) != 0)
}
