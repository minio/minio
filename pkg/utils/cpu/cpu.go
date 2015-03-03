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

package cpu

// int has_sse41 (void);
// int has_avx (void);
// int has_avx2 (void);
import "C"

// CPUID instruction verification wrapper for SSE41 extensions
func HasSSE41() bool {
	return int(C.has_sse41()) == 1
}

// CPUID instruction verification wrapper for AVX extensions
func HasAVX() bool {
	return int(C.has_avx()) == 1
}

// CPUID instruction verification wrapper for AVX2 extensions
func HasAVX2() bool {
	return int(C.has_avx2()) == 1
}
