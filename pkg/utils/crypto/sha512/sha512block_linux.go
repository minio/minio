// +build amd64

//
// Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package sha512

// #cgo CFLAGS: -DHAS_SSE41 -DHAS_AVX -DHAS_AVX2
// #include <stdint.h>
// void sha512_transform_ssse3 (const void* M, void* D, uint64_t L);
// void sha512_transform_avx (const void* M, void* D, uint64_t L);
// void sha512_transform_rorx (const void* M, void* D, uint64_t L);
import "C"
import "unsafe"

func blockSSE(dig *digest, p []byte) {
	C.sha512_transform_ssse3(unsafe.Pointer(&p[0]), unsafe.Pointer(&dig.h[0]), (C.uint64_t)(len(p)/chunk))
}

func blockAVX(dig *digest, p []byte) {
	C.sha512_transform_avx(unsafe.Pointer(&p[0]), unsafe.Pointer(&dig.h[0]), (C.uint64_t)(len(p)/chunk))
}

func blockAVX2(dig *digest, p []byte) {
	C.sha512_transform_rorx(unsafe.Pointer(&p[0]), unsafe.Pointer(&dig.h[0]), (C.uint64_t)(len(p)/chunk))
}
