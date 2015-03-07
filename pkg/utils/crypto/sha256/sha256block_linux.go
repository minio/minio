// +build amd64

//
// Mini Object Storage, (C) 2015 Minio, Inc.
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

package sha256

// #cgo CFLAGS: -DHAS_SSE41 -DHAS_AVX -DHAS_AVX2
// #include <stdint.h>
// void sha256_transform_ssse3 (const char *input_data, uint32_t *digest, unsigned long num_blks);
// void sha256_transform_avx (const char *input_data, uint32_t *digest, unsigned long num_blks);
// void sha256_transform_rorx (const char *input_data, uint32_t *digest, unsigned long num_blks);
import "C"
import "unsafe"

func blockSSE(dig *digest, p []byte) {
	C.sha256_transform_ssse3((*C.char)(unsafe.Pointer(&p[0])), (*C.uint32_t)(unsafe.Pointer(&dig.h[0])), (C.ulong)(len(p)/64))
}

func blockAVX(dig *digest, p []byte) {
	C.sha256_transform_avx((*C.char)(unsafe.Pointer(&p[0])), (*C.uint32_t)(unsafe.Pointer(&dig.h[0])), (C.ulong)(len(p)/64))
}

func blockAVX2(dig *digest, p []byte) {
	C.sha256_transform_rorx((*C.char)(unsafe.Pointer(&p[0])), (*C.uint32_t)(unsafe.Pointer(&dig.h[0])), (C.ulong)(len(p)/64))
}
