// +build amd64

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

package sha1

// #include <stdint.h>
// #include <stdlib.h>
// void sha1_update_intel(int32_t *hash, const char* input, size_t num_blocks);
import "C"
import "unsafe"

func blockSSE3(dig *digest, p []byte) {
	C.sha1_update_intel((*C.int32_t)(unsafe.Pointer(&dig.h[0])), (*C.char)(unsafe.Pointer(&p[0])), (C.size_t)(len(p)/chunk))
}
