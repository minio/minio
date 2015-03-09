// +build amd64

package sha1

// #cgo CFLAGS: -DHAS_AVX2
// #include <stdint.h>
// #include <stdlib.h>
// void sha1_transform(int32_t *hash, const char* input, size_t num_blocks);
// void sha1_update_intel(int32_t *hash, const char* input, size_t num_blocks);
import "C"
import "unsafe"

func blockAVX2(dig *digest, p []byte) {
	C.sha1_transform((*C.int32_t)(unsafe.Pointer(&dig.h[0])), (*C.char)(unsafe.Pointer(&p[0])), (C.size_t)(len(p)/chunk))
}

func blockSSE3(dig *digest, p []byte) {
	C.sha1_update_intel((*C.int32_t)(unsafe.Pointer(&dig.h[0])), (*C.char)(unsafe.Pointer(&p[0])), (C.size_t)(len(p)/chunk))
}
