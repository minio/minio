// +build amd64

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
