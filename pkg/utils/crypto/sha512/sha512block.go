// +build amd64

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
