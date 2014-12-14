// +build amd64

package sha512

// #include <stdint.h>
// void sha512_transform_avx(const void* M, void* D, uint64_t L);
// void sha512_transform_ssse3(const void* M, void* D, uint64_t L);
// void sha512_transform_rorx(const void* M, void* D, uint64_t L);
import "C"
