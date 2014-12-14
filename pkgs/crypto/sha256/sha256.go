// +build amd64

package sha256

// #include <stdint.h>
// void sha256_transform_avx (uint8_t *input_data, uint32_t digest[8], uint64_t num_blks);
// void sha256_transform_ssse3 (uint8_t *input_data, uint32_t digest[8], uint64_t num_blks);
// void sha256_transform_rorx (uint8_t *input_data, uint32_t digest[8], uint64_t num_blks);
// #define SHA256_DIGEST_SIZE      32
// #define SHA256_BLOCK_SIZE       64
// #define SHA256_H0       0x6a09e667UL
// #define SHA256_H1       0xbb67ae85UL
// #define SHA256_H2       0x3c6ef372UL
// #define SHA256_H3       0xa54ff53aUL
// #define SHA256_H4       0x510e527fUL
// #define SHA256_H5       0x9b05688cUL
// #define SHA256_H6       0x1f83d9abUL
// #define SHA256_H7       0x5be0cd19UL
import "C"

/*
func Sha256(buffer []byte) ([]uint32, error) {

	if cpu.HasSSE41() {
		C.sha256_transform_ssse3()
		return 0, nil
	}

	if cpu.HasAVX() {
		C.sha256_transform_avx()
		return 0, nil
	}

	if cpu.HasAVX2() {
		C.sha256_transform_rorx()
		return 0, nil
	}
}
*/
