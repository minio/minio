// +build amd64

package sha512

// #include <stdint.h>
// void sha512_transform_avx(const void* M, void* D, uint64_t L);
// void sha512_transform_ssse3(const void* M, void* D, uint64_t L);
// void sha512_transform_rorx(const void* M, void* D, uint64_t L);
import "C"
import (
	gosha512 "crypto/sha512"
	"io"
)

func Sum(reader io.Reader) ([]byte, error) {
	hash := gosha512.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		hash.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return hash.Sum(nil), nil
}
