// !build linux,amd64

package md5c

// /* Any 32-bit or wider unsigned integer data type will do */
// typedef unsigned int MD5_u32plus;
//
// typedef struct {
//   	  MD5_u32plus lo, hi;
//	      MD5_u32plus a, b, c, d;
//        unsigned char buffer[64];
//        MD5_u32plus block[16];
// } MD5_CTX;
//
// void MD5_Init(MD5_CTX *ctx);
// void MD5_Update(MD5_CTX *ctx, const void *data, unsigned long size);
// void MD5_Final(unsigned char *result, MD5_CTX *ctx);
import "C"
import (
	"io"
	"unsafe"
)

func context() *C.MD5_CTX {
	var ctx C.MD5_CTX
	C.MD5_Init(&ctx)
	return &ctx
}

func write(buffer []byte, ctx *C.MD5_CTX) {
	size := len(buffer)
	data := unsafe.Pointer(&buffer[0])
	C.MD5_Update(ctx, data, C.ulong(size))
}

func Sum(reader io.Reader) ([]byte, error) {
	ctx := context()
	var err error
	var length int
	for err == nil {
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		// break here since byteBuffer will go out of range
		// when invoking subsequent write() call
		if length == 0 {
			break
		}
		byteBuffer = byteBuffer[0:length]
		write(byteBuffer, ctx)
	}

	if err != io.EOF {
		return nil, err
	}

	outputBuffer := make([]byte, 16)
	coutputbuff := (*C.uchar)(unsafe.Pointer(&outputBuffer[0]))
	C.MD5_Final(coutputbuff, ctx)
	return outputBuffer, nil
}
