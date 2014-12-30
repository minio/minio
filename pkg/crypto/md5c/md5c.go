// !build linux,amd64

package md5c

// #include "md5.h"
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
