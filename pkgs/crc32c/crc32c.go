// +build linux,amd64

package crc32c

// #include "crc32c.h"
import "C"
import (
	"errors"
	"unsafe"
)

func Crc32c(buffer []byte) (uint32, error) {
	var length = len(buffer)
	if length == 0 {
		return 0, errors.New("Invalid input")
	}

	var cbuf *C.uint8_t
	cbuf = (*C.uint8_t)(unsafe.Pointer(&buffer[0]))
	crc := C.crc32c_pcl(cbuf, C.int32_t(length), C.uint32_t(0))

	return uint32(crc), nil
}
