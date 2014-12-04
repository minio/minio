// +build amd64

package cpu

// #include "cpu.h"
import "C"

func HasSSE41() int {
	return int(C.has_sse41())
}

func HasAVX() int {
	return int(C.has_avx())
}

func HasAVX2() int {
	return int(C.has_avx2())
}
