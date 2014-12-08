// +build amd64

package cpu

// #include "cpu.h"
import "C"

func HasSSE41() bool {
	return int(C.has_sse41()) == 1
}

func HasAVX() bool {
	return int(C.has_avx()) == 1
}

func HasAVX2() bool {
	return int(C.has_avx2()) == 1
}
