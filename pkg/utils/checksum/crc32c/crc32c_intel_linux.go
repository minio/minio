// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Golang project:
//    https://github.com/golang/go/blob/master/LICENSE

// Using this part of Minio codebase under the license
// Apache License Version 2.0 with modifications

// Package crc32 implements the 32-bit cyclic redundancy check, or CRC-32,
// checksum. See http://en.wikipedia.org/wiki/Cyclic_redundancy_check for
// information.

package crc32c

// #include <stdint.h>
// uint32_t crc32c_pcl(uint8_t *buf, int32_t len, uint32_t prev_crc);
import "C"
import (
	"unsafe"
)

func updateCastanagoliPCL(crc uint32, p []byte) uint32 {
	if len(p) == 0 {
		return 0
	}
	return uint32(C.crc32c_pcl((*C.uint8_t)(unsafe.Pointer(&p[0])), C.int32_t(len(p)), C.uint32_t(crc)))
}
