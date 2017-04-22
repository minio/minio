// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build !amd64,!386,!arm gccgo appengine nacl

package siphash

func core(hVal *[4]uint64, msg []byte) {
	genericCore(hVal, msg)
}

func finalize64(hVal *[4]uint64, block *[BlockSize]byte) uint64 {
	return genericFinalize64(hVal, block)
}

func finalize128(tag *[16]byte, hVal *[4]uint64, block *[BlockSize]byte) {
	genericFinalize128(tag, hVal, block)
}
