// Copyright (c) 2017 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build 386, !gccgo, !appengine

package siphash

var useSSE2 = supportsSSE2()

//go:noescape
func supportsSSE2() bool

//go:noescape
func coreSSE2(hVal *[4]uint64, msg []byte)

func core(hVal *[4]uint64, msg []byte) {
	if useSSE2 {
		coreSSE2(hVal, msg)
	} else {
		genericCore(hVal, msg)
	}
}

func finalize64(hVal *[4]uint64, block *[BlockSize]byte) uint64 {
	return genericFinalize64(hVal, block)
}

func finalize128(tag *[16]byte, hVal *[4]uint64, block *[BlockSize]byte) {
	genericFinalize128(tag, hVal, block)
}
