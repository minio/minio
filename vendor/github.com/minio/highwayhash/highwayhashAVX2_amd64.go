// Copyright (c) 2017 Minio Inc. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build go1.8
// +build amd64 !gccgo !appengine !nacl

package highwayhash

var (
	useSSE4 = supportsSSE4()
	useAVX2 = supportsAVX2()
	useNEON = false
)

//go:noescape
func supportsSSE4() bool

//go:noescape
func supportsAVX2() bool

//go:noescape
func initializeSSE4(state *[16]uint64, key []byte)

//go:noescape
func initializeAVX2(state *[16]uint64, key []byte)

//go:noescape
func updateSSE4(state *[16]uint64, msg []byte)

//go:noescape
func updateAVX2(state *[16]uint64, msg []byte)

//go:noescape
func finalizeSSE4(out []byte, state *[16]uint64)

//go:noescape
func finalizeAVX2(out []byte, state *[16]uint64)

func initialize(state *[16]uint64, key []byte) {
	if useAVX2 {
		initializeAVX2(state, key)
	} else if useSSE4 {
		initializeSSE4(state, key)
	} else {
		initializeGeneric(state, key)
	}
}

func update(state *[16]uint64, msg []byte) {
	if useAVX2 {
		updateAVX2(state, msg)
	} else if useSSE4 {
		updateSSE4(state, msg)
	} else {
		updateGeneric(state, msg)
	}
}

func finalize(out []byte, state *[16]uint64) {
	if useAVX2 {
		finalizeAVX2(out, state)
	} else if useSSE4 {
		finalizeSSE4(out, state)
	} else {
		finalizeGeneric(out, state)
	}
}
