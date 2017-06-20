// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build amd64, !gccgo, !appengine

#include "textflag.h"

DATA ·poly1305Mask<>+0x00(SB)/8, $0x0FFFFFFC0FFFFFFF
DATA ·poly1305Mask<>+0x08(SB)/8, $0x0FFFFFFC0FFFFFFC
GLOBL ·poly1305Mask<>(SB), RODATA, $16

// func initialize(state *[7]uint64, key *[32]byte)
TEXT ·initialize(SB), $0-16
	MOVQ state+0(FP), DI
	MOVQ key+8(FP), SI


	// state[0...7] is initialized with zero
	MOVOU 0(SI), X0
	MOVOU 16(SI), X1
	MOVOU ·poly1305Mask<>(SB), X2
	PAND  X2, X0 
	MOVOU X0, 24(DI)
	MOVOU X1, 40(DI)
	RET

// func finalize(tag *[TagSize]byte, state *[7]uint64)
TEXT ·finalize(SB), $0-16
	MOVQ tag+0(FP), DI
	MOVQ state+8(FP), SI

	MOVQ    0(SI), AX
	MOVQ    8(SI), BX
	MOVQ    16(SI), CX
	MOVQ    AX, R8
	MOVQ    BX, R9
	SUBQ    $0XFFFFFFFFFFFFFFFB, AX
	SBBQ    $0XFFFFFFFFFFFFFFFF, BX
	SBBQ    $3, CX
	CMOVQCS R8, AX
	CMOVQCS R9, BX
	ADDQ    40(SI), AX
	ADCQ    48(SI), BX

	MOVQ AX, 0(DI)
	MOVQ BX, 8(DI)
	RET
