// Copyright (c) 2017 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build 386, !gccgo, !appengine

#define ROTL(n, t, v) \
 	MOVO v, t; \
	PSLLQ $n, t; \
	PSRLQ $(64-n), v; \
	PXOR t, v

#define ROUND(v0, v1, v2, v3, t0, t1) \
    PADDQ v1, v0; \
    PADDQ v3, v2;  \
    ROTL(13, t0, v1); \
    ROTL(16, t1, v3); \
    PXOR v0, v1; \
    PXOR v2, v3; \
    PSHUFD $0xE1, v0, v0; \
    PADDQ v1, v2; \
    PADDQ v3, v0;  \
    ROTL(17, t0, v1); \
    ROTL(21, t1, v3); \
    PXOR v2, v1; \
    PXOR v0, v3; \
    PSHUFD $0xE1, v2, v2

// coreSSE2(hVal *[4]uint64, msg []byte)
TEXT ·coreSSE2(SB), 4, $0-16
	MOVL hVal+0(FP), AX
	MOVL msg_base+4(FP), SI
	MOVL msg_len+8(FP), BX
	MOVQ 0(AX), X0
	MOVQ 8(AX), X1
	MOVQ 16(AX), X2
	MOVQ 24(AX), X3
    PXOR X6, X6
	ANDL $-8, BX

loop:
	MOVQ 0(SI), X6
	PXOR X6, X3
	ROUND(X0, X1, X2, X3, X4, X5)
	ROUND(X0, X1, X2, X3, X4, X5)
	PXOR X6, X0

	LEAL 8(SI), SI
	SUBL $8, BX
	JNZ  loop

	MOVQ X0, 0(AX)
	MOVQ X1, 8(AX)
	MOVQ X2, 16(AX)
	MOVQ X3, 24(AX)
	RET

// func supportsSSE2() bool
TEXT ·supportsSSE2(SB), 4, $0-1
	MOVL $1, AX
	CPUID
	SHRL $26, DX
	ANDL $1, DX        // DX != 0 if support SSE2
	MOVB DX, ret+0(FP)
	RET
