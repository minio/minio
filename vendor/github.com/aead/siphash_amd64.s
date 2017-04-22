// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build amd64, !gccgo, !appengine

#define ROUND(v0, v1, v2, v3) \
	ADDQ v1, v0;  \
	ADDQ v3, v2;  \
	ROLQ $13, v1; \
	ROLQ $16, v3; \
	XORQ v0, v1;  \
	XORQ v2, v3;  \
	ROLQ $32, v0; \
	ADDQ v1, v2;  \
	ADDQ v3, v0;  \
	ROLQ $17, v1; \
	ROLQ $21, v3; \
	XORQ v2, v1;  \
	XORQ v0, v3;  \
	ROLQ $32, v2

// core(hVal *[4]uint64, msg []byte)
TEXT ·core(SB), 4, $0-32
	MOVQ hVal+0(FP), AX
	MOVQ msg_base+8(FP), SI
	MOVQ msg_len+16(FP), BX
	MOVQ 0(AX), R9
	MOVQ 8(AX), R10
	MOVQ 16(AX), R11
	MOVQ 24(AX), R12
	ANDQ $-8, BX

loop:
	MOVQ 0(SI), DX
	XORQ DX, R12
	ROUND(R9, R10, R11, R12)
	ROUND(R9, R10, R11, R12)
	XORQ DX, R9

	LEAQ 8(SI), SI
	SUBQ $8, BX
	JNZ  loop

	MOVQ R9, 0(AX)
	MOVQ R10, 8(AX)
	MOVQ R11, 16(AX)
	MOVQ R12, 24(AX)
	RET
