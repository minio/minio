// Copyright (c) 2017 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build arm, !gccgo, !appengine

#include "textflag.h"

#define R10 g
#define ROUND(v0, v1, v2, v3, v4, v5, v6, v7, v8, t) \
	ADD.S v2, v0, v0;     \
	ADC   v3, v1, v1;     \
	EOR   v2<<13, v0, v8; \
	EOR   v2>>19, v1, t;  \
	EOR   v3>>19, v8, v8; \
	EOR   v3<<13, t, t;   \
	ADD.S v6, v4, v4;     \
	ADC   v7, v5, v5;     \
	EOR   v6<<16, v4, v2; \
	EOR   v6>>16, v5, v3; \
	EOR   v7>>16, v2, v2; \
	EOR   v7<<16, v3, v3; \
	ADD.S v2, v1, v1;     \
	ADC   v3, v0, v0;     \
	EOR   v2<<21, v1, v6; \
	EOR   v2>>11, v0, v7; \
	EOR   v3>>11, v6, v6; \
	EOR   v3<<21, v7, v7; \
	ADD.S v8, v4, v4;     \
	ADC   t, v5, v5;      \
	EOR   v8<<17, v4, v2; \
	EOR   v8>>15, v5, v3; \
	EOR   t>>15, v2, v2;  \
	EOR   t<<17, v3, v3

// core(hVal *[4]uint64, msg []uint8)
TEXT ·core(SB), NOSPLIT, $8-16
	MOVW    R10, sav-8(SP)
	MOVW    hVal+0(FP), R8
	MOVW    msg+4(FP), R10
	MOVW    msg_len+8(FP), R11

	MOVM.IA (R8), [R0-R7]
	ADD     R10, R11, R11
	MOVW    R11, end-4(SP)
	AND.S   $3, R10, R8
	BNE     loop_unaligned

loop_aligned:
	MOVM.IA.W (R10), [R12, R14]
	EOR       R12, R6, R6
	EOR       R14, R7, R7
	ROUND(R0, R1, R2, R3, R4, R5, R6, R7, R8, R11)
	ROUND(R1, R0, R2, R3, R5, R4, R6, R7, R8, R11)
	EOR       R12, R0, R0
	EOR       R14, R1, R1
	MOVW      end-4(SP), R11
	CMP       R11, R10
	BLO       loop_aligned

	MOVW      hVal+0(FP), R8
	MOVM.IA   [R0-R7], (R8)
	MOVW      sav-8(SP), R10
	RET

loop_unaligned:
	MOVB    (R10), R12
	MOVB    1(R10), R11
	ORR     R11<<8, R12, R12
	MOVB    2(R10), R11
	ORR     R11<<16, R12, R12
	MOVB    3(R10), R11
	ORR     R11<<24, R12, R12
	MOVB    4(R10), R14
	MOVB    5(R10), R11
	ORR     R11<<8, R14, R14
	MOVB    6(R10), R11
	ORR     R11<<16, R14, R14
	MOVB    7(R10), R11
	ORR     R11<<24, R14, R14
	ADD     $8, R10, R10

	EOR     R12, R6, R6
	EOR     R14, R7, R7
	ROUND(R0, R1, R2, R3, R4, R5, R6, R7, R8, R11)
	ROUND(R1, R0, R2, R3, R5, R4, R6, R7, R8, R11)
	EOR     R12, R0, R0
	EOR     R14, R1, R1
	MOVW    end-4(SP), R11
	CMP     R11, R10
	BLO     loop_unaligned

	MOVW    hVal+0(FP), R8
	MOVM.IA [R0-R7], (R8)
	MOVW    sav-8(SP), R10
	RET
