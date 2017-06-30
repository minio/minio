// Copyright (c) 2017 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// +build go1.7,amd64,!gccgo,!appengine

#define POLY1305_ADD(msg, h0, h1, h2) \
	ADDQ 0(msg), h0;  \
	ADCQ 8(msg), h1;  \
	ADCQ $1, h2;      \
	LEAQ 16(msg), msg

#define POLY1305_MUL(h0, h1, h2, r0, r1, t0, t1, t2, t3) \
	MOVQ  r0, AX;                  \
	MULQ  h0;                      \
	MOVQ  AX, t0;                  \
	MOVQ  DX, t1;                  \
	MOVQ  r0, AX;                  \
	MULQ  h1;                      \
	ADDQ  AX, t1;                  \
	ADCQ  $0, DX;                  \
	MOVQ  r0, t2;                  \
	IMULQ h2, t2;                  \
	ADDQ  DX, t2;                  \
	                               \
	MOVQ  r1, AX;                  \
	MULQ  h0;                      \
	ADDQ  AX, t1;                  \
	ADCQ  $0, DX;                  \
	MOVQ  DX, h0;                  \
	MOVQ  r1, t3;                  \
	IMULQ h2, t3;                  \
	MOVQ  r1, AX;                  \
	MULQ  h1;                      \
	ADDQ  AX, t2;                  \
	ADCQ  DX, t3;                  \
	ADDQ  h0, t2;                  \
	ADCQ  $0, t3;                  \
	                               \
	MOVQ  t0, h0;                  \
	MOVQ  t1, h1;                  \
	MOVQ  t2, h2;                  \
	ANDQ  $3, h2;                  \
	MOVQ  t2, t0;                  \
	ANDQ  $0XFFFFFFFFFFFFFFFC, t0; \
	ADDQ  t0, h0;                  \
	ADCQ  t3, h1;                  \
	ADCQ  $0, h2;                  \
	SHRQ  $2, t3, t2;              \
	SHRQ  $2, t3;                  \
	ADDQ  t2, h0;                  \
	ADCQ  t3, h1;                  \
	ADCQ  $0, h2

// Use BMI2 instruction MULXQ to reduce MOVQs
// Only available with go1.7 and the supporting CPU
#define POLY1305_MUL_BMI2(h0, h1, h2, r0, r1, t0, t1, t2, t3, t4) \
	MOVQ  r0, DX;                  \
	MULXQ h0, t0, t1;              \
	MULXQ h1, t3, t2;              \
	IMULQ h2, DX;                  \
	ADDQ  t3, t1;                  \
	ADCQ  DX, t2;                  \
	                               \
	MOVQ  r1, DX;                  \
	MULXQ h0, t3, t4;              \
	ADDQ  t3, t1;                  \
	ADCQ  t4, t2;                  \
	MULXQ h1, t3, t4;              \
	IMULQ h2, DX;                  \
	ADDQ  t3, t2;                  \
	ADCQ  t4, DX;                  \
	ADCQ  $0, DX;                  \
	                               \
	MOVQ  t0, h0;                  \
	MOVQ  t1, h1;                  \
	MOVQ  t2, h2;                  \
	ANDQ  $3, h2;                  \
	MOVQ  t2, t0;                  \
	ANDQ  $0XFFFFFFFFFFFFFFFC, t0; \
	ADDQ  t0, h0;                  \
	ADCQ  DX, h1;                  \
	ADCQ  $0, h2;                  \
	SHRQ  $2, DX, t2;              \
	SHRQ  $2, DX;                  \
	ADDQ  t2, h0;                  \
	ADCQ  DX, h1;                  \
	ADCQ  $0, h2

// func update(state *[7]uint64, msg []byte)
TEXT ·update(SB), $0-32
	MOVQ state+0(FP), DI
	MOVQ msg_base+8(FP), SI
	MOVQ msg_len+16(FP), R15

	MOVQ 0(DI), R8   // h0
	MOVQ 8(DI), R9   // h1
	MOVQ 16(DI), R10 // h2
	MOVQ 24(DI), R11 // r0
	MOVQ 32(DI), R12 // h1

	MOVQ ·useBMI2(SB), AX
	CMPQ AX, $1
	JE   BMI2

	CMPQ R15, $16
	JB   BYTES_BETWEEN_0_AND_15

LOOP:
	POLY1305_ADD(SI, R8, R9, R10)

MULTIPLY:
	POLY1305_MUL(R8, R9, R10, R11, R12, BX, CX, R13, R14)
	SUBQ $16, R15
	CMPQ R15, $16
	JAE  LOOP

BYTES_BETWEEN_0_AND_15:
	TESTQ R15, R15
	JZ    DONE
	MOVQ  $1, BX
	XORQ  CX, CX
	XORQ  R13, R13
	ADDQ  R15, SI

FLUSH_BUFFER:
	SHLQ $8, BX, CX
	SHLQ $8, BX
	MOVB -1(SI), R13
	XORQ R13, BX
	DECQ SI
	DECQ R15
	JNZ  FLUSH_BUFFER

	ADDQ BX, R8
	ADCQ CX, R9
	ADCQ $0, R10
	MOVQ $16, R15
	JMP  MULTIPLY

BMI2:
	CMPQ R15, $16
	JB   BYTES_BETWEEN_0_AND_15_BMI2

LOOP_BMI2:
	POLY1305_ADD(SI, R8, R9, R10)

MULTIPLY_BMI2:
	POLY1305_MUL_BMI2(R8, R9, R10, R11, R12, BX, CX, R13, R14, BP)
	SUBQ $16, R15
	CMPQ R15, $16
	JAE  LOOP_BMI2

BYTES_BETWEEN_0_AND_15_BMI2:
	TESTQ R15, R15
	JZ    DONE
	MOVQ  $1, BX
	XORQ  CX, CX
	XORQ  R13, R13
	ADDQ  R15, SI

FLUSH_BUFFER_BMI2:
	SHLQ $8, BX, CX
	SHLQ $8, BX
	MOVB -1(SI), R13
	XORQ R13, BX
	DECQ SI
	DECQ R15
	JNZ  FLUSH_BUFFER_BMI2

	ADDQ BX, R8
	ADCQ CX, R9
	ADCQ $0, R10
	MOVQ $16, R15
	JMP  MULTIPLY_BMI2

DONE:
	MOVQ R8, 0(DI)
	MOVQ R9, 8(DI)
	MOVQ R10, 16(DI)
	RET

// func supportsBMI2() bool
TEXT ·supportsBMI2(SB), 4, $0-1
	MOVQ runtime·support_avx2(SB), BX
	CMPQ BX, $1
	JNE  NO_AVX2

	MOVL $7, AX
	XORQ CX, CX
	CPUID
	SHRL $8, BX
	ANDL $1, BX

NO_AVX2:
	MOVB BX, ret+0(FP)
	RET
