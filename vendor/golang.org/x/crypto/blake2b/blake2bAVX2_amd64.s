// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"

DATA ·AVX2_iv0<>+0x00(SB)/8, $0x6a09e667f3bcc908
DATA ·AVX2_iv0<>+0x08(SB)/8, $0xbb67ae8584caa73b
DATA ·AVX2_iv0<>+0x10(SB)/8, $0x3c6ef372fe94f82b
DATA ·AVX2_iv0<>+0x18(SB)/8, $0xa54ff53a5f1d36f1
GLOBL ·AVX2_iv0<>(SB), (NOPTR+RODATA), $32

DATA ·AVX2_iv1<>+0x00(SB)/8, $0x510e527fade682d1
DATA ·AVX2_iv1<>+0x08(SB)/8, $0x9b05688c2b3e6c1f
DATA ·AVX2_iv1<>+0x10(SB)/8, $0x1f83d9abfb41bd6b
DATA ·AVX2_iv1<>+0x18(SB)/8, $0x5be0cd19137e2179
GLOBL ·AVX2_iv1<>(SB), (NOPTR+RODATA), $32

DATA ·AVX2_c40<>+0x00(SB)/8, $0x0201000706050403
DATA ·AVX2_c40<>+0x08(SB)/8, $0x0a09080f0e0d0c0b
DATA ·AVX2_c40<>+0x10(SB)/8, $0x0201000706050403
DATA ·AVX2_c40<>+0x18(SB)/8, $0x0a09080f0e0d0c0b
GLOBL ·AVX2_c40<>(SB), (NOPTR+RODATA), $32

DATA ·AVX2_c48<>+0x00(SB)/8, $0x0100070605040302
DATA ·AVX2_c48<>+0x08(SB)/8, $0x09080f0e0d0c0b0a
DATA ·AVX2_c48<>+0x10(SB)/8, $0x0100070605040302
DATA ·AVX2_c48<>+0x18(SB)/8, $0x09080f0e0d0c0b0a
GLOBL ·AVX2_c48<>(SB), (NOPTR+RODATA), $32

DATA ·AVX_iv0<>+0x00(SB)/8, $0x6a09e667f3bcc908
DATA ·AVX_iv0<>+0x08(SB)/8, $0xbb67ae8584caa73b
GLOBL ·AVX_iv0<>(SB), (NOPTR+RODATA), $16

DATA ·AVX_iv1<>+0x00(SB)/8, $0x3c6ef372fe94f82b
DATA ·AVX_iv1<>+0x08(SB)/8, $0xa54ff53a5f1d36f1
GLOBL ·AVX_iv1<>(SB), (NOPTR+RODATA), $16

DATA ·AVX_iv2<>+0x00(SB)/8, $0x510e527fade682d1
DATA ·AVX_iv2<>+0x08(SB)/8, $0x9b05688c2b3e6c1f
GLOBL ·AVX_iv2<>(SB), (NOPTR+RODATA), $16

DATA ·AVX_iv3<>+0x00(SB)/8, $0x1f83d9abfb41bd6b
DATA ·AVX_iv3<>+0x08(SB)/8, $0x5be0cd19137e2179
GLOBL ·AVX_iv3<>(SB), (NOPTR+RODATA), $16

DATA ·AVX_c40<>+0x00(SB)/8, $0x0201000706050403
DATA ·AVX_c40<>+0x08(SB)/8, $0x0a09080f0e0d0c0b
GLOBL ·AVX_c40<>(SB), (NOPTR+RODATA), $16

DATA ·AVX_c48<>+0x00(SB)/8, $0x0100070605040302
DATA ·AVX_c48<>+0x08(SB)/8, $0x09080f0e0d0c0b0a
GLOBL ·AVX_c48<>(SB), (NOPTR+RODATA), $16

// unfortunately the BYTE representation of VPERMQ must be used
#define ROUND_AVX2(m0, m1, m2, m3, t, c40, c48) \
	VPADDQ  m0, Y0, Y0;                                                       \
	VPADDQ  Y1, Y0, Y0;                                                       \
	VPXOR   Y0, Y3, Y3;                                                       \
	VPSHUFD $-79, Y3, Y3;                                                     \
	VPADDQ  Y3, Y2, Y2;                                                       \
	VPXOR   Y2, Y1, Y1;                                                       \
	VPSHUFB c40, Y1, Y1;                                                      \
	VPADDQ  m1, Y0, Y0;                                                       \
	VPADDQ  Y1, Y0, Y0;                                                       \
	VPXOR   Y0, Y3, Y3;                                                       \
	VPSHUFB c48, Y3, Y3;                                                      \
	VPADDQ  Y3, Y2, Y2;                                                       \
	VPXOR   Y2, Y1, Y1;                                                       \
	VPADDQ  Y1, Y1, t;                                                        \
	VPSRLQ  $63, Y1, Y1;                                                      \
	VPXOR   t, Y1, Y1;                                                        \
	BYTE    $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xc9; BYTE $0x39 \ // VPERMQ 0x39, Y1, Y1
	BYTE    $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xd2; BYTE $0x4e \ // VPERMQ 0x4e, Y2, Y2
	BYTE    $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xdb; BYTE $0x93 \ // VPERMQ 0x93, Y3, Y3
	VPADDQ  m2, Y0, Y0;                                                       \
	VPADDQ  Y1, Y0, Y0;                                                       \
	VPXOR   Y0, Y3, Y3;                                                       \
	VPSHUFD $-79, Y3, Y3;                                                     \
	VPADDQ  Y3, Y2, Y2;                                                       \
	VPXOR   Y2, Y1, Y1;                                                       \
	VPSHUFB c40, Y1, Y1;                                                      \
	VPADDQ  m3, Y0, Y0;                                                       \
	VPADDQ  Y1, Y0, Y0;                                                       \
	VPXOR   Y0, Y3, Y3;                                                       \
	VPSHUFB c48, Y3, Y3;                                                      \
	VPADDQ  Y3, Y2, Y2;                                                       \
	VPXOR   Y2, Y1, Y1;                                                       \
	VPADDQ  Y1, Y1, t;                                                        \
	VPSRLQ  $63, Y1, Y1;                                                      \
	VPXOR   t, Y1, Y1;                                                        \
	BYTE    $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xdb; BYTE $0x39 \ // VPERMQ 0x39, Y3, Y3
	BYTE    $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xd2; BYTE $0x4e \ // VPERMQ 0x4e, Y2, Y2
	BYTE    $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xc9; BYTE $0x93 \ // VPERMQ 0x93, Y1, Y1

// load msg into Y12, Y13, Y14, Y15
#define LOAD_MSG_AVX2(src, i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15) \
	MOVQ        i0*8(src), X12;      \
	PINSRQ      $1, i1*8(src), X12;  \
	MOVQ        i2*8(src), X11;      \
	PINSRQ      $1, i3*8(src), X11;  \
	VINSERTI128 $1, X11, Y12, Y12;   \
	MOVQ        i4*8(src), X13;      \
	PINSRQ      $1, i5*8(src), X13;  \
	MOVQ        i6*8(src), X11;      \
	PINSRQ      $1, i7*8(src), X11;  \
	VINSERTI128 $1, X11, Y13, Y13;   \
	MOVQ        i8*8(src), X14;      \
	PINSRQ      $1, i9*8(src), X14;  \
	MOVQ        i10*8(src), X11;     \
	PINSRQ      $1, i11*8(src), X11; \
	VINSERTI128 $1, X11, Y14, Y14;   \
	MOVQ        i12*8(src), X15;     \
	PINSRQ      $1, i13*8(src), X15; \
	MOVQ        i14*8(src), X11;     \
	PINSRQ      $1, i15*8(src), X11; \
	VINSERTI128 $1, X11, Y15, Y15

// func hashBlocksAVX2(h *[8]uint64, c *[2]uint64, flag uint64, blocks []byte)
TEXT ·hashBlocksAVX2(SB), 4, $320-48 // frame size = 288 + 32 byte alignment
	MOVQ h+0(FP), AX
	MOVQ c+8(FP), BX
	MOVQ flag+16(FP), CX
	MOVQ blocks_base+24(FP), SI
	MOVQ blocks_len+32(FP), DI

	MOVQ SP, DX
	MOVQ SP, R9
	ADDQ $31, R9
	ANDQ $~31, R9
	MOVQ R9, SP

	MOVQ CX, 16(SP)
	XORQ CX, CX
	MOVQ CX, 24(SP)

	VMOVDQU ·AVX2_c40<>(SB), Y4
	VMOVDQU ·AVX2_c48<>(SB), Y5

	VMOVDQU 0(AX), Y8
	VMOVDQU 32(AX), Y9
	VMOVDQU ·AVX2_iv0<>(SB), Y6
	VMOVDQU ·AVX2_iv1<>(SB), Y7

	MOVQ 0(BX), R8
	MOVQ 8(BX), R9
	MOVQ R9, 8(SP)

loop:
	ADDQ $128, R8
	MOVQ R8, 0(SP)
	CMPQ R8, $128
	JGE  noinc
	INCQ R9
	MOVQ R9, 8(SP)

noinc:
	VMOVDQA Y8, Y0
	VMOVDQA Y9, Y1
	VMOVDQA Y6, Y2
	VPXOR   0(SP), Y7, Y3

	LOAD_MSG_AVX2(SI, 0, 2, 4, 6, 1, 3, 5, 7, 8, 10, 12, 14, 9, 11, 13, 15)
	VMOVDQA Y12, 32(SP)
	VMOVDQA Y13, 64(SP)
	VMOVDQA Y14, 96(SP)
	VMOVDQA Y15, 128(SP)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 14, 4, 9, 13, 10, 8, 15, 6, 1, 0, 11, 5, 12, 2, 7, 3)
	VMOVDQA Y12, 160(SP)
	VMOVDQA Y13, 192(SP)
	VMOVDQA Y14, 224(SP)
	VMOVDQA Y15, 256(SP)

	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 11, 12, 5, 15, 8, 0, 2, 13, 10, 3, 7, 9, 14, 6, 1, 4)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 7, 3, 13, 11, 9, 1, 12, 14, 2, 5, 4, 15, 6, 10, 0, 8)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 9, 5, 2, 10, 0, 7, 4, 15, 14, 11, 6, 3, 1, 12, 8, 13)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 2, 6, 0, 8, 12, 10, 11, 3, 4, 7, 15, 1, 13, 5, 14, 9)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 12, 1, 14, 4, 5, 15, 13, 10, 0, 6, 9, 8, 7, 3, 2, 11)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 13, 7, 12, 3, 11, 14, 1, 9, 5, 15, 8, 2, 0, 4, 6, 10)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 6, 14, 11, 0, 15, 9, 3, 8, 12, 13, 1, 10, 2, 7, 4, 5)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)
	LOAD_MSG_AVX2(SI, 10, 8, 7, 1, 2, 4, 6, 5, 15, 9, 3, 13, 11, 14, 12, 0)
	ROUND_AVX2(Y12, Y13, Y14, Y15, Y10, Y4, Y5)

	ROUND_AVX2(32(SP), 64(SP), 96(SP), 128(SP), Y10, Y4, Y5)
	ROUND_AVX2(160(SP), 192(SP), 224(SP), 256(SP), Y10, Y4, Y5)

	VPXOR Y0, Y8, Y8
	VPXOR Y1, Y9, Y9
	VPXOR Y2, Y8, Y8
	VPXOR Y3, Y9, Y9

	LEAQ 128(SI), SI
	SUBQ $128, DI
	JNE  loop

	MOVQ R8, 0(BX)
	MOVQ R9, 8(BX)

	VMOVDQU Y8, 0(AX)
	VMOVDQU Y9, 32(AX)

	MOVQ DX, SP
	RET

// unfortunately the BYTE representation of VPUNPCKLQDQ and VPUNPCKHQDQ must be used
#define VPUNPCKLQDQ_X8_X8_X10 BYTE $0xC4; BYTE $0x41; BYTE $0x39; BYTE $0x6C; BYTE $0xD0
#define VPUNPCKHQDQ_X7_X10_X6 BYTE $0xC4; BYTE $0xC1; BYTE $0x41; BYTE $0x6D; BYTE $0xF2
#define VPUNPCKLQDQ_X7_X7_X10 BYTE $0xC5; BYTE $0x41; BYTE $0x6C; BYTE $0xD7
#define VPUNPCKHQDQ_X8_X10_X7 BYTE $0xC4; BYTE $0xC1; BYTE $0x39; BYTE $0x6D; BYTE $0xFA
#define VPUNPCKLQDQ_X3_X3_X10 BYTE $0xC5; BYTE $0x61; BYTE $0x6C; BYTE $0xD3
#define VPUNPCKHQDQ_X2_X10_X2 BYTE $0xC4; BYTE $0xC1; BYTE $0x69; BYTE $0x6D; BYTE $0xD2
#define VPUNPCKLQDQ_X9_X9_X10 BYTE $0xC4; BYTE $0x41; BYTE $0x31; BYTE $0x6C; BYTE $0xD1
#define VPUNPCKHQDQ_X3_X10_X3 BYTE $0xC4; BYTE $0xC1; BYTE $0x61; BYTE $0x6D; BYTE $0xDA
#define VPUNPCKLQDQ_X2_X2_X10 BYTE $0xC5; BYTE $0x69; BYTE $0x6C; BYTE $0xD2
#define VPUNPCKHQDQ_X3_X10_X2 BYTE $0xC4; BYTE $0xC1; BYTE $0x61; BYTE $0x6D; BYTE $0xD2
#define VPUNPCKHQDQ_X8_X10_X3 BYTE $0xC4; BYTE $0xC1; BYTE $0x39; BYTE $0x6D; BYTE $0xDA
#define VPUNPCKHQDQ_X6_X10_X6 BYTE $0xC4; BYTE $0xC1; BYTE $0x49; BYTE $0x6D; BYTE $0xF2
#define VPUNPCKHQDQ_X7_X10_X7 BYTE $0xC4; BYTE $0xC1; BYTE $0x41; BYTE $0x6D; BYTE $0xFA

// shuffle X2 and X6 using the temp registers X8, X9, X10
#define SHUFFLE_AVX() \
	VMOVDQA X4, X9;        \
	VMOVDQA X5, X4;        \
	VMOVDQA X9, X5;        \
	VMOVDQA X6, X8;        \
	VPUNPCKLQDQ_X8_X8_X10; \
	VPUNPCKHQDQ_X7_X10_X6; \
	VPUNPCKLQDQ_X7_X7_X10; \
	VPUNPCKHQDQ_X8_X10_X7; \
	VPUNPCKLQDQ_X3_X3_X10; \
	VMOVDQA X2, X9;        \
	VPUNPCKHQDQ_X2_X10_X2; \
	VPUNPCKLQDQ_X9_X9_X10; \
	VPUNPCKHQDQ_X3_X10_X3; \

// inverse shuffle X2 and X6 using the temp registers X8, X9, X10
#define SHUFFLE_AVX_INV() \
	VMOVDQA X4, X9;        \
	VMOVDQA X5, X4;        \
	VMOVDQA X9, X5;        \
	VMOVDQA X2, X8;        \
	VPUNPCKLQDQ_X2_X2_X10; \
	VPUNPCKHQDQ_X3_X10_X2; \
	VPUNPCKLQDQ_X3_X3_X10; \
	VPUNPCKHQDQ_X8_X10_X3; \
	VPUNPCKLQDQ_X7_X7_X10; \
	VMOVDQA X6, X9;        \
	VPUNPCKHQDQ_X6_X10_X6; \
	VPUNPCKLQDQ_X9_X9_X10; \
	VPUNPCKHQDQ_X7_X10_X7; \

#define HALF_ROUND_AVX(v0, v1, v2, v3, v4, v5, v6, v7, m0, m1, m2, m3, t0, c40, c48) \
	VPADDQ  m0, v0, v0;   \
	VPADDQ  v2, v0, v0;   \
	VPADDQ  m1, v1, v1;   \
	VPADDQ  v3, v1, v1;   \
	VPXOR   v0, v6, v6;   \
	VPXOR   v1, v7, v7;   \
	VPSHUFD $-79, v6, v6; \
	VPSHUFD $-79, v7, v7; \
	VPADDQ  v6, v4, v4;   \
	VPADDQ  v7, v5, v5;   \
	VPXOR   v4, v2, v2;   \
	VPXOR   v5, v3, v3;   \
	VPSHUFB c40, v2, v2;  \
	VPSHUFB c40, v3, v3;  \
	VPADDQ  m2, v0, v0;   \
	VPADDQ  v2, v0, v0;   \
	VPADDQ  m3, v1, v1;   \
	VPADDQ  v3, v1, v1;   \
	VPXOR   v0, v6, v6;   \
	VPXOR   v1, v7, v7;   \
	VPSHUFB c48, v6, v6;  \
	VPSHUFB c48, v7, v7;  \
	VPADDQ  v6, v4, v4;   \
	VPADDQ  v7, v5, v5;   \
	VPXOR   v4, v2, v2;   \
	VPXOR   v5, v3, v3;   \
	VPADDQ  v2, v2, t0;   \
	VPSRLQ  $63, v2, v2;  \
	VPXOR   t0, v2, v2;   \
	VPADDQ  v3, v3, t0;   \
	VPSRLQ  $63, v3, v3;  \
	VPXOR   t0, v3, v3

// unfortunately the BYTE representation of VPINSRQ must be used
#define VPINSRQ_1_R10_X8_X8 BYTE $0xC4; BYTE $0x43; BYTE $0xB9; BYTE $0x22; BYTE $0xC2; BYTE $0x01
#define VPINSRQ_1_R11_X9_X9 BYTE $0xC4; BYTE $0x43; BYTE $0xB1; BYTE $0x22; BYTE $0xCB; BYTE $0x01
#define VPINSRQ_1_R12_X10_X10 BYTE $0xC4; BYTE $0x43; BYTE $0xA9; BYTE $0x22; BYTE $0xD4; BYTE $0x01
#define VPINSRQ_1_R13_X11_X11 BYTE $0xC4; BYTE $0x43; BYTE $0xA1; BYTE $0x22; BYTE $0xDD; BYTE $0x01

#define VPINSRQ_1_R9_X8_X8 BYTE $0xC4; BYTE $0x43; BYTE $0xB9; BYTE $0x22; BYTE $0xC1; BYTE $0x01

// load src into X8, X9, X10 and X11 using R10, R11, R12 and R13 for temp registers
#define LOAD_MSG_AVX(src, i0, i1, i2, i3, i4, i5, i6, i7) \
	MOVQ i0*8(src), X8;    \
	MOVQ i1*8(src), R10;   \
	MOVQ i2*8(src), X9;    \
	MOVQ i3*8(src), R11;   \
	MOVQ i4*8(src), X10;   \
	MOVQ i5*8(src), R12;   \
	MOVQ i6*8(src), X11;   \
	MOVQ i7*8(src), R13;   \
	VPINSRQ_1_R10_X8_X8;   \
	VPINSRQ_1_R11_X9_X9;   \
	VPINSRQ_1_R12_X10_X10; \
	VPINSRQ_1_R13_X11_X11

// func hashBlocksAVX(h *[8]uint64, c *[2]uint64, flag uint64, blocks []byte)
TEXT ·hashBlocksAVX(SB), 4, $288-48 // frame size = 272 + 16 byte alignment
	MOVQ h+0(FP), AX
	MOVQ c+8(FP), BX
	MOVQ flag+16(FP), CX
	MOVQ blocks_base+24(FP), SI
	MOVQ blocks_len+32(FP), DI

	MOVQ SP, BP
	MOVQ SP, R9
	ADDQ $15, R9
	ANDQ $~15, R9
	MOVQ R9, SP

	MOVOU ·AVX_c40<>(SB), X13
	MOVOU ·AVX_c48<>(SB), X14

	VMOVDQU ·AVX_iv3<>(SB), X0
	VMOVDQA X0, 0(SP)
	XORQ    CX, 0(SP)          // 0(SP) = ·AVX_iv3 ^ (CX || 0)

	VMOVDQU 0(AX), X12
	VMOVDQU 16(AX), X15
	VMOVDQU 32(AX), X2
	VMOVDQU 48(AX), X3

	MOVQ 0(BX), R8
	MOVQ 8(BX), R9

loop:
	ADDQ $128, R8
	CMPQ R8, $128
	JGE  noinc
	INCQ R9

noinc:
	MOVQ   R8, X8
	VPINSRQ_1_R9_X8_X8

	VMOVDQA X12, X0
	VMOVDQA X15, X1
	VMOVDQU ·AVX_iv0<>(SB), X4
	VMOVDQU ·AVX_iv1<>(SB), X5
	VMOVDQU ·AVX_iv2<>(SB), X6

	VPXOR   X8, X6, X6
	VMOVDQA 0(SP), X7

	LOAD_MSG_AVX(SI, 0, 2, 4, 6, 1, 3, 5, 7)
	VMOVDQA X8, 16(SP)
	VMOVDQA X9, 32(SP)
	VMOVDQA X10, 48(SP)
	VMOVDQA X11, 64(SP)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 8, 10, 12, 14, 9, 11, 13, 15)
	VMOVDQA X8, 80(SP)
	VMOVDQA X9, 96(SP)
	VMOVDQA X10, 112(SP)
	VMOVDQA X11, 128(SP)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 14, 4, 9, 13, 10, 8, 15, 6)
	VMOVDQA X8, 144(SP)
	VMOVDQA X9, 160(SP)
	VMOVDQA X10, 176(SP)
	VMOVDQA X11, 192(SP)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 1, 0, 11, 5, 12, 2, 7, 3)
	VMOVDQA X8, 208(SP)
	VMOVDQA X9, 224(SP)
	VMOVDQA X10, 240(SP)
	VMOVDQA X11, 256(SP)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 11, 12, 5, 15, 8, 0, 2, 13)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 10, 3, 7, 9, 14, 6, 1, 4)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 7, 3, 13, 11, 9, 1, 12, 14)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 2, 5, 4, 15, 6, 10, 0, 8)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 9, 5, 2, 10, 0, 7, 4, 15)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 14, 11, 6, 3, 1, 12, 8, 13)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 2, 6, 0, 8, 12, 10, 11, 3)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 4, 7, 15, 1, 13, 5, 14, 9)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 12, 1, 14, 4, 5, 15, 13, 10)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 0, 6, 9, 8, 7, 3, 2, 11)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 13, 7, 12, 3, 11, 14, 1, 9)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 5, 15, 8, 2, 0, 4, 6, 10)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 6, 14, 11, 0, 15, 9, 3, 8)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 12, 13, 1, 10, 2, 7, 4, 5)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	LOAD_MSG_AVX(SI, 10, 8, 7, 1, 2, 4, 6, 5)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX()
	LOAD_MSG_AVX(SI, 15, 9, 3, 13, 11, 14, 12, 0)
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11, X11, X13, X14)
	SHUFFLE_AVX_INV()

	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, 16(SP), 32(SP), 48(SP), 64(SP), X11, X13, X14)
	SHUFFLE_AVX()
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, 80(SP), 96(SP), 112(SP), 128(SP), X11, X13, X14)
	SHUFFLE_AVX_INV()

	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, 144(SP), 160(SP), 176(SP), 192(SP), X11, X13, X14)
	SHUFFLE_AVX()
	HALF_ROUND_AVX(X0, X1, X2, X3, X4, X5, X6, X7, 208(SP), 224(SP), 240(SP), 256(SP), X11, X13, X14)
	SHUFFLE_AVX_INV()

	VMOVDQU 32(AX), X10
	VMOVDQU 48(AX), X11
	VPXOR   X0, X12, X12
	VPXOR   X1, X15, X15
	VPXOR   X2, X10, X10
	VPXOR   X3, X11, X11
	VPXOR   X4, X12, X12
	VPXOR   X5, X15, X15
	VPXOR   X6, X10, X2
	VPXOR   X7, X11, X3
	VMOVDQU X2, 32(AX)
	VMOVDQU X3, 48(AX)

	LEAQ 128(SI), SI
	SUBQ $128, DI
	JNE  loop

	VMOVDQU X12, 0(AX)
	VMOVDQU X15, 16(AX)

	MOVQ R8, 0(BX)
	MOVQ R9, 8(BX)

	VZEROUPPER

	MOVQ BP, SP
	RET

// func supportsAVX2() bool
TEXT ·supportsAVX2(SB), 4, $0-1
	MOVQ runtime·support_avx2(SB), AX
	MOVB AX, ret+0(FP)
	RET

// func supportsAVX() bool
TEXT ·supportsAVX(SB), 4, $0-1
	MOVQ runtime·support_avx(SB), AX
	MOVB AX, ret+0(FP)
	RET
