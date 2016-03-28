//+build !noasm !appengine

// Copyright 2015, Klaus Post, see LICENSE for details.

// Based on http://www.snia.org/sites/default/files2/SDC2013/presentations/NewThinking/EthanMiller_Screaming_Fast_Galois_Field%20Arithmetic_SIMD%20Instructions.pdf
// and http://jerasure.org/jerasure/gf-complete/tree/master

// func galMulSSSE3Xor(low, high, in, out []byte)
TEXT ·galMulSSSE3Xor(SB), 7, $0
	MOVQ   low+0(FP), SI     // SI: &low
	MOVQ   high+24(FP), DX   // DX: &high
	MOVOU  (SI), X6          // X6 low
	MOVOU  (DX), X7          // X7: high
	MOVQ   $15, BX           // BX: low mask
	MOVQ   BX, X8
	PXOR   X5, X5
	MOVQ   in+48(FP), SI     // R11: &in
	MOVQ   in_len+56(FP), R9 // R9: len(in)
	MOVQ   out+72(FP), DX    // DX: &out
	PSHUFB X5, X8            // X8: lomask (unpacked)
	SHRQ   $4, R9            // len(in) / 16
	CMPQ   R9, $0
	JEQ    done_xor

loopback_xor:
	MOVOU  (SI), X0     // in[x]
	MOVOU  (DX), X4     // out[x]
	MOVOU  X0, X1       // in[x]
	MOVOU  X6, X2       // low copy
	MOVOU  X7, X3       // high copy
	PSRLQ  $4, X1       // X1: high input
	PAND   X8, X0       // X0: low input
	PAND   X8, X1       // X0: high input
	PSHUFB X0, X2       // X2: mul low part
	PSHUFB X1, X3       // X3: mul high part
	PXOR   X2, X3       // X3: Result
	PXOR   X4, X3       // X3: Result xor existing out
	MOVOU  X3, (DX)     // Store
	ADDQ   $16, SI      // in+=16
	ADDQ   $16, DX      // out+=16
	SUBQ   $1, R9
	JNZ    loopback_xor

done_xor:
	RET

// func galMulSSSE3(low, high, in, out []byte)
TEXT ·galMulSSSE3(SB), 7, $0
	MOVQ   low+0(FP), SI     // SI: &low
	MOVQ   high+24(FP), DX   // DX: &high
	MOVOU  (SI), X6          // X6 low
	MOVOU  (DX), X7          // X7: high
	MOVQ   $15, BX           // BX: low mask
	MOVQ   BX, X8
	PXOR   X5, X5
	MOVQ   in+48(FP), SI     // R11: &in
	MOVQ   in_len+56(FP), R9 // R9: len(in)
	MOVQ   out+72(FP), DX    // DX: &out
	PSHUFB X5, X8            // X8: lomask (unpacked)
	SHRQ   $4, R9            // len(in) / 16
	CMPQ   R9, $0
	JEQ    done

loopback:
	MOVOU  (SI), X0 // in[x]
	MOVOU  X0, X1   // in[x]
	MOVOU  X6, X2   // low copy
	MOVOU  X7, X3   // high copy
	PSRLQ  $4, X1   // X1: high input
	PAND   X8, X0   // X0: low input
	PAND   X8, X1   // X0: high input
	PSHUFB X0, X2   // X2: mul low part
	PSHUFB X1, X3   // X3: mul high part
	PXOR   X2, X3   // X3: Result
	MOVOU  X3, (DX) // Store
	ADDQ   $16, SI  // in+=16
	ADDQ   $16, DX  // out+=16
	SUBQ   $1, R9
	JNZ    loopback

done:
	RET

// func galMulAVX2Xor(low, high, in, out []byte)
TEXT ·galMulAVX2Xor(SB), 7, $0
	MOVQ  low+0(FP), SI     // SI: &low
	MOVQ  high+24(FP), DX   // DX: &high
	MOVQ  $15, BX           // BX: low mask
	MOVQ  BX, X5
	MOVOU (SI), X6          // X6 low
	MOVOU (DX), X7          // X7: high
	MOVQ  in_len+56(FP), R9 // R9: len(in)

/*
YASM:

VINSERTI128 YMM6, YMM6, XMM6, 1 ; low
VINSERTI128 YMM7, YMM7, XMM7, 1 ; high
VPBROADCASTB YMM8, XMM5         ; X8: lomask (unpacked)
*/
	BYTE $0xc4; BYTE $0xe3; BYTE $0x4d; BYTE $0x38; BYTE $0xf6; BYTE $0x01; BYTE $0xc4; BYTE $0xe3; BYTE $0x45; BYTE $0x38; BYTE $0xff; BYTE $0x01; BYTE $0xc4; BYTE $0x62; BYTE $0x7d; BYTE $0x78; BYTE $0xc5

	SHRQ  $5, R9         // len(in) /32
	MOVQ  out+72(FP), DX // DX: &out
	MOVQ  in+48(FP), SI  // R11: &in
	TESTQ R9, R9
	JZ    done_xor_avx2

loopback_xor_avx2:
/* Yasm:

VMOVDQU YMM0, [rsi]
VMOVDQU YMM4, [rdx]
VPSRLQ  YMM1, YMM0, 4   ; X1: high input
VPAND   YMM0, YMM0, YMM8      ; X0: low input
VPAND   YMM1, YMM1, YMM8      ; X1: high input
VPSHUFB  YMM2, YMM6, YMM0   ; X2: mul low part
VPSHUFB  YMM3, YMM7, YMM1   ; X2: mul high part
VPXOR   YMM3, YMM2, YMM3    ; X3: Result
VPXOR   YMM4, YMM3, YMM4    ; X4: Result
VMOVDQU [rdx], YMM4
*/
	BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x06; BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x22; BYTE $0xc5; BYTE $0xf5; BYTE $0x73; BYTE $0xd0; BYTE $0x04; BYTE $0xc4; BYTE $0xc1; BYTE $0x7d; BYTE $0xdb; BYTE $0xc0; BYTE $0xc4; BYTE $0xc1; BYTE $0x75; BYTE $0xdb; BYTE $0xc8; BYTE $0xc4; BYTE $0xe2; BYTE $0x4d; BYTE $0x00; BYTE $0xd0; BYTE $0xc4; BYTE $0xe2; BYTE $0x45; BYTE $0x00; BYTE $0xd9; BYTE $0xc5; BYTE $0xed; BYTE $0xef; BYTE $0xdb; BYTE $0xc5; BYTE $0xe5; BYTE $0xef; BYTE $0xe4; BYTE $0xc5; BYTE $0xfe; BYTE $0x7f; BYTE $0x22

	ADDQ $32, SI           // in+=32
	ADDQ $32, DX           // out+=32
	SUBQ $1, R9
	JNZ  loopback_xor_avx2

done_xor_avx2:
	// VZEROUPPER
	BYTE $0xc5; BYTE $0xf8; BYTE $0x77
	RET

// func galMulAVX2(low, high, in, out []byte)
TEXT ·galMulAVX2(SB), 7, $0
	MOVQ  low+0(FP), SI     // SI: &low
	MOVQ  high+24(FP), DX   // DX: &high
	MOVQ  $15, BX           // BX: low mask
	MOVQ  BX, X5
	MOVOU (SI), X6          // X6 low
	MOVOU (DX), X7          // X7: high
	MOVQ  in_len+56(FP), R9 // R9: len(in)

/*
YASM:

VINSERTI128 YMM6, YMM6, XMM6, 1 ; low
VINSERTI128 YMM7, YMM7, XMM7, 1 ; high
VPBROADCASTB YMM8, XMM5         ; X8: lomask (unpacked)
*/
	BYTE $0xc4; BYTE $0xe3; BYTE $0x4d; BYTE $0x38; BYTE $0xf6; BYTE $0x01; BYTE $0xc4; BYTE $0xe3; BYTE $0x45; BYTE $0x38; BYTE $0xff; BYTE $0x01; BYTE $0xc4; BYTE $0x62; BYTE $0x7d; BYTE $0x78; BYTE $0xc5

	SHRQ  $5, R9         // len(in) /32
	MOVQ  out+72(FP), DX // DX: &out
	MOVQ  in+48(FP), SI  // R11: &in
	TESTQ R9, R9
	JZ    done_avx2

loopback_avx2:
/* Yasm:

VMOVDQU YMM0, [rsi]
VPSRLQ  YMM1, YMM0, 4   ; X1: high input
VPAND   YMM0, YMM0, YMM8      ; X0: low input
VPAND   YMM1, YMM1, YMM8      ; X1: high input
VPSHUFB  YMM2, YMM6, YMM0   ; X2: mul low part
VPSHUFB  YMM3, YMM7, YMM1   ; X2: mul high part
VPXOR   YMM4, YMM2, YMM3    ; X4: Result
VMOVDQU [rdx], YMM4
*/
	BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x06; BYTE $0xc5; BYTE $0xf5; BYTE $0x73; BYTE $0xd0; BYTE $0x04; BYTE $0xc4; BYTE $0xc1; BYTE $0x7d; BYTE $0xdb; BYTE $0xc0; BYTE $0xc4; BYTE $0xc1; BYTE $0x75; BYTE $0xdb; BYTE $0xc8; BYTE $0xc4; BYTE $0xe2; BYTE $0x4d; BYTE $0x00; BYTE $0xd0; BYTE $0xc4; BYTE $0xe2; BYTE $0x45; BYTE $0x00; BYTE $0xd9; BYTE $0xc5; BYTE $0xed; BYTE $0xef; BYTE $0xe3; BYTE $0xc5; BYTE $0xfe; BYTE $0x7f; BYTE $0x22

	ADDQ $32, SI       // in+=32
	ADDQ $32, DX       // out+=32
	SUBQ $1, R9
	JNZ  loopback_avx2
	JMP  done_avx2

done_avx2:
	// VZEROUPPER
	BYTE $0xc5; BYTE $0xf8; BYTE $0x77
	RET
