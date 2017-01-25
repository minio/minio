//+build !noasm !appengine

//
// Minio Cloud Storage, (C) 2016 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//
// Based on SSE implementation from https://github.com/BLAKE2/BLAKE2/blob/master/sse/blake2b.c
//
// Use github.com/fwessels/asm2plan9s on this file to assemble instructions to their Plan9 equivalent
//
// Assembly code below essentially follows the ROUND macro (see blake2b-round.h) which is defined as:
//   #define ROUND(r) \
//     LOAD_MSG_ ##r ##_1(b0, b1); \
//     G1(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h,b0,b1); \
//     LOAD_MSG_ ##r ##_2(b0, b1); \
//     G2(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h,b0,b1); \
//     DIAGONALIZE(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h); \
//     LOAD_MSG_ ##r ##_3(b0, b1); \
//     G1(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h,b0,b1); \
//     LOAD_MSG_ ##r ##_4(b0, b1); \
//     G2(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h,b0,b1); \
//     UNDIAGONALIZE(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h);
//
// as well as the go equivalent in https://github.com/dchest/blake2b/blob/master/block.go
//
// As in the macro, G1/G2 in the 1st and 2nd half are identical (so literal copy of assembly)
//
// Rounds are also the same, except for the loading of the message (and rounds 1 & 11 and
// rounds 2 & 12 are identical)
//

#define G1 \
	\ // G1(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h,b0,b1);
	LONG $0xd40f4166; BYTE $0xc0   \ // PADDQ  XMM0,XMM8        /* v0 += m[0], v1 += m[2] */
	LONG $0xd40f4166; BYTE $0xc9   \ // PADDQ  XMM1,XMM9        /* v2 += m[4], v3 += m[6] */
	LONG $0xc2d40f66               \ // PADDQ  XMM0,XMM2        /* v0 += v4, v1 += v5 */
	LONG $0xcbd40f66               \ // PADDQ  XMM1,XMM3        /* v2 += v6, v3 += v7 */
	LONG $0xf0ef0f66               \ // PXOR   XMM6,XMM0        /* v12 ^= v0, v13 ^= v1 */
	LONG $0xf9ef0f66               \ // PXOR   XMM7,XMM1        /* v14 ^= v2, v15 ^= v3 */
	LONG $0xf6700f66; BYTE $0xb1   \ // PSHUFD XMM6,XMM6,0xb1   /* v12 = v12<<(64-32) | v12>>32, v13 = v13<<(64-32) | v13>>32 */
	LONG $0xff700f66; BYTE $0xb1   \ // PSHUFD XMM7,XMM7,0xb1   /* v14 = v14<<(64-32) | v14>>32, v15 = v15<<(64-32) | v15>>32 */
	LONG $0xe6d40f66               \ // PADDQ  XMM4,XMM6        /* v8 += v12, v9 += v13  */
	LONG $0xefd40f66               \ // PADDQ  XMM5,XMM7        /* v10 += v14, v11 += v15 */
	LONG $0xd4ef0f66               \ // PXOR   XMM2,XMM4        /* v4 ^= v8, v5 ^= v9 */
	LONG $0xddef0f66               \ // PXOR   XMM3,XMM5        /* v6 ^= v10, v7 ^= v11 */
	LONG $0x380f4166; WORD $0xd400 \ // PSHUFB XMM2,XMM12       /* v4 = v4<<(64-24) | v4>>24, v5 = v5<<(64-24) | v5>>24 */
	LONG $0x380f4166; WORD $0xdc00   // PSHUFB XMM3,XMM12       /* v6 = v6<<(64-24) | v6>>24, v7 = v7<<(64-24) | v7>>24 */

#define G2 \
	\ // G2(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h,b0,b1);
	LONG  $0xd40f4166; BYTE $0xc2   \ // PADDQ  XMM0,XMM10        /* v0 += m[1], v1 += m[3] */
	LONG  $0xd40f4166; BYTE $0xcb   \ // PADDQ  XMM1,XMM11        /* v2 += m[5], v3 += m[7] */
	LONG  $0xc2d40f66               \ // PADDQ  XMM0,XMM2         /* v0 += v4, v1 += v5 */
	LONG  $0xcbd40f66               \ // PADDQ  XMM1,XMM3         /* v2 += v6, v3 += v7 */
	LONG  $0xf0ef0f66               \ // PXOR   XMM6,XMM0         /* v12 ^= v0, v13 ^= v1 */
	LONG  $0xf9ef0f66               \ // PXOR   XMM7,XMM1         /* v14 ^= v2, v15 ^= v3 */
	LONG  $0xf6700ff2; BYTE $0x39   \ // PSHUFLW XMM6,XMM6,0x39   /* combined with next ... */
	LONG  $0xf6700ff3; BYTE $0x39   \ // PSHUFHW XMM6,XMM6,0x39   /* v12 = v12<<(64-16) | v12>>16, v13 = v13<<(64-16) | v13>>16 */
	LONG  $0xff700ff2; BYTE $0x39   \ // PSHUFLW XMM7,XMM7,0x39   /* combined with next ... */
	LONG  $0xff700ff3; BYTE $0x39   \ // PSHUFHW XMM7,XMM7,0x39   /* v14 = v14<<(64-16) | v14>>16, v15 = v15<<(64-16) | v15>>16 */
	LONG  $0xe6d40f66               \ // PADDQ  XMM4,XMM6         /* v8 += v12, v9 += v13 */
	LONG  $0xefd40f66               \ // PADDQ  XMM5,XMM7         /* v10 += v14, v11 += v15 */
	LONG  $0xd4ef0f66               \ // PXOR   XMM2,XMM4         /* v4 ^= v8, v5 ^= v9 */
	LONG  $0xddef0f66               \ // PXOR   XMM3,XMM5         /* v6 ^= v10, v7 ^= v11 */
	MOVOU X2, X15                   \
	LONG  $0xd40f4466; BYTE $0xfa   \ // PADDQ  XMM15,XMM2        /* temp reg = reg*2   */
	LONG  $0xd2730f66; BYTE $0x3f   \ // PSRLQ  XMM2,0x3f         /*      reg = reg>>63 */
	LONG  $0xef0f4166; BYTE $0xd7   \ // PXOR   XMM2,XMM15        /* ORed together: v4 = v4<<(64-63) | v4>>63, v5 = v5<<(64-63) | v5>>63 */
	MOVOU X3, X15                   \
	LONG  $0xd40f4466; BYTE $0xfb   \ // PADDQ XMM15,XMM3         /* temp reg = reg*2   */
	LONG  $0xd3730f66; BYTE $0x3f   \ // PSRLQ XMM3,0x3f          /*      reg = reg>>63 */
	LONG  $0xef0f4166; BYTE $0xdf     // PXOR  XMM3,XMM15         /* ORed together: v6 = v6<<(64-63) | v6>>63, v7 = v7<<(64-63) | v7>>63 */

#define DIAGONALIZE \
	\ // DIAGONALIZE(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h);
	MOVOU X6, X13                   \                                     /* t0 = row4l;\                                                            */
	MOVOU X2, X14                   \                                     /* t1 = row2l;\                                                            */
	MOVOU X4, X6                    \                                     /* row4l = row3l;\                                                         */
	MOVOU X5, X4                    \                                     /* row3l = row3h;\                                                         */
	MOVOU X6, X5                    \                                     /* row3h = row4l;\                                                         */
	LONG  $0x6c0f4566; BYTE $0xfd   \ // PUNPCKLQDQ XMM15, XMM13          /*                                    _mm_unpacklo_epi64(t0, t0)           */
	MOVOU X7, X6                    \
	LONG  $0x6d0f4166; BYTE $0xf7   \ // PUNPCKHQDQ  XMM6, XMM15          /*  row4l = _mm_unpackhi_epi64(row4h,                           ); \       */
	LONG  $0x6c0f4466; BYTE $0xff   \ // PUNPCKLQDQ XMM15,  XMM7          /*                                 _mm_unpacklo_epi64(row4h, row4h)        */
	MOVOU X13, X7                   \
	LONG  $0x6d0f4166; BYTE $0xff   \ // PUNPCKHQDQ  XMM7, XMM15          /*  row4h = _mm_unpackhi_epi64(t0,                                 ); \    */
	LONG  $0x6c0f4466; BYTE $0xfb   \ // PUNPCKLQDQ XMM15,  XMM3          /*                                    _mm_unpacklo_epi64(row2h, row2h)     */
	LONG  $0x6d0f4166; BYTE $0xd7   \ // PUNPCKHQDQ  XMM2, XMM15          /*  row2l = _mm_unpackhi_epi64(row2l,                                 ); \ */
	LONG  $0x6c0f4566; BYTE $0xfe   \ // PUNPCKLQDQ XMM15, XMM14          /*                                    _mm_unpacklo_epi64(t1, t1)           */
	LONG  $0x6d0f4166; BYTE $0xdf     // PUNPCKHQDQ  XMM3, XMM15          /*  row2h = _mm_unpackhi_epi64(row2h,                           )          */

#define UNDIAGONALIZE \
	\ // UNDIAGONALIZE(row1l,row2l,row3l,row4l,row1h,row2h,row3h,row4h);
	MOVOU X4, X13                   \                                     /* t0 = row3l;\                                                            */
	MOVOU X5, X4                    \                                     /* row3l = row3h;\                                                         */
	MOVOU X13, X5                   \                                     /* row3h = t0;\                                                            */
	MOVOU X2, X13                   \                                     /* t0 = row2l;\                                                            */
	MOVOU X6, X14                   \                                     /* t1 = row4l;\                                                            */
	LONG  $0x6c0f4466; BYTE $0xfa   \ // PUNPCKLQDQ XMM15,  XMM2          /*                                    _mm_unpacklo_epi64(row2l, row2l)     */
	MOVOU X3, X2                    \
	LONG  $0x6d0f4166; BYTE $0xd7   \ // PUNPCKHQDQ  XMM2, XMM15          /*  row2l = _mm_unpackhi_epi64(row2h,                                 ); \ */
	LONG  $0x6c0f4466; BYTE $0xfb   \ // PUNPCKLQDQ XMM15,  XMM3          /*                                 _mm_unpacklo_epi64(row2h, row2h)        */
	MOVOU X13, X3                   \
	LONG  $0x6d0f4166; BYTE $0xdf   \ // PUNPCKHQDQ  XMM3, XMM15          /*  row2h = _mm_unpackhi_epi64(t0,                                 ); \    */
	LONG  $0x6c0f4466; BYTE $0xff   \ // PUNPCKLQDQ XMM15,  XMM7          /*                                    _mm_unpacklo_epi64(row4h, row4h)     */
	LONG  $0x6d0f4166; BYTE $0xf7   \ // PUNPCKHQDQ  XMM6, XMM15          /*  row4l = _mm_unpackhi_epi64(row4l,                                 ); \ */
	LONG  $0x6c0f4566; BYTE $0xfe   \ // PUNPCKLQDQ XMM15, XMM14          /*                                    _mm_unpacklo_epi64(t1, t1)           */
	LONG  $0x6d0f4166; BYTE $0xff     // PUNPCKHQDQ  XMM7, XMM15          /*  row4h = _mm_unpackhi_epi64(row4h,                           )          */

#define LOAD_SHUFFLE \
	\ // Load shuffle value
	MOVQ  shffle+120(FP), SI \ // SI: &shuffle
	MOVOU 0(SI), X12           // X12 = 03040506 07000102 0b0c0d0e 0f08090a

// func blockSSELoop(p []uint8, in, iv, t, f, shffle, out []uint64)
TEXT ·blockSSELoop(SB), 7, $0
	// REGISTER USE
	//        R8: loop counter
	//        DX: message pointer
	//        SI: temp pointer for loading
	//  X0 -  X7: v0 - v15
	//  X8 - X11: m[0] - m[7]
	//       X12: shuffle value
	// X13 - X15: temp registers

	// Load digest
	MOVQ  in+24(FP), SI // SI: &in
	MOVOU 0(SI), X0     // X0 = in[0]+in[1]      /* row1l = LOAD( &S->h[0] ); */
	MOVOU 16(SI), X1    // X1 = in[2]+in[3]      /* row1h = LOAD( &S->h[2] ); */
	MOVOU 32(SI), X2    // X2 = in[4]+in[5]      /* row2l = LOAD( &S->h[4] ); */
	MOVOU 48(SI), X3    // X3 = in[6]+in[7]      /* row2h = LOAD( &S->h[6] ); */

	// Already store digest into &out (so we can reload it later generically)
	MOVQ  out+144(FP), SI // SI: &out
	MOVOU X0, 0(SI)       // out[0]+out[1] = X0
	MOVOU X1, 16(SI)      // out[2]+out[3] = X1
	MOVOU X2, 32(SI)      // out[4]+out[5] = X2
	MOVOU X3, 48(SI)      // out[6]+out[7] = X3

	// Initialize message pointer and loop counter
	MOVQ message+0(FP), DX     // DX: &p (message)
	MOVQ message_len+8(FP), R8 // R8: len(message)
	SHRQ $7, R8                // len(message) / 128
	CMPQ R8, $0
	JEQ  complete

loop:
	// Increment counter
	MOVQ t+72(FP), SI // SI: &t
	MOVQ 0(SI), R9
	ADDQ $128, R9     // /* d.t[0] += BlockSize */
	MOVQ R9, 0(SI)
	CMPQ R9, $128     // /* if d.t[0] < BlockSize { */
	JGE  noincr
	MOVQ 8(SI), R9
	ADDQ $1, R9       // /*     d.t[1]++ */
	MOVQ R9, 8(SI)

noincr:  // /* } */

	// Load initialization vector
	MOVQ  iv+48(FP), SI // SI: &iv
	MOVOU 0(SI), X4     // X4 = iv[0]+iv[1]      /* row3l = LOAD( &blake2b_IV[0] ); */
	MOVOU 16(SI), X5    // X5 = iv[2]+iv[3]      /* row3h = LOAD( &blake2b_IV[2] ); */
	MOVOU 32(SI), X6    // X6 = iv[4]+iv[5]      /*                        LOAD( &blake2b_IV[4] )                      */
	MOVOU 48(SI), X7    // X7 = iv[6]+iv[7]      /*                        LOAD( &blake2b_IV[6] )                      */
	MOVQ  t+72(FP), SI  // SI: &t
	MOVOU 0(SI), X8     // X8 = t[0]+t[1]        /*                                                LOAD( &S->t[0] )    */
	PXOR  X8, X6        // X6 = X6 ^ X8          /* row4l = _mm_xor_si128(                       ,                  ); */
	MOVQ  t+96(FP), SI  // SI: &f
	MOVOU 0(SI), X8     // X8 = f[0]+f[1]        /*                                                LOAD( &S->f[0] )    */
	PXOR  X8, X7        // X7 = X7 ^ X8          /* row4h = _mm_xor_si128(                       ,                  ); */

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   1
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12             // X12 = m[0]+m[1]
	MOVOU 16(DX), X13            // X13 = m[2]+m[3]
	MOVOU 32(DX), X14            // X14 = m[4]+m[5]
	MOVOU 48(DX), X15            // X15 = m[6]+m[7]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5 // PUNPCKLQDQ  XMM8, XMM13  /* m[0], m[2] */
	MOVOU X14, X9
	LONG $0x6c0f4566; BYTE $0xcf // PUNPCKLQDQ  XMM9, XMM15  /* m[4], m[6] */
	MOVOU X12, X10
	LONG $0x6d0f4566; BYTE $0xd5 // PUNPCKHQDQ XMM10, XMM13  /* m[1], m[3] */
	MOVOU X14, X11
	LONG $0x6d0f4566; BYTE $0xdf // PUNPCKHQDQ XMM11, XMM15  /* m[5], m[7] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 64(DX), X12            // X12 =  m[8]+ m[9]
	MOVOU 80(DX), X13            // X13 = m[10]+m[11]
	MOVOU 96(DX), X14            // X14 = m[12]+m[13]
	MOVOU 112(DX), X15           // X15 = m[14]+m[15]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5 // PUNPCKLQDQ  XMM8, XMM13  /*  m[8],m[10] */
	MOVOU X14, X9
	LONG $0x6c0f4566; BYTE $0xcf // PUNPCKLQDQ  XMM9, XMM15  /* m[12],m[14] */
	MOVOU X12, X10
	LONG $0x6d0f4566; BYTE $0xd5 // PUNPCKHQDQ XMM10, XMM13  /*  m[9],m[11] */
	MOVOU X14, X11
	LONG $0x6d0f4566; BYTE $0xdf // PUNPCKHQDQ XMM11, XMM15  /* m[13],m[15] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   2
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 112(DX), X12                         // X12 = m[14]+m[15]
	MOVOU 32(DX), X13                          // X13 =  m[4]+ m[5]
	MOVOU 64(DX), X14                          // X14 =  m[8]+ m[9]
	MOVOU 96(DX), X15                          // X15 = m[12]+m[13]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5               // PUNPCKLQDQ  XMM8, XMM13  /* m[14],  m[4] */
	MOVOU X14, X9
	LONG $0x6d0f4566; BYTE $0xcf               // PUNPCKHQDQ  XMM9, XMM15  /*  m[9], m[13] */
	MOVOU 80(DX), X10                          // X10 = m[10]+m[11]
	MOVOU 48(DX), X11                          // X11 =  m[6]+ m[7]
	LONG $0x6c0f4566; BYTE $0xd6               // PUNPCKLQDQ XMM10, XMM14  /* m[10],  m[8] */
	LONG $0x3a0f4566; WORD $0xdc0f; BYTE $0x08 // PALIGNR    XMM11, XMM12, 0x8  /* m[15],  m[6] */; ; ; ; ;

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 32(DX), X13                          // X13 =  m[4]+ m[5]
	MOVOU 80(DX), X14                          // X14 = m[10]+m[11]
	MOVOU X12, X8
	LONG $0x3a0f4566; WORD $0xc40f; BYTE $0x08 // PALIGNR     XMM8, XMM12, 0x8  /*  m[1],  m[0] */
	MOVOU X14, X9
	LONG $0x6d0f4566; BYTE $0xcd               // PUNPCKHQDQ  XMM9, XMM13  /* m[11], m[5] */
	MOVOU 16(DX), X12                          // X12 =  m[2]+ m[3]
	MOVOU 48(DX), X11                          // X11 =  m[6]+ m[7]
	MOVOU 96(DX), X10                          // X10 = m[12]+m[13]
	LONG $0x6c0f4566; BYTE $0xd4               // PUNPCKLQDQ XMM10, XMM12  /* m[12], m[2] */
	LONG $0x6d0f4566; BYTE $0xdc               // PUNPCKHQDQ XMM11, XMM12  /*  m[7], m[3] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   3
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 32(DX), X12                          // X12 =  m[4]+ m[5]
	MOVOU 80(DX), X13                          // X13 = m[10]+m[11]
	MOVOU 96(DX), X14                          // X14 = m[12]+m[13]
	MOVOU 112(DX), X15                         // X15 = m[14]+m[15]
	MOVOU X14, X8
	LONG $0x3a0f4566; WORD $0xc50f; BYTE $0x08 // PALIGNR     XMM8, XMM13, 0x8  /* m[11],  m[12] */
	MOVOU X12, X9
	LONG $0x6d0f4566; BYTE $0xcf               // PUNPCKHQDQ  XMM9, XMM15  /*  m[5], m[15] */
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 16(DX), X13                          // X13 =  m[2]+ m[3]
	MOVOU 64(DX), X10                          // X10 =  m[8]+ m[9]
	LONG $0x6c0f4566; BYTE $0xd4               // PUNPCKLQDQ XMM10, XMM12  /*  m[8],  m[0] */
	LONG $0x6d0f4566; BYTE $0xf6               // PUNPCKHQDQ XMM14, XMM14  /*   ___, m[13] */
	MOVOU X13, X11
	LONG $0x6c0f4566; BYTE $0xde               // PUNPCKLQDQ XMM11, XMM14  /*  m[2],   ___ */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12                          // X12 =  m[2]+ m[3]
	MOVOU 48(DX), X13                          // X13 =  m[6]+ m[7]
	MOVOU 64(DX), X14                          // X14 =  m[8]+ m[9]
	MOVOU 80(DX), X15                          // X15 = m[10]+m[11]
	MOVOU X12, X9
	LONG $0x6d0f4566; BYTE $0xcc               // PUNPCKHQDQ  XMM9, XMM12  /*   ___, m[3] */
	MOVOU X15, X8
	LONG $0x6c0f4566; BYTE $0xc1               // PUNPCKLQDQ  XMM8,  XMM9  /* m[10],  ___ */
	MOVOU X13, X9
	LONG $0x6d0f4566; BYTE $0xce               // PUNPCKHQDQ  XMM9, XMM14  /*  m[7], m[9] */
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 32(DX), X11                          // X11 =  m[4]+ m[5]
	MOVOU 112(DX), X10                         // X10 = m[14]+m[15]
	LONG $0x6c0f4566; BYTE $0xd5               // PUNPCKLQDQ XMM10, XMM13  /* m[14], m[6] */
	LONG $0x3a0f4566; WORD $0xdc0f; BYTE $0x08 // PALIGNR    XMM11, XMM12, 0x8  /*  m[1],  m[4] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   4
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12            // X12 =  m[2]+ m[3]
	MOVOU 48(DX), X13            // X13 =  m[6]+ m[7]
	MOVOU 80(DX), X14            // X14 = m[10]+m[11]
	MOVOU 96(DX), X15            // X15 = m[12]+m[13]
	MOVOU X13, X8
	LONG $0x6d0f4566; BYTE $0xc4 // PUNPCKHQDQ  XMM8, XMM12  /*  m[7],  m[3] */
	MOVOU X15, X9
	LONG $0x6d0f4566; BYTE $0xce // PUNPCKHQDQ  XMM9, XMM14  /* m[13], m[11] */
	MOVOU 0(DX), X12             // X12 =  m[0]+ m[1]
	MOVOU 64(DX), X10            // X10 =  m[8]+ m[9]
	MOVOU 112(DX), X14           // X14 = m[14]+m[15]
	LONG $0x6d0f4566; BYTE $0xd4 // PUNPCKHQDQ XMM10, XMM12  /*  m[9],  m[1] */
	MOVOU X15, X11
	LONG $0x6c0f4566; BYTE $0xde // PUNPCKLQDQ XMM11, XMM14  /* m[12], m[14] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12            // X12 =  m[2]+ m[3]
	MOVOU 32(DX), X13            // X13 =  m[4]+ m[5]
	MOVOU 80(DX), X14            // X14 = m[10]+m[11]
	MOVOU 112(DX), X15           // X15 = m[14]+m[15]
	MOVOU X13, X9
	LONG $0x6d0f4566; BYTE $0xcd // PUNPCKHQDQ  XMM9, XMM13  /*   ___,  m[5] */
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc1 // PUNPCKLQDQ  XMM8,  XMM9  /*  m[2],  ____ */
	MOVOU X15, X10
	LONG $0x6d0f4566; BYTE $0xd7 // PUNPCKHQDQ XMM10, XMM15  /*   ___, m[15] */
	MOVOU X13, X9
	LONG $0x6c0f4566; BYTE $0xca // PUNPCKLQDQ  XMM9, XMM10  /*  m[4],  ____ */
	MOVOU 0(DX), X11             // X11 =  m[0]+ m[1]
	MOVOU 48(DX), X10            // X10 =  m[6]+ m[7]
	MOVOU 64(DX), X15            // X15 =  m[8]+ m[9]
	LONG $0x6c0f4566; BYTE $0xd6 // PUNPCKLQDQ XMM10, XMM14  /*  m[6], m[10] */
	LONG $0x6c0f4566; BYTE $0xdf // PUNPCKLQDQ XMM11, XMM15  /*  m[0],  m[8] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   5
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12            // X12 =  m[2]+ m[3]
	MOVOU 32(DX), X13            // X13 =  m[4]+ m[5]
	MOVOU 64(DX), X14            // X14 =  m[8]+ m[9]
	MOVOU 80(DX), X15            // X15 = m[10]+m[11]
	MOVOU X14, X8
	LONG $0x6d0f4566; BYTE $0xc5 // PUNPCKHQDQ  XMM8, XMM13  /*  m[9],  m[5] */
	MOVOU X12, X9
	LONG $0x6c0f4566; BYTE $0xcf // PUNPCKLQDQ  XMM9, XMM15  /*  m[2], m[10] */
	MOVOU 0(DX), X10             // X10 =  m[0]+ m[1]
	MOVOU 48(DX), X14            // X14 =  m[6]+ m[7]
	MOVOU 112(DX), X15           // X15 = m[14]+m[15]
	LONG $0x6d0f4566; BYTE $0xf6 // PUNPCKHQDQ XMM14, XMM14  /*   ___,  m[7] */
	LONG $0x6c0f4566; BYTE $0xd6 // PUNPCKLQDQ XMM10, XMM14  /*  m[0],  ____ */
	LONG $0x6d0f4566; BYTE $0xff // PUNPCKHQDQ XMM15, XMM15  /*   ___, m[15] */
	MOVOU X13, X11
	LONG $0x6c0f4566; BYTE $0xdf // PUNPCKLQDQ XMM11, XMM15  /*  m[4],  ____ */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12                          // X12 =  m[2]+ m[3]
	MOVOU 48(DX), X13                          // X13 =  m[6]+ m[7]
	MOVOU 80(DX), X14                          // X14 = m[10]+m[11]
	MOVOU 112(DX), X15                         // X15 = m[14]+m[15]
	LONG $0x6d0f4566; BYTE $0xf6               // PUNPCKHQDQ  XMM14, XMM14  /*   ___, m[11] */
	MOVOU X15, X8
	LONG $0x6c0f4566; BYTE $0xc6               // PUNPCKLQDQ  XMM8,  XMM14  /* m[14],  ____ */
	LONG $0x6d0f4566; BYTE $0xe4               // PUNPCKHQDQ  XMM12, XMM12  /*   ___,  m[3] */
	MOVOU X13, X9
	LONG $0x6c0f4566; BYTE $0xcc               // PUNPCKLQDQ  XMM9,  XMM12  /*  m[6],  ____ */
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 64(DX), X11                          // X11 =  m[8]+ m[9]
	MOVOU 96(DX), X14                          // X14 = m[12]+m[13]
	MOVOU X14, X10
	LONG $0x3a0f4566; WORD $0xd40f; BYTE $0x08 // PALIGNR    XMM10, XMM12, 0x8  /*  m[1], m[12] */
	LONG $0x6d0f4566; BYTE $0xf6               // PUNPCKHQDQ XMM14, XMM14  /*   ___, m[13] */
	LONG $0x6c0f4566; BYTE $0xde               // PUNPCKLQDQ XMM11, XMM14  /*  m[8],  ____ */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   6
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12             // X12 =  m[0]+ m[1]
	MOVOU 16(DX), X13            // X13 =  m[2]+ m[3]
	MOVOU 48(DX), X14            // X14 =  m[6]+ m[7]
	MOVOU 64(DX), X15            // X15 =  m[8]+ m[9]
	MOVOU X13, X8
	LONG $0x6c0f4566; BYTE $0xc6 // PUNPCKLQDQ  XMM8, XMM14  /*  m[2],  m[6] */
	MOVOU X12, X9
	LONG $0x6c0f4566; BYTE $0xcf // PUNPCKLQDQ  XMM9, XMM15  /*  m[0],  m[8] */
	MOVOU 80(DX), X12            // X12 = m[10]+m[11]
	MOVOU 96(DX), X10            // X10 = m[12]+m[13]
	LONG $0x6c0f4566; BYTE $0xd4 // PUNPCKLQDQ XMM10, XMM12  /* m[12], m[10] */
	MOVOU X12, X11
	LONG $0x6d0f4566; BYTE $0xdd // PUNPCKHQDQ XMM11, XMM13  /* m[11],  m[3] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12             // X12 =  m[0]+ m[1]
	MOVOU 32(DX), X13            // X13 =  m[4]+ m[5]
	MOVOU 48(DX), X14            // X14 =  m[6]+ m[7]
	MOVOU 112(DX), X15           // X15 = m[14]+m[15]
	MOVOU X14, X9
	LONG $0x6d0f4566; BYTE $0xce // PUNPCKHQDQ  XMM9, XMM14  /*   ___,  m[7] */
	MOVOU X13, X8
	LONG $0x6c0f4566; BYTE $0xc1 // PUNPCKLQDQ  XMM8,  XMM9  /*  m[4],  ____ */
	MOVOU X15, X9
	LONG $0x6d0f4566; BYTE $0xcc // PUNPCKHQDQ  XMM9, XMM12  /* m[15],  m[1] */
	MOVOU 64(DX), X12            // X12 =  m[8]+ m[9]
	MOVOU 96(DX), X10            // X10 = m[12]+m[13]
	LONG $0x6d0f4566; BYTE $0xd5 // PUNPCKHQDQ XMM10, XMM13  /* m[13],  m[5] */
	LONG $0x6d0f4566; BYTE $0xe4 // PUNPCKHQDQ XMM12, XMM12  /*   ___,  m[9] */
	MOVOU X15, X11
	LONG $0x6c0f4566; BYTE $0xdc // PUNPCKLQDQ XMM11, XMM12  /* m[14],  ____ */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   7
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 32(DX), X13                          // X13 =  m[4]+ m[5]
	MOVOU 96(DX), X14                          // X14 = m[12]+m[13]
	MOVOU 112(DX), X15                         // X15 = m[14]+m[15]
	MOVOU X12, X9
	LONG $0x6d0f4566; BYTE $0xcc               // PUNPCKHQDQ  XMM9, XMM12  /*   ___,  m[1] */
	MOVOU X14, X8
	LONG $0x6c0f4566; BYTE $0xc1               // PUNPCKLQDQ  XMM8,  XMM9  /* m[12],  ____ */
	MOVOU X15, X9
	LONG $0x6c0f4566; BYTE $0xcd               // PUNPCKLQDQ  XMM9, XMM13  /* m[14],  m[4] */
	MOVOU 80(DX), X11                          // X11 = m[10]+m[11]
	MOVOU X13, X10
	LONG $0x6d0f4566; BYTE $0xd7               // PUNPCKHQDQ XMM10, XMM15  /*  m[5], m[15] */
	LONG $0x3a0f4566; WORD $0xde0f; BYTE $0x08 // PALIGNR    XMM11, XMM14, 0x8  /* m[13], m[10] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 48(DX), X13                          // X13 =  m[6]+ m[7]
	MOVOU 64(DX), X14                          // X14 =  m[8]+ m[9]
	MOVOU 80(DX), X15                          // X15 = m[10]+m[11]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5               // PUNPCKLQDQ  XMM8, XMM13  /*  m[0],  m[6] */
	MOVOU X14, X9
	LONG $0x3a0f4566; WORD $0xce0f; BYTE $0x08 // PALIGNR     XMM9, XMM14, 0x8  /*  m[9],  m[8] */
	MOVOU 16(DX), X11                          // X14 =  m[2]+ m[3]
	MOVOU X13, X10
	LONG $0x6d0f4566; BYTE $0xd3               // PUNPCKHQDQ XMM10, XMM11  /*  m[7],  m[3] */
	LONG $0x6d0f4566; BYTE $0xff               // PUNPCKHQDQ XMM15, XMM15  /*   ___, m[11] */
	LONG $0x6c0f4566; BYTE $0xdf               // PUNPCKLQDQ XMM11, XMM15  /*  m[2],  ____ */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   8
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12                          // X12 =  m[2]+ m[3]
	MOVOU 48(DX), X13                          // X13 =  m[6]+ m[7]
	MOVOU 96(DX), X14                          // X14 = m[12]+m[13]
	MOVOU 112(DX), X15                         // X15 = m[14]+m[15]
	MOVOU X14, X8
	LONG $0x6d0f4566; BYTE $0xc5               // PUNPCKHQDQ  XMM8, XMM13  /* m[13],  m[7] */
	MOVOU X12, X10
	LONG $0x6d0f4566; BYTE $0xd4               // PUNPCKHQDQ XMM10, XMM12  /*   ___,  m[3] */
	MOVOU X14, X9
	LONG $0x6c0f4566; BYTE $0xca               // PUNPCKLQDQ  XMM9,  XMM10  /* m[12],  ____ */
	MOVOU 0(DX), X11                           // X11 =  m[0]+ m[1]
	MOVOU 64(DX), X13                          // X13 =  m[8]+ m[9]
	MOVOU 80(DX), X14                          // X14 = m[10]+m[11]
	MOVOU X15, X10
	LONG $0x3a0f4566; WORD $0xd60f; BYTE $0x08 // PALIGNR    XMM10, XMM14, 0x8  /* m[11], m[14] */
	LONG $0x6d0f4566; BYTE $0xdd               // PUNPCKHQDQ XMM11, XMM13  /*  m[1],  m[9] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12            // X12 =  m[2]+ m[3]
	MOVOU 32(DX), X13            // X13 =  m[4]+ m[5]
	MOVOU 64(DX), X14            // X14 =  m[8]+ m[9]
	MOVOU 112(DX), X15           // X15 = m[14]+m[15]
	MOVOU X13, X8
	LONG $0x6d0f4566; BYTE $0xc7 // PUNPCKHQDQ  XMM8, XMM15  /*  m[5], m[15] */
	MOVOU X14, X9
	LONG $0x6c0f4566; BYTE $0xcc // PUNPCKLQDQ  XMM9, XMM12  /*  m[8],  m[2] */
	MOVOU 0(DX), X10             // X10 =  m[0]+ m[1]
	MOVOU 48(DX), X11            // X11 =  m[6]+ m[7]
	MOVOU 80(DX), X15            // X15 = m[10]+m[11]
	LONG $0x6c0f4566; BYTE $0xd5 // PUNPCKLQDQ XMM10, XMM13  /*  m[0],  m[4] */
	LONG $0x6c0f4566; BYTE $0xdf // PUNPCKLQDQ XMM11, XMM15  /*  m[6], m[10] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   9
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12                            // X12 =  m[0]+ m[1]
	MOVOU 48(DX), X13                           // X13 =  m[6]+ m[7]
	MOVOU 80(DX), X14                           // X14 = m[10]+m[11]
	MOVOU 112(DX), X15                          // X15 = m[14]+m[15]
	MOVOU X13, X8
	LONG  $0x6c0f4566; BYTE $0xc7               // PUNPCKLQDQ  XMM8, XMM15  /*  m[6], m[14] */
	MOVOU X12, X9
	LONG  $0x3a0f4566; WORD $0xce0f; BYTE $0x08 // PALIGNR     XMM9, XMM14, 0x8  /* m[11],  m[0] */
	MOVOU 16(DX), X13                           // X13 =  m[2]+ m[3]
	MOVOU 64(DX), X11                           // X11 =  m[8]+ m[9]
	MOVOU X15, X10
	LONG  $0x6d0f4566; BYTE $0xd3               // PUNPCKHQDQ XMM10, XMM11  /* m[15],  m[9] */
	LONG  $0x3a0f4566; WORD $0xdd0f; BYTE $0x08 // PALIGNR    XMM11, XMM13, 0x8  /*  m[3],  m[8] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 16(DX), X13                          // X13 =  m[2]+ m[3]
	MOVOU 80(DX), X14                          // X14 = m[10]+m[11]
	MOVOU 96(DX), X15                          // X15 = m[12]+m[13]
	MOVOU X15, X9
	LONG $0x6d0f4566; BYTE $0xcf               // PUNPCKHQDQ  XMM9, XMM15  /*   ___, m[13] */
	MOVOU X15, X8
	LONG $0x6c0f4566; BYTE $0xc1               // PUNPCKLQDQ  XMM8,  XMM9  /* m[12],  ____ */
	MOVOU X14, X9
	LONG $0x3a0f4566; WORD $0xcc0f; BYTE $0x08 // PALIGNR     XMM9, XMM12, 0x8  /*  m[1], m[10] */
	MOVOU 32(DX), X12                          // X12 =  m[4]+ m[5]
	MOVOU 48(DX), X15                          // X15 =  m[6]+ m[7]
	MOVOU X15, X11
	LONG $0x6d0f4566; BYTE $0xdf               // PUNPCKHQDQ XMM11, XMM15  /*   ___,  m[7] */
	MOVOU X13, X10
	LONG $0x6c0f4566; BYTE $0xd3               // PUNPCKLQDQ XMM10, XMM11  /*  m[2],  ____ */
	MOVOU X12, X15
	LONG $0x6d0f4566; BYTE $0xfc               // PUNPCKHQDQ XMM15, XMM12  /*   ___,  m[5] */
	MOVOU X12, X11
	LONG $0x6c0f4566; BYTE $0xdf               // PUNPCKLQDQ XMM11, XMM15  /*  m[4],  ____ */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   1 0
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12             // X12 =  m[0]+ m[1]
	MOVOU 48(DX), X13            // X13 =  m[6]+ m[7]
	MOVOU 64(DX), X14            // X14 =  m[8]+ m[9]
	MOVOU 80(DX), X15            // X15 = m[10]+m[11]
	MOVOU X15, X8
	LONG $0x6c0f4566; BYTE $0xc6 // PUNPCKLQDQ  XMM8, XMM14  /* m[10],  m[8] */
	MOVOU X13, X9
	LONG $0x6d0f4566; BYTE $0xcc // PUNPCKHQDQ  XMM9, XMM12  /*  m[7],  m[1] */
	MOVOU 16(DX), X10            // X10 =  m[2]+ m[3]
	MOVOU 32(DX), X14            // X14 =  m[4]+ m[5]
	LONG $0x6c0f4566; BYTE $0xd6 // PUNPCKLQDQ XMM10, XMM14  /*  m[2],  m[4] */
	MOVOU X14, X15
	LONG $0x6d0f4566; BYTE $0xfe // PUNPCKHQDQ XMM15, XMM14  /*   ___,  m[5] */
	MOVOU X13, X11
	LONG $0x6c0f4566; BYTE $0xdf // PUNPCKLQDQ XMM11, XMM15  /*  m[6],  ____ */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 16(DX), X12                          // X12 =  m[2]+ m[3]
	MOVOU 64(DX), X13                          // X13 =  m[8]+ m[9]
	MOVOU 96(DX), X14                          // X14 = m[12]+m[13]
	MOVOU 112(DX), X15                         // X15 = m[14]+m[15]
	MOVOU X15, X8
	LONG $0x6d0f4566; BYTE $0xc5               // PUNPCKHQDQ  XMM8, XMM13  /* m[15],  m[9] */
	MOVOU X12, X9
	LONG $0x6d0f4566; BYTE $0xce               // PUNPCKHQDQ  XMM9, XMM14  /*  m[3], m[13] */
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 80(DX), X13                          // X13 = m[10]+m[11]
	MOVOU X15, X10
	LONG $0x3a0f4566; WORD $0xd50f; BYTE $0x08 // PALIGNR    XMM10, XMM13, 0x8  /* m[11], m[14] */
	MOVOU X14, X11
	LONG $0x6c0f4566; BYTE $0xdc               // PUNPCKLQDQ XMM11, XMM12  /* m[12],  m[0] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   1 1
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12             // X12 = m[0]+m[1]
	MOVOU 16(DX), X13            // X13 = m[2]+m[3]
	MOVOU 32(DX), X14            // X14 = m[4]+m[5]
	MOVOU 48(DX), X15            // X15 = m[6]+m[7]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5 // PUNPCKLQDQ  XMM8, XMM13  /* m[0], m[2] */
	MOVOU X14, X9
	LONG $0x6c0f4566; BYTE $0xcf // PUNPCKLQDQ  XMM9, XMM15  /* m[4], m[6] */
	MOVOU X12, X10
	LONG $0x6d0f4566; BYTE $0xd5 // PUNPCKHQDQ XMM10, XMM13  /* m[1], m[3] */
	MOVOU X14, X11
	LONG $0x6d0f4566; BYTE $0xdf // PUNPCKHQDQ XMM11, XMM15  /* m[5], m[7] */

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 64(DX), X12            // X12 =  m[8]+ m[9]
	MOVOU 80(DX), X13            // X13 = m[10]+m[11]
	MOVOU 96(DX), X14            // X14 = m[12]+m[13]
	MOVOU 112(DX), X15           // X15 = m[14]+m[15]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5 // PUNPCKLQDQ  XMM8, XMM13  /*  m[8],m[10] */
	MOVOU X14, X9
	LONG $0x6c0f4566; BYTE $0xcf // PUNPCKLQDQ  XMM9, XMM15  /* m[12],m[14] */
	MOVOU X12, X10
	LONG $0x6d0f4566; BYTE $0xd5 // PUNPCKHQDQ XMM10, XMM13  /*  m[9],m[11] */
	MOVOU X14, X11
	LONG $0x6d0f4566; BYTE $0xdf // PUNPCKHQDQ XMM11, XMM15  /* m[13],m[15] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	///////////////////////////////////////////////////////////////////////////
	// R O U N D   1 2
	///////////////////////////////////////////////////////////////////////////

	// LOAD_MSG_ ##r ##_1 / ##_2(b0, b1); (X12 is temp register)
	MOVOU 112(DX), X12                         // X12 = m[14]+m[15]
	MOVOU 32(DX), X13                          // X13 =  m[4]+ m[5]
	MOVOU 64(DX), X14                          // X14 =  m[8]+ m[9]
	MOVOU 96(DX), X15                          // X15 = m[12]+m[13]
	MOVOU X12, X8
	LONG $0x6c0f4566; BYTE $0xc5               // PUNPCKLQDQ  XMM8, XMM13  /* m[14],  m[4] */
	MOVOU X14, X9
	LONG $0x6d0f4566; BYTE $0xcf               // PUNPCKHQDQ  XMM9, XMM15  /*  m[9], m[13] */
	MOVOU 80(DX), X10                          // X10 = m[10]+m[11]
	MOVOU 48(DX), X11                          // X11 =  m[6]+ m[7]
	LONG $0x6c0f4566; BYTE $0xd6               // PUNPCKLQDQ XMM10, XMM14  /* m[10],  m[8] */
	LONG $0x3a0f4566; WORD $0xdc0f; BYTE $0x08 // PALIGNR    XMM11, XMM12, 0x8  /* m[15],  m[6] */; ; ; ; ;

	LOAD_SHUFFLE
	G1
	G2
	DIAGONALIZE

	// LOAD_MSG_ ##r ##_3 / ##_4(b0, b1); (X12 is temp register)
	MOVOU 0(DX), X12                           // X12 =  m[0]+ m[1]
	MOVOU 32(DX), X13                          // X13 =  m[4]+ m[5]
	MOVOU 80(DX), X14                          // X14 = m[10]+m[11]
	MOVOU X12, X8
	LONG $0x3a0f4566; WORD $0xc40f; BYTE $0x08 // PALIGNR     XMM8, XMM12, 0x8  /*  m[1],  m[0] */
	MOVOU X14, X9
	LONG $0x6d0f4566; BYTE $0xcd               // PUNPCKHQDQ  XMM9, XMM13  /* m[11], m[5] */
	MOVOU 16(DX), X12                          // X12 =  m[2]+ m[3]
	MOVOU 48(DX), X11                          // X11 =  m[6]+ m[7]
	MOVOU 96(DX), X10                          // X10 = m[12]+m[13]
	LONG $0x6c0f4566; BYTE $0xd4               // PUNPCKLQDQ XMM10, XMM12  /* m[12], m[2] */
	LONG $0x6d0f4566; BYTE $0xdc               // PUNPCKHQDQ XMM11, XMM12  /*  m[7], m[3] */

	LOAD_SHUFFLE
	G1
	G2
	UNDIAGONALIZE

	// Reload digest (most current value store in &out)
	MOVQ  out+144(FP), SI // SI: &in
	MOVOU 0(SI), X12      // X12 = in[0]+in[1]      /* row1l = LOAD( &S->h[0] ); */
	MOVOU 16(SI), X13     // X13 = in[2]+in[3]      /* row1h = LOAD( &S->h[2] ); */
	MOVOU 32(SI), X14     // X14 = in[4]+in[5]      /* row2l = LOAD( &S->h[4] ); */
	MOVOU 48(SI), X15     // X15 = in[6]+in[7]      /* row2h = LOAD( &S->h[6] ); */

	// Final computations and prepare for storing
	PXOR X4, X0  // X0 = X0 ^ X4          /* row1l = _mm_xor_si128( row3l, row1l ); */
	PXOR X5, X1  // X1 = X1 ^ X5          /* row1h = _mm_xor_si128( row3h, row1h ); */
	PXOR X12, X0 // X0 = X0 ^ X12         /*  STORE( &S->h[0], _mm_xor_si128( LOAD( &S->h[0] ), row1l ) ); */
	PXOR X13, X1 // X1 = X1 ^ X13         /*  STORE( &S->h[2], _mm_xor_si128( LOAD( &S->h[2] ), row1h ) ); */
	PXOR X6, X2  // X2 = X2 ^ X6          /*  row2l = _mm_xor_si128( row4l, row2l ); */
	PXOR X7, X3  // X3 = X3 ^ X7          /*  row2h = _mm_xor_si128( row4h, row2h ); */
	PXOR X14, X2 // X2 = X2 ^ X14         /*  STORE( &S->h[4], _mm_xor_si128( LOAD( &S->h[4] ), row2l ) ); */
	PXOR X15, X3 // X3 = X3 ^ X15         /*  STORE( &S->h[6], _mm_xor_si128( LOAD( &S->h[6] ), row2h ) ); */

	// Store digest into &out
	MOVQ  out+144(FP), SI // SI: &out
	MOVOU X0, 0(SI)       // out[0]+out[1] = X0
	MOVOU X1, 16(SI)      // out[2]+out[3] = X1
	MOVOU X2, 32(SI)      // out[4]+out[5] = X2
	MOVOU X3, 48(SI)      // out[6]+out[7] = X3

	// Increment message pointer and check if there's more to do
	ADDQ $128, DX // message += 128
	SUBQ $1, R8
	JNZ  loop

complete:
	RET
