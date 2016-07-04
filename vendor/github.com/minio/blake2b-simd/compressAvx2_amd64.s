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
// Based on AVX2 implementation from https://github.com/sneves/blake2-avx2/blob/master/blake2b-common.h
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
    BYTE $0xc5; BYTE $0xfd; BYTE $0xd4; BYTE $0xc4             \ // VPADDQ  YMM0,YMM0,YMM4   /* v0 += m[0], v1 += m[2], v2 += m[4], v3 += m[6] */
    BYTE $0xc5; BYTE $0xfd; BYTE $0xd4; BYTE $0xc1             \ // VPADDQ  YMM0,YMM0,YMM1   /* v0 += v4, v1 += v5, v2 += v6, v3 += v7 */
    BYTE $0xc5; BYTE $0xe5; BYTE $0xef; BYTE $0xd8             \ // VPXOR   YMM3,YMM3,YMM0   /* v12 ^= v0, v13 ^= v1, v14 ^= v2, v15 ^= v3 */
    BYTE $0xc5; BYTE $0xfd; BYTE $0x70; BYTE $0xdb; BYTE $0xb1 \ // VPSHUFD YMM3,YMM3,0xb1   /* v12 = v12<<(64-32) | v12>>32, v13 = */
    BYTE $0xc5; BYTE $0xed; BYTE $0xd4; BYTE $0xd3             \ // VPADDQ  YMM2,YMM2,YMM3   /* v8 += v12, v9 += v13, v10 += v14, v11 += v15 */
    BYTE $0xc5; BYTE $0xf5; BYTE $0xef; BYTE $0xca             \ // VPXOR   YMM1,YMM1,YMM2   /* v4 ^= v8, v5 ^= v9, v6 ^= v10, v7 ^= v11 */
    BYTE $0xc4; BYTE $0xe2; BYTE $0x75; BYTE $0x00; BYTE $0xce   // VPSHUFB YMM1,YMM1,YMM6   /* v4 = v4<<(64-24) | v4>>24, ..., ..., v7 = v7<<(64-24) | v7>>24 */

#define G2 \
    BYTE $0xc5; BYTE $0xfd; BYTE $0xd4; BYTE $0xc5             \ // VPADDQ  YMM0,YMM0,YMM5   /* v0 += m[1], v1 += m[3], v2 += m[5], v3 += m[7] */
    BYTE $0xc5; BYTE $0xfd; BYTE $0xd4; BYTE $0xc1             \ // VPADDQ  YMM0,YMM0,YMM1   /* v0 += v4, v1 += v5, v2 += v6, v3 += v7 */
    BYTE $0xc5; BYTE $0xe5; BYTE $0xef; BYTE $0xd8             \ // VPXOR   YMM3,YMM3,YMM0   /* v12 ^= v0, v13 ^= v1, v14 ^= v2, v15 ^= v3 */
    BYTE $0xc4; BYTE $0xe2; BYTE $0x65; BYTE $0x00; BYTE $0xdf \ // VPSHUFB YMM3,YMM3,YMM7   /* v12 = v12<<(64-16) | v12>>16, ..., ..., v15 = v15<<(64-16) | v15>>16 */
    BYTE $0xc5; BYTE $0xed; BYTE $0xd4; BYTE $0xd3             \ // VPADDQ  YMM2,YMM2,YMM3   /* v8 += v12, v9 += v13, v10 += v14, v11 += v15 */
    BYTE $0xc5; BYTE $0xf5; BYTE $0xef; BYTE $0xca             \ // VPXOR   YMM1,YMM1,YMM2   /* v4 ^= v8, v5 ^= v9, v6 ^= v10, v7 ^= v11 */
    BYTE $0xc5; BYTE $0x75; BYTE $0xd4; BYTE $0xf9             \ // VPADDQ  YMM15,YMM1,YMM1  /* temp reg = reg*2   */
    BYTE $0xc5; BYTE $0xf5; BYTE $0x73; BYTE $0xd1; BYTE $0x3f \ // VPSRLQ  YMM1,YMM1,0x3f   /*      reg = reg>>63 */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x75; BYTE $0xef; BYTE $0xcf   // VPXOR   YMM1,YMM1,YMM15  /* ORed together: v4 = v4<<(64-63) | v4>>63, v5 = v5<<(64-63) | v5>>63 */

#define DIAGONALIZE \
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xdb \ // VPERMQ YMM3, YMM3, 0x93
                BYTE $0x93                                     \
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xd2 \ // VPERMQ YMM2, YMM2, 0x4e
                BYTE $0x4e                                     \
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xc9 \ // VPERMQ YMM1, YMM1, 0x39
                BYTE $0x39                                     \
    // DO NOT DELETE -- macro delimiter (previous line extended)

#define UNDIAGONALIZE \
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xdb \ // VPERMQ YMM3, YMM3, 0x39
                BYTE $0x39                                     \
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xd2 \ // VPERMQ YMM2, YMM2, 0x4e
                BYTE $0x4e                                     \
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xc9 \ // VPERMQ YMM1, YMM1, 0x93
                BYTE $0x93                                     \
    // DO NOT DELETE -- macro delimiter (previous line extended)

#define LOAD_SHUFFLE \
    MOVQ   shffle+120(FP), SI \ // SI: &shuffle
    BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x36             \ // VMOVDQU YMM6, [rsi]
    BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x7e; BYTE $0x20   // VMOVDQU YMM7, 32[rsi]

// func compressAVX2Loop(compressSSE(p []uint8, in, iv, t, f, shffle, out []uint64)
TEXT ·compressAVX2Loop(SB), 7, $0

    // REGISTER USE
    //  Y0 - Y3: v0 - v15
    //  Y4 - Y5: m[0] - m[7]
    //  Y6 - Y7: shuffle value
    //  Y8 - Y9: temp registers
    // Y10 -Y13: copy of full message
    //      Y15: temp register

    // Load digest
    MOVQ   in+24(FP),  SI     // SI: &in
    BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x06               // VMOVDQU YMM0, [rsi]
    BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x4e; BYTE $0x20   // VMOVDQU YMM1, 32[rsi]

    // Already store digest into &out (so we can reload it later generically)
    MOVQ  out+144(FP), SI     // SI: &out
    BYTE $0xc5; BYTE $0xfe; BYTE $0x7f; BYTE $0x06               // VMOVDQU [rsi], YMM0
    BYTE $0xc5; BYTE $0xfe; BYTE $0x7f; BYTE $0x4e; BYTE $0x20   // VMOVDQU 32[rsi], YMM1

    // Initialize message pointer and loop counter
    MOVQ   message+0(FP), DX  // DX: &p (message)
    MOVQ   message_len+8(FP), R8 // R8: len(message)
    SHRQ   $7, R8             // len(message) / 128
    CMPQ   R8, $0
    JEQ    complete

loop:
    // Increment counter
    MOVQ t+72(FP), SI         // SI: &t
    MOVQ   0(SI), R9          //
    ADDQ   $128, R9           //                       /* d.t[0] += BlockSize */
    MOVQ   R9, 0(SI)          //
    CMPQ   R9, $128           //                       /* if d.t[0] < BlockSize { */
    JGE    noincr             //
    MOVQ   8(SI), R9          //
    ADDQ   $1, R9             //                       /*     d.t[1]++ */
    MOVQ   R9, 8(SI)          //
noincr:                       //                       /* } */

    // Load initialization vector
    MOVQ iv+48(FP), SI        // SI: &iv
    BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x16               // VMOVDQU YMM2, [rsi]
    BYTE $0xc5; BYTE $0xfe; BYTE $0x6f; BYTE $0x5e; BYTE $0x20   // VMOVDQU YMM3, 32[rsi]
    MOVQ t+72(FP), SI         // SI: &t
    BYTE $0xc4; BYTE $0x63; BYTE $0x3d; BYTE $0x38; BYTE $0x06   // VINSERTI128 YMM8, YMM8, [rsi], 0  /* Y8 = t[0]+t[1] */
                BYTE $0x00
    MOVQ t+96(FP), SI         // SI: &f
    BYTE $0xc4; BYTE $0x63; BYTE $0x3d; BYTE $0x38; BYTE $0x06   // VINSERTI128 YMM8, YMM8, [rsi], 1  /* Y8 = t[0]+t[1]+f[0]+f[1] */
                BYTE $0x01
    BYTE $0xc4; BYTE $0xc1; BYTE $0x65; BYTE $0xef; BYTE $0xd8   // VPXOR   YMM3,YMM3,YMM8            /* Y3 = Y3 ^ Y8 */

    BYTE $0xc5; BYTE $0x7e; BYTE $0x6f; BYTE $0x12               // VMOVDQU YMM10, [rdx]              /* Y10 =  m[0]+ m[1]+ m[2]+ m[3] */
    BYTE $0xc5; BYTE $0x7e; BYTE $0x6f; BYTE $0x5a; BYTE $0x20   // VMOVDQU YMM11, 32[rdx]            /* Y11 =  m[4]+ m[5]+ m[6]+ m[7] */
    BYTE $0xc5; BYTE $0x7e; BYTE $0x6f; BYTE $0x62; BYTE $0x40   // VMOVDQU YMM12, 64[rdx]            /* Y12 =  m[8]+ m[9]+m[10]+m[11] */
    BYTE $0xc5; BYTE $0x7e; BYTE $0x6f; BYTE $0x6a; BYTE $0x60   // VMOVDQU YMM13, 96[rdx]            /* Y13 = m[12]+m[13]+m[14]+m[15] */

    LOAD_SHUFFLE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   1
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0xc1; BYTE $0x2d; BYTE $0x6c; BYTE $0xe3   // VPUNPCKLQDQ  YMM4, YMM10, YMM11 /* m[0], m[4], m[2], m[6] */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x2d; BYTE $0x6d; BYTE $0xeb   // VPUNPCKHQDQ  YMM5, YMM10, YMM11 /* m[1], m[5], m[3], m[7] */
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xe4   // VPERMQ       YMM4, YMM4, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xed   // VPERMQ       YMM5, YMM5, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0xc1; BYTE $0x1d; BYTE $0x6c; BYTE $0xe5   // VPUNPCKLQDQ  YMM4, YMM12, YMM13 /* m[8], m[12], m[10], m[14] */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x1d; BYTE $0x6d; BYTE $0xed   // VPUNPCKHQDQ  YMM5, YMM12, YMM13 /* m[9], m[13], m[11], m[15] */
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xe4   // VPERMQ       YMM4, YMM4, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xed   // VPERMQ       YMM5, YMM5, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   2
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc5   // VPUNPCKLQDQ  YMM8, YMM11, YMM13        /*  m[4],  ____,  ____, m[14] */
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8,  YMM8, 0x03         /* m[14],  m[4],  ____,  ____ */ /* xxxx 0011 = 0x03 */
                BYTE $0x03
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xcd   // VPUNPCKHQDQ  YMM9, YMM12, YMM13        /*  m[9], m[13],  ____,  ____ */
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4,  YMM8,  YMM9, 0x20  /*  m[9], m[13],  ____,  ____ */ /* 0010 0000 = 0x20 */
                BYTE $0x20

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc4   // VPERMQ       YMM8,  YMM12, 0x02        /* m[10],  m[8],  ____,  ____ */ /* xxxx 0010 = 0x02 */
                BYTE $0x02
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9,  YMM13, 0x30        /*  ____,  ____, m[15],  ____ */ /* xx11 xxxx = 0x30 */
                BYTE $0x30
    BYTE $0xc4; BYTE $0x41; BYTE $0x35; BYTE $0x6c; BYTE $0xcb   // VPUNPCKLQDQ  YMM9,   YMM9, YMM11       /*  ____,  ____, m[15],  m[6] */
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5,   YMM8, YMM9, 0x30  /*  m[9], m[13], m[15],  m[6] */ /* 0011 0000 = 0x30 */
                BYTE $0x30

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc2   // VPERMQ       YMM8, YMM10, 0x01         /*  m[1],  m[0],  ____,  ____ */ /* xxxx 0001 = 0x01 */
                BYTE $0x01
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xcc   // VPUNPCKHQDQ  YMM9, YMM11, YMM12        /*  m[5],  ____,  ____, m[11] */
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9,  YMM9, 0x03         /* m[11],  m[5],  ____,  ____ */ /* xxxx 0011 = 0x03 */
                BYTE $0x03
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4,  YMM8, YMM9, 0x20   /*  m[1],  m[0], m[11],  m[5] */ /* 0010 0000 = 0x20 */
                BYTE $0x20

    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc5   // VPUNPCKLQDQ  YMM8, YMM10, YMM13        /*  ___,  m[12],  m[2],  ____ */
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8,  YMM8, 0x09         /* m[12],  m[2],  ____,  ____ */ /* xxxx 1001 = 0x09 */
                BYTE $0x09
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xca   // VPUNPCKHQDQ  YMM9, YMM11, YMM10        /*  ____,  ____,  m[7],  m[3] */
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5,  YMM8, YMM9, 0x30   /*  m[9], m[13], m[15],  m[6] */ /* 0011 0000 = 0x30 */
                BYTE $0x30

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   3
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc5   // VPERMQ       YMM8, YMM13, 0x00
                BYTE $0x00
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xc0   // VPUNPCKHQDQ  YMM8, YMM12, YMM8
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xcd   // VPUNPCKHQDQ  YMM9, YMM11, YMM13
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x21
                BYTE $0x21

    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6c; BYTE $0xc2   // VPUNPCKLQDQ  YMM8, YMM12, YMM10
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9, YMM13, 0x55
                BYTE $0x55
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM10, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x30
                BYTE $0x30

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc2   // VPERMQ       YMM8, YMM10, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6c; BYTE $0xc0   // VPUNPCKLQDQ  YMM8, YMM12, YMM8
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xcc   // VPUNPCKHQDQ  YMM9, YMM11, YMM12
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x31
                BYTE $0x31

    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xc3   // VPUNPCKLQDQ  YMM8, YMM13, YMM11
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcb   // VPERMQ       YMM9, YMM11, 0x00
                BYTE $0x00
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6d; BYTE $0xc9   // VPUNPCKHQDQ  YMM9, YMM10, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x21
                BYTE $0x21

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   4
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xc2   // VPUNPCKHQDQ  YMM8, YMM11, YMM10
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xcc   // VPUNPCKHQDQ  YMM9, YMM13, YMM12
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x21
                BYTE $0x21

    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xc2   // VPUNPCKHQDQ  YMM8, YMM12, YMM10
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9, YMM13, 0x08
                BYTE $0x08
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x20
                BYTE $0x20

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc3   // VPERMQ       YMM8, YMM11, 0x55
                BYTE $0x55
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc0   // VPUNPCKLQDQ  YMM8, YMM10, YMM8
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9, YMM13, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM11, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x21
                BYTE $0x21

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc4   // VPUNPCKLQDQ  YMM8, YMM11, YMM12
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xcc   // VPUNPCKLQDQ  YMM9, YMM10, YMM12
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x21
                BYTE $0x21

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   5
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xc3   // VPUNPCKHQDQ  YMM8, YMM12, YMM11
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xcc   // VPUNPCKLQDQ  YMM9, YMM10, YMM12
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x30
                BYTE $0x30

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc3   // VPERMQ       YMM8, YMM11, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc0   // VPUNPCKLQDQ  YMM8, YMM10, YMM8
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9, YMM13, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM11, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x20
                BYTE $0x20

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc4   // VPERMQ       YMM8, YMM12, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xc0   // VPUNPCKLQDQ  YMM8, YMM13, YMM8
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xca   // VPERMQ       YMM9, YMM10, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM11, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x31
                BYTE $0x31

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc5   // VPERMQ       YMM8, YMM13, 0x00
                BYTE $0x00
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6d; BYTE $0xc0   // VPUNPCKHQDQ  YMM8, YMM10, YMM8
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9, YMM13, 0x55
                BYTE $0x55
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM12, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x20
                BYTE $0x20

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   6
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc3   // VPUNPCKLQDQ  YMM8, YMM10, YMM11
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xcc   // VPUNPCKLQDQ  YMM9, YMM10, YMM12
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x21
                BYTE $0x21

    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xc4   // VPUNPCKLQDQ  YMM8, YMM13, YMM12
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xca   // VPUNPCKHQDQ  YMM9, YMM12, YMM10
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x30
                BYTE $0x30

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc3   // VPERMQ       YMM8, YMM11, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xca   // VPUNPCKHQDQ  YMM9, YMM13, YMM10
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x30
                BYTE $0x30

    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xc3   // VPUNPCKHQDQ  YMM8, YMM13, YMM11
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcc   // VPERMQ       YMM9, YMM12, 0x55
                BYTE $0x55
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM13, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x30
                BYTE $0x30

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   7
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc2   // VPERMQ       YMM8, YMM10, 0x55
                BYTE $0x55
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xc0   // VPUNPCKLQDQ  YMM8, YMM13, YMM8
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xcb   // VPUNPCKLQDQ  YMM9, YMM13, YMM11
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x30
                BYTE $0x30

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xc5   // VPUNPCKHQDQ  YMM8, YMM11, YMM13
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcc   // VPERMQ       YMM9, YMM12, 0xaa
                BYTE $0xaa
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xc9   // VPUNPCKHQDQ  YMM9, YMM13, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x20
                BYTE $0x20

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc3   // VPUNPCKLQDQ  YMM8, YMM10, YMM11
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcc   // VPERMQ       YMM9, YMM12, 0x01
                BYTE $0x01
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x20
                BYTE $0x20

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xc2   // VPUNPCKHQDQ  YMM8, YMM11, YMM10
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcc   // VPERMQ       YMM9, YMM12, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM10, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x31
                BYTE $0x31

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   8
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xc3   // VPUNPCKHQDQ  YMM8, YMM13, YMM11
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xca   // VPERMQ       YMM9, YMM10, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xc9   // VPUNPCKLQDQ  YMM9, YMM13, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x20
                BYTE $0x20

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc5   // VPERMQ       YMM8, YMM13, 0xaa
                BYTE $0xaa
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xc0   // VPUNPCKHQDQ  YMM8, YMM12, YMM8
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6d; BYTE $0xcc   // VPUNPCKHQDQ  YMM9, YMM10, YMM12
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x21
                BYTE $0x21

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xc5   // VPUNPCKHQDQ  YMM8, YMM11, YMM13
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6c; BYTE $0xca   // VPUNPCKLQDQ  YMM9, YMM12, YMM10
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x0c
                BYTE $0x0c
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x20
                BYTE $0x20

    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc3   // VPUNPCKLQDQ  YMM8, YMM10, YMM11
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xcc   // VPUNPCKLQDQ  YMM9, YMM11, YMM12
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x30
                BYTE $0x30

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   9
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc5   // VPUNPCKLQDQ  YMM8, YMM11, YMM13
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xca   // VPERMQ       YMM9, YMM10, 0x00
                BYTE $0x00
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xc9   // VPUNPCKHQDQ  YMM9, YMM12, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x31
                BYTE $0x31

    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xc4   // VPUNPCKHQDQ  YMM8, YMM13, YMM12
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcc   // VPERMQ       YMM9, YMM12, 0x00
                BYTE $0x00
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6d; BYTE $0xc9   // VPUNPCKHQDQ  YMM9, YMM10, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x31
                BYTE $0x31

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcc   // VPERMQ       YMM9, YMM12, 0xaa
                BYTE $0xaa
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6d; BYTE $0xc9   // VPUNPCKHQDQ  YMM9, YMM10, YMM9
    BYTE $0xc4; BYTE $0xc3; BYTE $0x15; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM13, YMM9, 0x20
                BYTE $0x20

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc3   // VPERMQ       YMM8, YMM11, 0xff
                BYTE $0xff
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc0   // VPUNPCKLQDQ  YMM8, YMM10, YMM8
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcb   // VPERMQ       YMM9, YMM11, 0x04
                BYTE $0x04
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x21
                BYTE $0x21

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   10
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc4   // VPERMQ       YMM8, YMM12, 0x20
                BYTE $0x20
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xca   // VPUNPCKHQDQ  YMM9, YMM11, YMM10
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x31
                BYTE $0x31

    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc3   // VPUNPCKLQDQ  YMM8, YMM10, YMM11
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcb   // VPERMQ       YMM9, YMM11, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x31
                BYTE $0x31

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6d; BYTE $0xc4   // VPUNPCKHQDQ  YMM8, YMM13, YMM12
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8, YMM8, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6d; BYTE $0xcd   // VPUNPCKHQDQ  YMM9, YMM10, YMM13
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9, YMM9, 0x60
                BYTE $0x60
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4, YMM8, YMM9, 0x31
                BYTE $0x31

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc5   // VPERMQ       YMM8, YMM13, 0xaa
                BYTE $0xaa
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xc0   // VPUNPCKHQDQ  YMM8, YMM12, YMM8
    BYTE $0xc4; BYTE $0x41; BYTE $0x15; BYTE $0x6c; BYTE $0xca   // VPUNPCKLQDQ  YMM9, YMM13, YMM10
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5, YMM8, YMM9, 0x21
                BYTE $0x21
                                                                 
    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   1 1
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0xc1; BYTE $0x2d; BYTE $0x6c; BYTE $0xe3   // VPUNPCKLQDQ  YMM4, YMM10, YMM11 /* m[0], m[4], m[2], m[6] */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x2d; BYTE $0x6d; BYTE $0xeb   // VPUNPCKHQDQ  YMM5, YMM10, YMM11 /* m[1], m[5], m[3], m[7] */
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xe4   // VPERMQ       YMM4, YMM4, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xed   // VPERMQ       YMM5, YMM5, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0xc1; BYTE $0x1d; BYTE $0x6c; BYTE $0xe5   // VPUNPCKLQDQ  YMM4, YMM12, YMM13 /* m[8], m[12], m[10], m[14] */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x1d; BYTE $0x6d; BYTE $0xed   // VPUNPCKHQDQ  YMM5, YMM12, YMM13 /* m[9], m[13], m[11], m[15] */
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xe4   // VPERMQ       YMM4, YMM4, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8
    BYTE $0xc4; BYTE $0xe3; BYTE $0xfd; BYTE $0x00; BYTE $0xed   // VPERMQ       YMM5, YMM5, 0xd8   /* 0x1101 1000 = 0xd8 */
                BYTE $0xd8

    G1
    G2

    UNDIAGONALIZE

    ///////////////////////////////////////////////////////////////////////////
    // R O U N D   1 2
    ///////////////////////////////////////////////////////////////////////////

    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6c; BYTE $0xc5   // VPUNPCKLQDQ  YMM8, YMM11, YMM13        /*  m[4],  ____,  ____, m[14] */
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8,  YMM8, 0x03         /* m[14],  m[4],  ____,  ____ */ /* xxxx 0011 = 0x03 */
                BYTE $0x03
    BYTE $0xc4; BYTE $0x41; BYTE $0x1d; BYTE $0x6d; BYTE $0xcd   // VPUNPCKHQDQ  YMM9, YMM12, YMM13        /*  m[9], m[13],  ____,  ____ */
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4,  YMM8,  YMM9, 0x20  /*  m[9], m[13],  ____,  ____ */ /* 0010 0000 = 0x20 */
                BYTE $0x20

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc4   // VPERMQ       YMM8,  YMM12, 0x02        /* m[10],  m[8],  ____,  ____ */ /* xxxx 0010 = 0x02 */
                BYTE $0x02
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xcd   // VPERMQ       YMM9,  YMM13, 0x30        /*  ____,  ____, m[15],  ____ */ /* xx11 xxxx = 0x30 */
                BYTE $0x30
    BYTE $0xc4; BYTE $0x41; BYTE $0x35; BYTE $0x6c; BYTE $0xcb   // VPUNPCKLQDQ  YMM9,   YMM9, YMM11       /*  ____,  ____, m[15],  m[6] */
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5,   YMM8, YMM9, 0x30  /*  m[9], m[13], m[15],  m[6] */ /* 0011 0000 = 0x30 */
                BYTE $0x30

    G1
    G2

    DIAGONALIZE

    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc2   // VPERMQ       YMM8, YMM10, 0x01         /*  m[1],  m[0],  ____,  ____ */ /* xxxx 0001 = 0x01 */
                BYTE $0x01
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xcc   // VPUNPCKHQDQ  YMM9, YMM11, YMM12        /*  m[5],  ____,  ____, m[11] */
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc9   // VPERMQ       YMM9,  YMM9, 0x03         /* m[11],  m[5],  ____,  ____ */ /* xxxx 0011 = 0x03 */
                BYTE $0x03
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe1   // VPERM2I128   YMM4,  YMM8, YMM9, 0x20   /*  m[1],  m[0], m[11],  m[5] */ /* 0010 0000 = 0x20 */
                BYTE $0x20

    BYTE $0xc4; BYTE $0x41; BYTE $0x2d; BYTE $0x6c; BYTE $0xc5   // VPUNPCKLQDQ  YMM8, YMM10, YMM13        /*  ___,  m[12],  m[2],  ____ */
    BYTE $0xc4; BYTE $0x43; BYTE $0xfd; BYTE $0x00; BYTE $0xc0   // VPERMQ       YMM8,  YMM8, 0x09         /* m[12],  m[2],  ____,  ____ */ /* xxxx 1001 = 0x09 */
                BYTE $0x09
    BYTE $0xc4; BYTE $0x41; BYTE $0x25; BYTE $0x6d; BYTE $0xca   // VPUNPCKHQDQ  YMM9, YMM11, YMM10        /*  ____,  ____,  m[7],  m[3] */
    BYTE $0xc4; BYTE $0xc3; BYTE $0x3d; BYTE $0x46; BYTE $0xe9   // VPERM2I128   YMM5,  YMM8, YMM9, 0x30   /*  m[9], m[13], m[15],  m[6] */ /* 0011 0000 = 0x30 */
                BYTE $0x30

    G1
    G2

    UNDIAGONALIZE

    // Reload digest (most current value store in &out)
    MOVQ  out+144(FP),  SI    // SI: &in
    BYTE $0xc5; BYTE $0x7e; BYTE $0x6f; BYTE $0x26               // VMOVDQU YMM12, [rsi]
    BYTE $0xc5; BYTE $0x7e; BYTE $0x6f; BYTE $0x6e; BYTE $0x20   // VMOVDQU YMM13, 32[rsi]

    BYTE $0xc5; BYTE $0xfd; BYTE $0xef; BYTE $0xc2               // VPXOR   YMM0,YMM0,YMM2   /* X0 = X0 ^ X4,  X1 = X1 ^ X5 */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x7d; BYTE $0xef; BYTE $0xc4   // VPXOR   YMM0,YMM0,YMM12  /* X0 = X0 ^ X12, X1 = X1 ^ X13 */
    BYTE $0xc5; BYTE $0xf5; BYTE $0xef; BYTE $0xcb               // VPXOR   YMM1,YMM1,YMM3   /* X2 = X2 ^ X6,  X3 = X3 ^ X7 */
    BYTE $0xc4; BYTE $0xc1; BYTE $0x75; BYTE $0xef; BYTE $0xcd   // VPXOR   YMM1,YMM1,YMM13  /* X2 = X2 ^ X14, X3 = X3 ^ X15 */

    // Store digest into &out
    MOVQ  out+144(FP), SI     // SI: &out
    BYTE $0xc5; BYTE $0xfe; BYTE $0x7f; BYTE $0x06               // VMOVDQU [rsi], YMM0
    BYTE $0xc5; BYTE $0xfe; BYTE $0x7f; BYTE $0x4e; BYTE $0x20   // VMOVDQU 32[rsi], YMM1

    // Increment message pointer and check if there's more to do
    ADDQ   $128, DX           // message += 128
    SUBQ   $1, R8
    JNZ    loop

complete:
    BYTE $0xc5; BYTE $0xf8; BYTE $0x77                           // VZEROUPPER  /* Prevent further context switches */
    RET

