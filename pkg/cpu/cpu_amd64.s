// Copyright (c) 2015 Klaus Post, released under MIT License. See LICENSE file.
//
// See https://github.com/klauspost/cpuid/blob/master/LICENSE
//
// Using this inside Minio with modifications
//

// func cpuid(op uint32) (eax, ebx, ecx, edx uint32)
TEXT ·cpuid(SB),7,$0
        MOVL op+0(FP),AX
        CPUID
        MOVL AX,eax+8(FP)
        MOVL BX,ebx+12(FP)
        MOVL CX,ecx+16(FP)
        MOVL DX,edx+20(FP)
        RET

// func cpuidex(op, op2 uint32) (eax, ebx, ecx, edx uint32)
TEXT ·cpuidex(SB),7,$0
        MOVL op+0(FP),AX
        MOVL op2+4(FP),CX
        CPUID
        MOVL AX,eax+8(FP)
        MOVL BX,ebx+12(FP)
        MOVL CX,ecx+16(FP)
        MOVL DX,edx+20(FP)
        RET
