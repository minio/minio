;---------------------
;   https://software.intel.com/en-us/articles/improving-the-performance-of-the-secure-hash-algorithm-1
;
; License information:
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  This implementation notably advances the performance of SHA-1 algorithm compared to existing
;  implementations. We are encouraging all projects utilizing SHA-1 to integrate this new fast
;  implementation and are ready to help if issues or concerns arise (you are welcome to leave
;  a comment or write an email to the authors). It is provided 'as is' and free for either
;  commercial or non-commercial use.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;   This code implements two interfaces of SHA-1 update function: 1) working on a single
;   64-byte block and 2) working on a buffer of multiple 64-bit blocks. Multiple blocks
;   version of code is software pipelined and faster overall, it is a default. Assemble
;   with -DINTEL_SHA1_SINGLEBLOCK to select single 64-byte block function interface.
;
;   C++ prototypes of implemented functions are below:
;
;   #ifndef INTEL_SHA1_SINGLEBLOCK
;      // Updates 20-byte SHA-1 record in 'hash' for 'num_blocks' consequtive 64-byte blocks
;      extern "C" void sha1_update_intel(int *hash, const char* input, size_t num_blocks );
;   #else
;      // Updates 20-byte SHA-1 record in 'hash' for one 64-byte block pointed by 'input'
;      extern "C" void sha1_update_intel(int *hash, const char* input);
;   #endif
;
;   Function name 'sha1_update_intel' can be changed in the source or via macro:
;     -DINTEL_SHA1_UPDATE_FUNCNAME=my_sha1_update_func_name
;
;   It implements both UNIX(default) and Windows ABIs, use -DWIN_ABI on Windows
;
;   Code checks CPU for SSSE3 support via CPUID feature flag (CPUID.1.ECX.SSSE3[bit 9]==1),
;   and performs dispatch. Since in most cases the functionality on non-SSSE3 supporting CPUs
;   is also required, the default (e.g. one being replaced) function can be provided for
;   dispatch on such CPUs, the name of old function can be changed in the source or via macro:
;      -DINTEL_SHA1_UPDATE_DEFAULT_DISPATCH=default_sha1_update_function_name
;
;   Authors: Maxim Locktyukhin and Ronen Zohar at Intel.com
;

%ifndef INTEL_SHA1_UPDATE_DEFAULT_DISPATCH
 ;; can be replaced with a default SHA-1 update function name
%define INTEL_SHA1_UPDATE_DEFAULT_DISPATCH  sha1_intel_non_ssse3_cpu_stub_
%else
extern  INTEL_SHA1_UPDATE_DEFAULT_DISPATCH
%endif

;; provide alternative SHA-1 update function's name here
%ifndef INTEL_SHA1_UPDATE_FUNCNAME
%define INTEL_SHA1_UPDATE_FUNCNAME     sha1_update_intel
%endif

global INTEL_SHA1_UPDATE_FUNCNAME


%ifndef INTEL_SHA1_SINGLEBLOCK
%assign multiblock 1
%else
%assign multiblock 0
%endif


bits 64
default rel

%ifdef WIN_ABI
 %xdefine arg1 rcx
 %xdefine arg2 rdx
 %xdefine arg3 r8
%else
 %xdefine arg1 rdi
 %xdefine arg2 rsi
 %xdefine arg3 rdx
%endif

%xdefine ctx arg1
%xdefine buf arg2
%xdefine cnt arg3

%macro REGALLOC 0
 %xdefine A ecx
 %xdefine B esi
 %xdefine C edi
 %xdefine D ebp
 %xdefine E edx

 %xdefine T1 eax
 %xdefine T2 ebx
%endmacro

%xdefine K_BASE     r8
%xdefine HASH_PTR   r9
%xdefine BUFFER_PTR r10
%xdefine BUFFER_END r11

%xdefine W_TMP  xmm0
%xdefine W_TMP2 xmm9

%xdefine W0  xmm1
%xdefine W4  xmm2
%xdefine W8  xmm3
%xdefine W12 xmm4
%xdefine W16 xmm5
%xdefine W20 xmm6
%xdefine W24 xmm7
%xdefine W28 xmm8

%xdefine XMM_SHUFB_BSWAP xmm10

;; we keep window of 64 w[i]+K pre-calculated values in a circular buffer
%xdefine WK(t) (rsp + (t & 15)*4)

;------------------------------------------------------------------------------
;
; macro implements SHA-1 function's body for single or several 64-byte blocks
; first param: function's name
; second param: =0 - function implements single 64-byte block hash
;               =1 - function implements multiple64-byte blocks hash
;                    3rd function's argument is a number, greater 0, of 64-byte blocks to calc hash for
;
%macro  SHA1_VECTOR_ASM  2
align 4096
%1:
 push rbx
 push rbp

 %ifdef WIN_ABI
 push rdi
 push rsi

 %xdefine stack_size (16*4 + 16*5 + 8)
 %else
 %xdefine stack_size (16*4 + 8)
 %endif

 sub     rsp, stack_size

 %ifdef WIN_ABI
 %xdefine xmm_save_base (rsp + 16*4)

 xmm_mov [xmm_save_base + 0*16], xmm6
 xmm_mov [xmm_save_base + 1*16], xmm7
 xmm_mov [xmm_save_base + 2*16], xmm8
 xmm_mov [xmm_save_base + 3*16], xmm9
 xmm_mov [xmm_save_base + 4*16], xmm10
 %endif

 mov     HASH_PTR, ctx
 mov     BUFFER_PTR, buf

 %if (%2 == 1)
 shl     cnt, 6           ;; mul by 64
 add     cnt, buf
 mov     BUFFER_END, cnt
 %endif

 lea     K_BASE, [K_XMM_AR]
 xmm_mov XMM_SHUFB_BSWAP, [bswap_shufb_ctl]

 SHA1_PIPELINED_MAIN_BODY %2

 %ifdef WIN_ABI
 xmm_mov xmm6, [xmm_save_base + 0*16]
 xmm_mov xmm7, [xmm_save_base + 1*16]
 xmm_mov xmm8, [xmm_save_base + 2*16]
 xmm_mov xmm9, [xmm_save_base + 3*16]
 xmm_mov xmm10,[xmm_save_base + 4*16]
 %endif

 add rsp, stack_size

 %ifdef WIN_ABI
 pop rsi
 pop rdi
 %endif

 pop rbp
 pop rbx

 ret
%endmacro

;--------------------------------------------
; macro implements 80 rounds of SHA-1, for one 64-byte block or multiple blocks with s/w pipelining
; macro param: =0 - process single 64-byte block
;              =1 - multiple blocks
;
%macro SHA1_PIPELINED_MAIN_BODY 1

 REGALLOC

 mov A, [HASH_PTR   ]
 mov B, [HASH_PTR+ 4]
 mov C, [HASH_PTR+ 8]
 mov D, [HASH_PTR+12]

 mov E, [HASH_PTR+16]

 %assign i 0
 %rep    W_PRECALC_AHEAD
 W_PRECALC i
 %assign i i+1
 %endrep

 %xdefine F F1

 %if (%1 == 1)                         ;; code loops through more than one block
 %%_loop:
 cmp BUFFER_PTR, K_BASE          ;; we use K_BASE value as a signal of a last block,
 jne %%_begin                    ;; it is set below by: cmovae BUFFER_PTR, K_BASE
 jmp %%_end

 align 32
 %%_begin:
 %endif
 RR A,B,C,D,E,0
 RR D,E,A,B,C,2
 RR B,C,D,E,A,4
 RR E,A,B,C,D,6
 RR C,D,E,A,B,8

 RR A,B,C,D,E,10
 RR D,E,A,B,C,12
 RR B,C,D,E,A,14
 RR E,A,B,C,D,16
 RR C,D,E,A,B,18

 %xdefine F F2

 RR A,B,C,D,E,20
 RR D,E,A,B,C,22
 RR B,C,D,E,A,24
 RR E,A,B,C,D,26
 RR C,D,E,A,B,28

 RR A,B,C,D,E,30
 RR D,E,A,B,C,32
 RR B,C,D,E,A,34
 RR E,A,B,C,D,36
 RR C,D,E,A,B,38

 %xdefine F F3

 RR A,B,C,D,E,40
 RR D,E,A,B,C,42
 RR B,C,D,E,A,44
 RR E,A,B,C,D,46
 RR C,D,E,A,B,48

 RR A,B,C,D,E,50
 RR D,E,A,B,C,52
 RR B,C,D,E,A,54
 RR E,A,B,C,D,56
 RR C,D,E,A,B,58

 %xdefine F F4

 %if (%1 == 1)                         ;; if code loops through more than one block
 add   BUFFER_PTR, 64            ;; move to next 64-byte block
 cmp   BUFFER_PTR, BUFFER_END    ;; check if current block is the last one
 cmovae BUFFER_PTR, K_BASE       ;; smart way to signal the last iteration
 %else
 %xdefine W_NO_TAIL_PRECALC 1    ;; no software pipelining for single block interface
 %endif

 RR A,B,C,D,E,60
 RR D,E,A,B,C,62
 RR B,C,D,E,A,64
 RR E,A,B,C,D,66
 RR C,D,E,A,B,68

 RR A,B,C,D,E,70
 RR D,E,A,B,C,72
 RR B,C,D,E,A,74
 RR E,A,B,C,D,76
 RR C,D,E,A,B,78

 UPDATE_HASH [HASH_PTR   ],A
 UPDATE_HASH [HASH_PTR+ 4],B
 UPDATE_HASH [HASH_PTR+ 8],C
 UPDATE_HASH [HASH_PTR+12],D
 UPDATE_HASH [HASH_PTR+16],E

 %if (%1 == 1)
 jmp %%_loop

 align 32
 %%_end:
 %endif


 %xdefine W_NO_TAIL_PRECALC 0
 %xdefine F %error

%endmacro


%macro F1 3
 mov T1,%2
 xor T1,%3
 and T1,%1
 xor T1,%3
%endmacro

%macro F2 3
 mov T1,%3
 xor T1,%2
 xor T1,%1
%endmacro

%macro F3 3
 mov T1,%2
 mov T2,%1
 or  T1,%1
 and T2,%2
 and T1,%3
 or  T1,T2
%endmacro

%define F4 F2

%macro UPDATE_HASH 2
 add %2, %1
 mov %1, %2
%endmacro


%macro W_PRECALC 1
 %xdefine i (%1)

 %if (i < 20)
 %xdefine K_XMM  0
 %elif (i < 40)
 %xdefine K_XMM  16
 %elif (i < 60)
 %xdefine K_XMM  32
 %else
 %xdefine K_XMM  48
 %endif

 %if (i<16 || (i>=80 && i<(80 + W_PRECALC_AHEAD)))

 %if (W_NO_TAIL_PRECALC == 0)

 %xdefine i ((%1) % 80)        ;; pre-compute for the next iteration

 %if (i == 0)
 W_PRECALC_RESET
 %endif


 W_PRECALC_00_15
 %endif

 %elif (i < 32)
 W_PRECALC_16_31
 %elif (i < 80)   ;; rounds 32-79
 W_PRECALC_32_79
 %endif
%endmacro

%macro W_PRECALC_RESET 0
 %xdefine    W             W0
 %xdefine    W_minus_04    W4
 %xdefine    W_minus_08    W8
 %xdefine    W_minus_12    W12
 %xdefine    W_minus_16    W16
 %xdefine    W_minus_20    W20
 %xdefine    W_minus_24    W24
 %xdefine    W_minus_28    W28
 %xdefine    W_minus_32    W
%endmacro

%macro W_PRECALC_ROTATE 0
 %xdefine    W_minus_32    W_minus_28
 %xdefine    W_minus_28    W_minus_24
 %xdefine    W_minus_24    W_minus_20
 %xdefine    W_minus_20    W_minus_16
 %xdefine    W_minus_16    W_minus_12
 %xdefine    W_minus_12    W_minus_08
 %xdefine    W_minus_08    W_minus_04
 %xdefine    W_minus_04    W
 %xdefine    W             W_minus_32
%endmacro

%xdefine W_PRECALC_AHEAD   16
%xdefine W_NO_TAIL_PRECALC 0


%xdefine xmm_mov            movdqa

%macro W_PRECALC_00_15 0
 ;; message scheduling pre-compute for rounds 0-15
 %if ((i & 3) == 0)       ;; blended SSE and ALU instruction scheduling, 1 vector iteration per 4 rounds
 movdqu W_TMP, [BUFFER_PTR + (i * 4)]
 %elif ((i & 3) == 1)
 pshufb W_TMP, XMM_SHUFB_BSWAP
 movdqa W, W_TMP
 %elif ((i & 3) == 2)
 paddd  W_TMP, [K_BASE]
 %elif ((i & 3) == 3)
 movdqa  [WK(i&~3)], W_TMP

 W_PRECALC_ROTATE
 %endif
%endmacro

%macro W_PRECALC_16_31 0
 ;; message scheduling pre-compute for rounds 16-31
 ;; calculating last 32 w[i] values in 8 XMM registers
 ;; pre-calculate K+w[i] values and store to mem, for later load by ALU add instruction
 ;;
 ;; "brute force" vectorization for rounds 16-31 only due to w[i]->w[i-3] dependency
 ;;
 %if ((i & 3) == 0)    ;; blended SSE and ALU instruction scheduling, 1 vector iteration per 4 rounds
 movdqa  W, W_minus_12
 palignr W, W_minus_16, 8       ;; w[i-14]
 movdqa  W_TMP, W_minus_04
 psrldq  W_TMP, 4               ;; w[i-3]
 pxor    W, W_minus_08
 %elif ((i & 3) == 1)
 pxor    W_TMP, W_minus_16
 pxor    W, W_TMP
 movdqa  W_TMP2, W
 movdqa  W_TMP, W
 pslldq  W_TMP2, 12
 %elif ((i & 3) == 2)
 psrld   W, 31
 pslld   W_TMP, 1
 por     W_TMP, W
 movdqa  W, W_TMP2
 psrld   W_TMP2, 30
 pslld   W, 2
 %elif ((i & 3) == 3)
 pxor    W_TMP, W
 pxor    W_TMP, W_TMP2
 movdqa  W, W_TMP
 paddd   W_TMP, [K_BASE + K_XMM]
 movdqa  [WK(i&~3)],W_TMP

 W_PRECALC_ROTATE
 %endif
%endmacro

%macro W_PRECALC_32_79 0
 ;; in SHA-1 specification: w[i] = (w[i-3] ^ w[i-8]  ^ w[i-14] ^ w[i-16]) rol 1
 ;; instead we do equal:    w[i] = (w[i-6] ^ w[i-16] ^ w[i-28] ^ w[i-32]) rol 2
 ;; allows more efficient vectorization since w[i]=>w[i-3] dependency is broken
 ;;
 %if ((i & 3) == 0)    ;; blended SSE and ALU instruction scheduling, 1 vector iteration per 4 rounds
 movdqa  W_TMP, W_minus_04
 pxor    W, W_minus_28         ;; W is W_minus_32 before xor
 palignr W_TMP, W_minus_08, 8
 %elif ((i & 3) == 1)
 pxor    W, W_minus_16
 pxor    W, W_TMP
 movdqa  W_TMP, W
 %elif ((i & 3) == 2)
 psrld   W, 30
 pslld   W_TMP, 2
 por     W_TMP, W
 %elif ((i & 3) == 3)
 movdqa  W, W_TMP
 paddd   W_TMP, [K_BASE + K_XMM]
 movdqa  [WK(i&~3)],W_TMP

 W_PRECALC_ROTATE
 %endif
%endmacro

%macro RR 6             ;; RR does two rounds of SHA-1 back to back with W pre-calculation

 ;;     TEMP = A
 ;;     A = F( i, B, C, D ) + E + ROTATE_LEFT( A, 5 ) + W[i] + K(i)
 ;;     C = ROTATE_LEFT( B, 30 )
 ;;     D = C
 ;;     E = D
 ;;     B = TEMP

 W_PRECALC (%6 + W_PRECALC_AHEAD)
 F    %2, %3, %4     ;; F returns result in T1
 add  %5, [WK(%6)]
 rol  %2, 30
 mov  T2, %1
 add  %4, [WK(%6 + 1)]
 rol  T2, 5
 add  %5, T1

 W_PRECALC (%6 + W_PRECALC_AHEAD + 1)
 add  T2, %5
 mov  %5, T2
 rol  T2, 5
 add  %4, T2
 F    %1, %2, %3    ;; F returns result in T1
 add  %4, T1
 rol  %1, 30

;; write:  %1, %2
;; rotate: %1<=%4, %2<=%5, %3<=%1, %4<=%2, %5<=%3
%endmacro



;;----------------------
section .data align=128

%xdefine K1 0x5a827999
%xdefine K2 0x6ed9eba1
%xdefine K3 0x8f1bbcdc
%xdefine K4 0xca62c1d6

align 128
K_XMM_AR:
 DD K1, K1, K1, K1
 DD K2, K2, K2, K2
 DD K3, K3, K3, K3
 DD K4, K4, K4, K4

align 16
bswap_shufb_ctl:
 DD 00010203h
 DD 04050607h
 DD 08090a0bh
 DD 0c0d0e0fh

;; dispatch pointer, points to the init routine for the first invocation
sha1_update_intel_dispatched:
 DQ  sha1_update_intel_init_

;;----------------------
section .text align=4096

SHA1_VECTOR_ASM     sha1_update_intel_ssse3_, multiblock

align 32
sha1_update_intel_init_:       ;; we get here with the first time invocation
 call    sha1_update_intel_dispacth_init_
INTEL_SHA1_UPDATE_FUNCNAME:    ;; we get here after init
 jmp     qword [sha1_update_intel_dispatched]

;; CPUID feature flag based dispatch
sha1_update_intel_dispacth_init_:
 push    rax
 push    rbx
 push    rcx
 push    rdx
 push    rsi

 lea     rsi, [INTEL_SHA1_UPDATE_DEFAULT_DISPATCH]

 mov     eax, 1
 cpuid

 test    ecx, 0200h          ;; SSSE3 support, CPUID.1.ECX[bit 9]
 jz      _done

 lea     rsi, [sha1_update_intel_ssse3_]

_done:
 mov     [sha1_update_intel_dispatched], rsi

 pop     rsi
 pop     rdx
 pop     rcx
 pop     rbx
 pop     rax
 ret

;;----------------------
;; in the case a default SHA-1 update function implementation was not provided
;; and code was invoked on a non-SSSE3 supporting CPU, dispatch handles this
;; failure in a safest way - jumps to the stub function with UD2 instruction below
sha1_intel_non_ssse3_cpu_stub_:
 ud2     ;; in the case no default SHA-1 was provided non-SSSE3 CPUs safely fail here
 ret

; END
;----------------------
