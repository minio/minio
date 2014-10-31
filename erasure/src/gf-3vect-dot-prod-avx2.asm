;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) 2011-2014 Intel Corporation All rights reserved.
;
;  Redistribution and use in source and binary forms, with or without
;  modification, are permitted provided that the following conditions 
;  are met:
;    * Redistributions of source code must retain the above copyright
;      notice, this list of conditions and the following disclaimer.
;    * Redistributions in binary form must reproduce the above copyright
;      notice, this list of conditions and the following disclaimer in
;      the documentation and/or other materials provided with the
;      distribution.
;    * Neither the name of Intel Corporation nor the names of its
;      contributors may be used to endorse or promote products derived
;      from this software without specific prior written permission.
;
;  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
;  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
;  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
;  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
;  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;
;;; gf_3vect_dot_prod_avx2(len, vec, *g_tbls, **buffs, **dests);
;;;
;;; Author: Gregory Tucker


%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0  rdi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9

 %define tmp   r11
 %define tmp.w r11d
 %define tmp.b r11b
 %define tmp2  r10
 %define tmp3  r13		; must be saved and restored
 %define tmp4  r12		; must be saved and restored
 %define return rax
 %define PS 8
 %define LOG_PS 3

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
	push	r13
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r13
	pop	r12
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9

 %define arg4   r12 		; must be saved, loaded and restored
 %define arg5   r15 		; must be saved and restored
 %define tmp    r11
 %define tmp.w  r11d
 %define tmp.b  r11b
 %define tmp2   r10
 %define tmp3   r13		; must be saved and restored
 %define tmp4   r14		; must be saved and restored
 %define return rax
 %define PS     8
 %define LOG_PS 3
 %define stack_size  6*16 + 5*8 	; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + PS + PS*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	vmovdqa	[rsp + 0*16], xmm6
	vmovdqa	[rsp + 1*16], xmm7
	vmovdqa	[rsp + 2*16], xmm8
	vmovdqa	[rsp + 3*16], xmm9
	vmovdqa	[rsp + 4*16], xmm10
	vmovdqa	[rsp + 5*16], xmm11
	save_reg	r12,  6*16 + 0*8
	save_reg	r13,  6*16 + 1*8
	save_reg	r14,  6*16 + 2*8
	save_reg	r15,  6*16 + 3*8
	end_prolog
	mov	arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
	vmovdqa	xmm6, [rsp + 0*16]
	vmovdqa	xmm7, [rsp + 1*16]
	vmovdqa	xmm8, [rsp + 2*16]
	vmovdqa	xmm9, [rsp + 3*16]
	vmovdqa	xmm10, [rsp + 4*16]
	vmovdqa	xmm11, [rsp + 5*16]
	mov	r12,  [rsp + 6*16 + 0*8]
	mov	r13,  [rsp + 6*16 + 1*8]
	mov	r14,  [rsp + 6*16 + 2*8]
	mov	r15,  [rsp + 6*16 + 3*8]
	add	rsp, stack_size
 %endmacro
%endif

%define len   arg0
%define vec   arg1
%define mul_array arg2
%define	src   arg3
%define dest1 arg4
%define ptr   arg5
%define vec_i tmp2
%define dest2 tmp3
%define dest3 tmp4
%define pos   return

%ifndef EC_ALIGNED_ADDR
;;; Use Un-aligned load/store
 %define XLDR vmovdqu
 %define XSTR vmovdqu
%else
;;; Use Non-temporal load/stor
 %ifdef NO_NT_LDST
  %define XLDR vmovdqa
  %define XSTR vmovdqa
 %else
  %define XLDR vmovntdqa
  %define XSTR vmovntdq
 %endif
%endif


default rel

[bits 64]
section .text

%define xmask0f   ymm11
%define xmask0fx  xmm11
%define xgft1_lo  ymm10
%define xgft1_hi  ymm9
%define xgft2_lo  ymm8
%define xgft2_hi  ymm7
%define xgft3_lo  ymm6
%define xgft3_hi  ymm5

%define x0     ymm0
%define xtmpa  ymm1
%define xp1    ymm2
%define xp2    ymm3
%define xp3    ymm4

align 16
global gf_3vect_dot_prod_avx2:function
func(gf_3vect_dot_prod_avx2)
	FUNC_SAVE
	sub	len, 32
	jl	.return_fail
	xor	pos, pos
	mov	tmp.b, 0x0f
	vpinsrb	xmask0fx, xmask0fx, tmp.w, 0
	vpbroadcastb xmask0f, xmask0fx	;Construct mask 0x0f0f0f...

	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	mov	dest2, [dest1+PS]
	mov	dest3, [dest1+2*PS]
	mov	dest1, [dest1]


.loop32:
	vpxor	xp1, xp1
	vpxor	xp2, xp2
	vpxor	xp3, xp3
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect:
	mov	ptr, [src+vec_i]

	vmovdqu	xgft1_lo, [tmp]		;Load array Ax{00}, Ax{01}, ..., Ax{0f}
					;     "     Ax{00}, Ax{10}, ..., Ax{f0}
	vperm2i128 xgft1_hi, xgft1_lo, xgft1_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft1_lo, xgft1_lo, xgft1_lo, 0x00 ; swapped to lo | lo

	vmovdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
						;     "     Bx{00}, Bx{10}, ..., Bx{f0}
	vperm2i128 xgft2_hi, xgft2_lo, xgft2_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft2_lo, xgft2_lo, xgft2_lo, 0x00 ; swapped to lo | lo

	vmovdqu	xgft3_lo, [tmp+vec*(64/PS)]	;Load array Cx{00}, Cx{01}, ..., Cx{0f}
						;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	vperm2i128 xgft3_hi, xgft3_lo, xgft3_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft3_lo, xgft3_lo, xgft3_lo, 0x00 ; swapped to lo | lo

	add	tmp, 32
	add	vec_i, PS
	XLDR	x0, [ptr+pos]		;Get next source vector

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	vpshufb	xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft1_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft1_hi, xgft1_lo	;GF add high and low partials
	vpxor	xp1, xgft1_hi		;xp1 += partial

	vpshufb	xgft2_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft2_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft2_hi, xgft2_lo	;GF add high and low partials
	vpxor	xp2, xgft2_hi		;xp2 += partial

	vpshufb	xgft3_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft3_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft3_hi, xgft3_lo	;GF add high and low partials
	vpxor	xp3, xgft3_hi		;xp3 += partial

	cmp	vec_i, vec
	jl	.next_vect

	XSTR	[dest1+pos], xp1
	XSTR	[dest2+pos], xp2
	XSTR	[dest3+pos], xp3

	add	pos, 32			;Loop on 32 bytes at a time
	cmp	pos, len
	jle	.loop32

	lea	tmp, [len + 32]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-16
	jmp	.loop32		;Do one more overlap pass

.return_pass:
	mov	return, 0
	FUNC_RESTORE
	ret

.return_fail:
	mov	return, 1
	FUNC_RESTORE
	ret

endproc_frame

section .data

%macro slversion 4
global %1_slver_%2%3%4
global %1_slver
%1_slver:
%1_slver_%2%3%4:
	dw 0x%4
	db 0x%3, 0x%2
%endmacro
;;;       func                   core, ver, snum
slversion gf_3vect_dot_prod_avx2, 04,  03,  0197
