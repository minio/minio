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
;;; gf_vect_dot_prod_avx(len, vec, *g_tbls, **buffs, *dest);
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
 %define tmp2  r10
 %define tmp3  r9
 %define return rax
 %define PS 8
 %define func(x) x:
 %define FUNC_SAVE
 %define FUNC_RESTORE
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9

 %define arg4   r12 		; must be saved and loaded
 %define tmp    r11
 %define tmp2   r10
 %define tmp3   rdi 		; must be saved and loaded
 %define return rax
 %define PS 8
 %define frame_size 2*8
 %define arg(x)      [rsp + frame_size + PS + PS*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	rex_push_reg	r12
	push_reg	rdi
	end_prolog
	mov	arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
	pop	rdi
	pop	r12
 %endmacro
%endif


%define len   arg0
%define vec   arg1
%define mul_array arg2
%define	src   arg3
%define dest  arg4

%define vec_i tmp2
%define ptr   tmp3
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

%define xmask0f  xmm5
%define xgft_lo  xmm4
%define xgft_hi  xmm3

%define x0     xmm0
%define xtmpa  xmm1
%define xp     xmm2

align 16
global gf_vect_dot_prod_avx:function
func(gf_vect_dot_prod_avx)
	FUNC_SAVE
	sub	len, 16
	jl	.return_fail
	xor	pos, pos
	vmovdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte

.loop16:
	vpxor	xp, xp
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect:
	mov	ptr, [src+vec_i*PS]
	vmovdqu	xgft_lo, [tmp]		;Load array Cx{00}, Cx{01}, ..., Cx{0f}
	vmovdqu	xgft_hi, [tmp+16]	;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	XLDR	x0, [ptr+pos]		;Get next source vector
	add	tmp, 32
	add	vec_i, 1

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	vpshufb	xgft_hi, xgft_hi, x0	;Lookup mul table of high nibble
	vpshufb	xgft_lo, xgft_lo, xtmpa	;Lookup mul table of low nibble
	vpxor	xgft_hi, xgft_hi, xgft_lo ;GF add high and low partials
	vpxor	xp, xp, xgft_hi		;xp += partial
	cmp	vec_i, vec
	jl	.next_vect

	XSTR	[dest+pos], xp
	add	pos, 16			;Loop on 16 bytes at a time
	cmp	pos, len
	jle	.loop16

	lea	tmp, [len + 16]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-16
	jmp	.loop16		;Do one more overlap pass

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

align 16

poly:
mask0f:
ddq 0x0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f

%macro slversion 4
global %1_slver_%2%3%4
global %1_slver
%1_slver:
%1_slver_%2%3%4:
	dw 0x%4
	db 0x%3, 0x%2
%endmacro
;;;       func                 core, ver, snum
slversion gf_vect_dot_prod_avx, 02,  03,  0061
