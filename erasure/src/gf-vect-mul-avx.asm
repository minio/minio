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
;;; gf_vect_mul_avx(len, mul_array, src, dest)
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
 %define return rax
 %define func(x) x:
 %define FUNC_SAVE
 %define FUNC_RESTORE

%elifidn __OUTPUT_FORMAT__, win64
 %define arg0  rcx
 %define arg1  rdx
 %define arg2  r8
 %define arg3  r9
 %define return rax
 %define stack_size  5*16 + 8 	; must be an odd multiple of 8
 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	save_xmm128	xmm6, 0*16
	save_xmm128	xmm7, 1*16
	save_xmm128	xmm13, 2*16
	save_xmm128	xmm14, 3*16
	save_xmm128	xmm15, 4*16
	end_prolog
 %endmacro

 %macro FUNC_RESTORE 0
	vmovdqa	xmm6, [rsp + 0*16]
	vmovdqa	xmm7, [rsp + 1*16]
	vmovdqa	xmm13, [rsp + 2*16]
	vmovdqa	xmm14, [rsp + 3*16]
	vmovdqa	xmm15, [rsp + 4*16]
	add	rsp, stack_size
 %endmacro

%endif


%define len   arg0
%define mul_array arg1
%define	src   arg2
%define dest  arg3
%define pos   return


;;; Use Non-temporal load/stor
%ifdef NO_NT_LDST
 %define XLDR vmovdqa
 %define XSTR vmovdqa
%else
 %define XLDR vmovntdqa
 %define XSTR vmovntdq
%endif

default rel

[bits 64]
section .text

%define xmask0f  xmm15
%define xgft_lo  xmm14
%define xgft_hi  xmm13

%define x0     xmm0
%define xtmp1a xmm1
%define xtmp1b xmm2
%define xtmp1c xmm3
%define x1     xmm4
%define xtmp2a xmm5
%define xtmp2b xmm6
%define xtmp2c xmm7

align 16
global gf_vect_mul_avx:function
func(gf_vect_mul_avx)
	FUNC_SAVE
	mov	pos, 0
	vmovdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte
	vmovdqu	xgft_lo, [mul_array]	;Load array Cx{00}, Cx{01}, Cx{02}, ...
	vmovdqu	xgft_hi, [mul_array+16]	; " Cx{00}, Cx{10}, Cx{20}, ... , Cx{f0}

loop32:
	XLDR	x0, [src+pos]		;Get next source vector
	XLDR	x1, [src+pos+16]	;Get next source vector + 16B ahead
	add	pos, 32			;Loop on 16 bytes at a time
	cmp	pos, len
	vpand	xtmp1a, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpand	xtmp2a, x1, xmask0f
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpsraw	x1, x1, 4
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0
	vpand	x1, x1, xmask0f
	vpshufb	xtmp1b, xgft_hi, x0	;Lookup mul table of high nibble
	vpshufb	xtmp1c, xgft_lo, xtmp1a	;Lookup mul table of low nibble
	vpshufb	xtmp2b, xgft_hi, x1	;Lookup mul table of high nibble
	vpshufb	xtmp2c, xgft_lo, xtmp2a	;Lookup mul table of low nibble
	vpxor	xtmp1b, xtmp1b, xtmp1c	;GF add high and low partials
	vpxor	xtmp2b, xtmp2b, xtmp2c
	XSTR	[dest+pos-32], xtmp1b	;Store result
	XSTR	[dest+pos-16], xtmp2b	;Store +16B result
	jl	loop32


return_pass:
	FUNC_RESTORE
	sub	pos, len
	ret

return_fail:
	FUNC_RESTORE
	mov	return, 1
	ret

endproc_frame

section .data

align 16

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
;;;       func             core, ver, snum
slversion gf_vect_mul_avx, 01,   02,  0036
