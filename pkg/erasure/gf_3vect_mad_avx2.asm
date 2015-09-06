;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) 2011-2015 Intel Corporation All rights reserved.
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
;;; gf_3vect_mad_avx2(len, vec, vec_i, mul_array, src, dest);
;;;

%define PS 8

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg0.w ecx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9
 %define arg4   r12 		; must be saved, loaded and restored
 %define arg5   r15 		; must be saved and restored

 %define tmp    r11
 %define tmp.w  r11d
 %define tmp.b  r11b
 %define return rax
 %define return.w eax
 %define stack_size 16*10 + 3*8
 %define arg(x)      [rsp + stack_size + PS + PS*x]
 %define func(x) proc_frame x

 %macro FUNC_SAVE 0
	sub	rsp, stack_size
	vmovdqa	[rsp+16*0],xmm6
	vmovdqa	[rsp+16*1],xmm7
	vmovdqa	[rsp+16*2],xmm8
	vmovdqa	[rsp+16*3],xmm9
	vmovdqa	[rsp+16*4],xmm10
	vmovdqa	[rsp+16*5],xmm11
	vmovdqa	[rsp+16*6],xmm12
	vmovdqa	[rsp+16*7],xmm13
	vmovdqa	[rsp+16*8],xmm14
	vmovdqa	[rsp+16*9],xmm15
	save_reg	r12,  10*16 + 0*8
	save_reg	r15,  10*16 + 1*8
	end_prolog
	mov	arg4, arg(4)
	mov	arg5, arg(5)
 %endmacro

 %macro FUNC_RESTORE 0
	vmovdqa	xmm6, [rsp+16*0]
	vmovdqa	xmm7, [rsp+16*1]
	vmovdqa	xmm8, [rsp+16*2]
	vmovdqa	xmm9, [rsp+16*3]
	vmovdqa	xmm10, [rsp+16*4]
	vmovdqa	xmm11, [rsp+16*5]
	vmovdqa	xmm12, [rsp+16*6]
	vmovdqa	xmm13, [rsp+16*7]
	vmovdqa	xmm14, [rsp+16*8]
	vmovdqa	xmm15, [rsp+16*9]
	mov	r12,  [rsp + 10*16 + 0*8]
	mov	r15,  [rsp + 10*16 + 1*8]
	add	rsp, stack_size
 %endmacro

%elifidn __OUTPUT_FORMAT__, elf64
 %define arg0  rdi
 %define arg0.w edi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9

 %define tmp   r11
 %define tmp.w r11d
 %define tmp.b r11b
 %define return rax
 %define return.w eax

 %define func(x) x:
 %define FUNC_SAVE
 %define FUNC_RESTORE

%elifidn __OUTPUT_FORMAT__, macho64
 %define arg0  rdi
 %define arg0.w edi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9

 %define tmp   r11
 %define tmp.w r11d
 %define tmp.b r11b
 %define return rax
 %define return.w eax

 %define func(x) x:
 %define FUNC_SAVE
 %define FUNC_RESTORE
%endif

;;; gf_3vect_mad_avx2(len, vec, vec_i, mul_array, src, dest)
%define len   arg0
%define len.w arg0.w
%define vec    arg1
%define vec_i    arg2
%define mul_array arg3
%define	src   arg4
%define dest1 arg5
%define pos   return
%define pos.w return.w

%define dest2 mul_array
%define dest3 vec_i

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

%define xmask0f   ymm15
%define xmask0fx  xmm15
%define xgft1_lo  ymm14
%define xgft1_hi  ymm13
%define xgft2_lo  ymm12
%define xgft3_lo  ymm11

%define x0      ymm0
%define xtmpa   ymm1
%define xtmph1  ymm2
%define xtmpl1  ymm3
%define xtmph2  ymm4
%define xtmpl2  ymm5
%define xtmpl2x xmm5
%define xtmph3  ymm6
%define xtmpl3  ymm7
%define xtmpl3x xmm7
%define xd1     ymm8
%define xd2     ymm9
%define xd3     ymm10

align 16
global gf_3vect_mad_avx2:function
func(gf_3vect_mad_avx2)
	FUNC_SAVE
	sub	len, 32
	jl	.return_fail
	xor	pos, pos
	mov	tmp.b, 0x0f
	vpinsrb	xmask0fx, xmask0fx, tmp.w, 0
	vpbroadcastb xmask0f, xmask0fx	;Construct mask 0x0f0f0f...

	sal	vec_i, 5		;Multiply by 32
	sal	vec, 5
	lea	tmp, [mul_array + vec_i]

	vmovdqu	xgft1_lo, [tmp]		;Load array Ax{00}, Ax{01}, ..., Ax{0f}
					;     "     Ax{00}, Ax{10}, ..., Ax{f0}
	vperm2i128 xgft1_hi, xgft1_lo, xgft1_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft1_lo, xgft1_lo, xgft1_lo, 0x00 ; swapped to lo | lo

	vmovdqu	xgft2_lo, [tmp+vec]	;Load array Bx{00}, Bx{01}, Bx{02}, ...
					; " Bx{00}, Bx{10}, Bx{20}, ... , Bx{f0}
	vmovdqu	xgft3_lo, [tmp+2*vec]	;Load array Cx{00}, Cx{01}, Cx{02}, ...
					; " Cx{00}, Cx{10}, Cx{20}, ... , Cx{f0}
	mov	dest2, [dest1+PS]	; reuse mul_array
	mov	dest3, [dest1+2*PS]	; reuse vec_i
	mov	dest1, [dest1]

.loop32:
	XLDR	x0, [src+pos]		;Get next source vector
	XLDR	xd1, [dest1+pos]		;Get next dest vector
	XLDR	xd2, [dest2+pos]		;Get next dest vector
	XLDR	xd3, [dest3+pos]		;Get next dest vector
	vperm2i128 xtmph2, xgft2_lo, xgft2_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xtmpl2, xgft2_lo, xgft2_lo, 0x00 ; swapped to lo | lo

	vperm2i128 xtmph3, xgft3_lo, xgft3_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xtmpl3, xgft3_lo, xgft3_lo, 0x00 ; swapped to lo | lo

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	; dest1
	vpshufb	xtmph1, xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl1, xgft1_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph1, xtmph1, xtmpl1		;GF add high and low partials
	vpxor	xd1, xd1, xtmph1		;xd1 += partial

	; dest2
	vpshufb	xtmph2, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl2, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph2, xtmpl2		;GF add high and low partials
	vpxor	xd2, xtmph2		;xd2 += partial

	; dest3
	vpshufb	xtmph3, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl3, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph3, xtmpl3		;GF add high and low partials
	vpxor	xd3, xtmph3		;xd3 += partial

	XSTR	[dest1+pos], xd1
	XSTR	[dest2+pos], xd2
	XSTR	[dest3+pos], xd3

	add	pos, 32			;Loop on 32 bytes at a time
	cmp	pos, len
	jle	.loop32

	lea	tmp, [len + 32]
	cmp	pos, tmp
	je	.return_pass

.lessthan32:
	;; Tail len
	;; Do one more overlap pass
	mov	tmp.b, 0x1f
	vpinsrb	xtmpl2x, xtmpl2x, tmp.w, 0
	vpbroadcastb xtmpl2, xtmpl2x	;Construct mask 0x1f1f1f...

	mov	tmp, len		;Overlapped offset length-32

	XLDR	x0, [src+tmp]		;Get next source vector
	XLDR	xd1, [dest1+tmp]	;Get next dest vector
	XLDR	xd2, [dest2+tmp]	;Get next dest vector
	XLDR	xd3, [dest3+tmp]	;Get next dest vector

	sub	len, pos

	vmovdqa	xtmph3, [constip32]	;Load const of i + 32
	vpinsrb	xtmpl3x, xtmpl3x, len.w, 15
	vinserti128	xtmpl3, xtmpl3, xtmpl3x, 1 ;swapped to xtmpl3x | xtmpl3x
	vpshufb	xtmpl3, xtmpl3, xtmpl2	;Broadcast len to all bytes. xtmpl2=0x1f1f1f...
	vpcmpgtb	xtmpl3, xtmpl3, xtmph3

	vperm2i128 xtmph2, xgft2_lo, xgft2_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft2_lo, xgft2_lo, xgft2_lo, 0x00 ; swapped to lo | lo

	vperm2i128 xtmph3, xgft3_lo, xgft3_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft3_lo, xgft3_lo, xgft3_lo, 0x00 ; swapped to lo | lo

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	; dest1
	vpshufb	xtmph1, xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl1, xgft1_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph1, xtmph1, xtmpl1		;GF add high and low partials
	vpand	xtmph1, xtmph1, xtmpl3
	vpxor	xd1, xd1, xtmph1		;xd1 += partial

	; dest2
	vpshufb	xtmph2, xtmph2, x0		;Lookup mul table of high nibble
	vpshufb	xgft2_lo, xgft2_lo, xtmpa	;Lookup mul table of low nibble
	vpxor	xtmph2, xtmph2, xgft2_lo	;GF add high and low partials
	vpand	xtmph2, xtmph2, xtmpl3
	vpxor	xd2, xd2, xtmph2		;xd2 += partial

	; dest3
	vpshufb	xtmph3, xtmph3, x0		;Lookup mul table of high nibble
	vpshufb	xgft3_lo, xgft3_lo, xtmpa	;Lookup mul table of low nibble
	vpxor	xtmph3, xtmph3, xgft3_lo	;GF add high and low partials
	vpand	xtmph3, xtmph3, xtmpl3
	vpxor	xd3, xd3, xtmph3		;xd3 += partial

	XSTR	[dest1+tmp], xd1
	XSTR	[dest2+tmp], xd2
	XSTR	[dest3+tmp], xd3

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

align 32
constip32:
	ddq 0xf0f1f2f3f4f5f6f7f8f9fafbfcfdfeff
	ddq 0xe0e1e2e3e4e5e6e7e8e9eaebecedeeef

%macro slversion 4
global %1_slver_%2%3%4
global %1_slver
%1_slver:
%1_slver_%2%3%4:
	dw 0x%4
	db 0x%3, 0x%2
%endmacro
;;;       func              core, ver, snum
slversion gf_3vect_mad_avx2, 04,  00,  0208
