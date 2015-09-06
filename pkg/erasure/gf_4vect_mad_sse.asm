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
;;; gf_4vect_mad_sse(len, vec, vec_i, mul_array, src, dest);
;;;

%define PS 8

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0  rcx
 %define arg0.w ecx
 %define arg1  rdx
 %define arg2  r8
 %define arg3  r9
 %define arg4  r12
 %define arg5  r15
 %define tmp   r11
 %define tmp2   r10
 %define tmp3   r13
 %define return rax
 %define return.w eax
 %define stack_size 16*10 + 3*8
 %define arg(x)      [rsp + stack_size + PS + PS*x]
 %define func(x) proc_frame x

%macro FUNC_SAVE 0
	sub	rsp, stack_size
	movdqa	[rsp+16*0],xmm6
	movdqa	[rsp+16*1],xmm7
	movdqa	[rsp+16*2],xmm8
	movdqa	[rsp+16*3],xmm9
	movdqa	[rsp+16*4],xmm10
	movdqa	[rsp+16*5],xmm11
	movdqa	[rsp+16*6],xmm12
	movdqa	[rsp+16*7],xmm13
	movdqa	[rsp+16*8],xmm14
	movdqa	[rsp+16*9],xmm15
	save_reg	r12,  10*16 + 0*8
	save_reg	r13,  10*16 + 1*8
	save_reg	r15,  10*16 + 2*8
	end_prolog
	mov	arg4, arg(4)
	mov	arg5, arg(5)
%endmacro

%macro FUNC_RESTORE 0
	movdqa	xmm6, [rsp+16*0]
	movdqa	xmm7, [rsp+16*1]
	movdqa	xmm8, [rsp+16*2]
	movdqa	xmm9, [rsp+16*3]
	movdqa	xmm10, [rsp+16*4]
	movdqa	xmm11, [rsp+16*5]
	movdqa	xmm12, [rsp+16*6]
	movdqa	xmm13, [rsp+16*7]
	movdqa	xmm14, [rsp+16*8]
	movdqa	xmm15, [rsp+16*9]
	mov	r12,  [rsp + 10*16 + 0*8]
	mov	r13,  [rsp + 10*16 + 1*8]
	mov	r15,  [rsp + 10*16 + 2*8]
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
 %define tmp2   r10
 %define tmp3   r12
 %define return rax
 %define return.w eax

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r12
 %endmacro

%elifidn __OUTPUT_FORMAT__, macho64
 %define arg0  rdi
 %define arg0.w edi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9
 %define tmp   r11
 %define tmp2   r10
 %define tmp3   r12
 %define return rax
 %define return.w eax

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r12
 %endmacro
%endif

;;; gf_4vect_mad_sse(len, vec, vec_i, mul_array, src, dest)
%define len   arg0
%define len.w arg0.w
%define vec    arg1
%define vec_i    arg2
%define mul_array arg3
%define	src   arg4
%define dest1  arg5
%define pos   return
%define pos.w return.w

%define dest2 mul_array
%define dest3 tmp2
%define dest4 vec_i

%ifndef EC_ALIGNED_ADDR
;;; Use Un-aligned load/store
 %define XLDR movdqu
 %define XSTR movdqu
%else
;;; Use Non-temporal load/stor
 %ifdef NO_NT_LDST
  %define XLDR movdqa
  %define XSTR movdqa
 %else
  %define XLDR movntdqa
  %define XSTR movntdq
 %endif
%endif

default rel

[bits 64]
section .text

%define xmask0f  xmm15
%define xgft3_hi xmm14
%define xgft4_hi xmm13
%define xgft4_lo xmm12

%define x0      xmm0
%define xtmpa   xmm1
%define xtmph1  xmm2
%define xtmpl1  xmm3
%define xtmph2  xmm4
%define xtmpl2  xmm5
%define xtmph3  xmm6
%define xtmpl3  xmm7
%define xtmph4  xmm8
%define xtmpl4  xmm9
%define xd1     xmm10
%define xd2     xmm11
%define xd3     xtmph1
%define xd4     xtmpl1

align 16
global gf_4vect_mad_sse:function
func(gf_4vect_mad_sse)
	FUNC_SAVE
	sub	len, 16
	jl	.return_fail
	xor	pos, pos
	movdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte
	mov	tmp, vec

	sal	vec_i, 5		;Multiply by 32
	lea	tmp3, [mul_array + vec_i]

	sal	tmp, 6			;Multiply by 64

	movdqu	xgft3_hi, [tmp3+tmp+16]	; " Cx{00}, Cx{10}, Cx{20}, ... , Cx{f0}
	sal	vec, 5			;Multiply by 32
	add	tmp, vec
	movdqu	xgft4_lo, [tmp3+tmp]	;Load array Dx{00}, Dx{01}, Dx{02}, ...
	movdqu	xgft4_hi, [tmp3+tmp+16]	; " Dx{00}, Dx{10}, Dx{20}, ... , Dx{f0}

	mov	dest2, [dest1+PS]		; reuse mul_array
	mov	dest3, [dest1+2*PS]
	mov	dest4, [dest1+3*PS]		; reuse vec_i
	mov	dest1, [dest1]

.loop16:
	XLDR	x0, [src+pos]		;Get next source vector
	movdqu	xtmph1, [tmp3+16]	; " Ax{00}, Ax{10}, Ax{20}, ... , Ax{f0}
	movdqu	xtmpl1, [tmp3]		;Load array Ax{00}, Ax{01}, Ax{02}, ...
	movdqu	xtmph2, [tmp3+vec+16]	; " Bx{00}, Bx{10}, Bx{20}, ... , Bx{f0}
	movdqu	xtmpl2, [tmp3+vec]	;Load array Bx{00}, Bx{01}, Bx{02}, ...
	movdqu	xtmpl3, [tmp3+2*vec]	;Load array Cx{00}, Cx{01}, Cx{02}, ...

	movdqa	xtmph3, xgft3_hi
	movdqa	xtmpl4, xgft4_lo
	movdqa	xtmph4, xgft4_hi

	XLDR	xd1, [dest1+pos]	;Get next dest vector
	XLDR	xd2, [dest2+pos]	;Get next dest vector

	movdqa	xtmpa, x0		;Keep unshifted copy of src
	psraw	x0, 4			;Shift to put high nibble into bits 4-0
	pand	x0, xmask0f		;Mask high src nibble in bits 4-0
	pand	xtmpa, xmask0f		;Mask low src nibble in bits 4-0

	; dest1
	pshufb	xtmph1, x0		;Lookup mul table of high nibble
	pshufb	xtmpl1, xtmpa		;Lookup mul table of low nibble
	pxor	xtmph1, xtmpl1		;GF add high and low partials
	pxor	xd1, xtmph1

	XLDR	xd3, [dest3+pos]	;Reuse xtmph1, Get next dest vector
	XLDR	xd4, [dest4+pos]	;Reuse xtmpl1, Get next dest vector

	; dest2
	pshufb	xtmph2, x0		;Lookup mul table of high nibble
	pshufb	xtmpl2, xtmpa		;Lookup mul table of low nibble
	pxor	xtmph2, xtmpl2		;GF add high and low partials
	pxor	xd2, xtmph2

	; dest3
	pshufb	xtmph3, x0		;Lookup mul table of high nibble
	pshufb	xtmpl3, xtmpa		;Lookup mul table of low nibble
	pxor	xtmph3, xtmpl3		;GF add high and low partials
	pxor	xd3, xtmph3

	; dest4
	pshufb	xtmph4, x0		;Lookup mul table of high nibble
	pshufb	xtmpl4, xtmpa		;Lookup mul table of low nibble
	pxor	xtmph4, xtmpl4		;GF add high and low partials
	pxor	xd4, xtmph4

	XSTR	[dest1+pos], xd1	;Store result
	XSTR	[dest2+pos], xd2	;Store result
	XSTR	[dest3+pos], xd3	;Store result
	XSTR	[dest4+pos], xd4	;Store result

	add	pos, 16			;Loop on 16 bytes at a time
	cmp	pos, len
	jle	.loop16

	lea	tmp, [len + 16]
	cmp	pos, tmp
	je	.return_pass

.lessthan16:
	;; Tail len
	;; Do one more overlap pass
	mov	tmp, len	;Overlapped offset length-16

	XLDR	x0, [src+tmp]		;Get next source vector

	movdqu	xtmph1, [tmp3+16]	; " Ax{00}, Ax{10}, Ax{20}, ... , Ax{f0}
	movdqu	xtmpl1, [tmp3]		;Load array Ax{00}, Ax{01}, Ax{02}, ...
	movdqu	xtmph2, [tmp3+vec+16]	; " Bx{00}, Bx{10}, Bx{20}, ... , Bx{f0}
	movdqu	xtmpl2, [tmp3+vec]	;Load array Bx{00}, Bx{01}, Bx{02}, ...
	movdqu	xtmpl3, [tmp3+2*vec]	;Load array Cx{00}, Cx{01}, Cx{02}, ...

	XLDR	xd1, [dest1+tmp]	;Get next dest vector
	XLDR	xd2, [dest2+tmp]	;Get next dest vector
	XLDR	xtmph4, [dest3+tmp]	;Reuse xtmph1. Get next dest vector

	sub	len, pos

	movdqa	xtmpl4, [constip16]	;Load const of i + 16
	pinsrb	xtmph3, len.w, 15
	pshufb	xtmph3, xmask0f		;Broadcast len to all bytes
	pcmpgtb	xtmph3, xtmpl4

	XLDR	xtmpl4, [dest4+tmp]	;Get next dest vector

	movdqa	xtmpa, x0		;Keep unshifted copy of src
	psraw	x0, 4			;Shift to put high nibble into bits 4-0
	pand	x0, xmask0f		;Mask high src nibble in bits 4-0
	pand	xtmpa, xmask0f		;Mask low src nibble in bits 4-0

	; dest1
	pshufb	xtmph1, x0		;Lookup mul table of high nibble
	pshufb	xtmpl1, xtmpa		;Lookup mul table of low nibble
	pxor	xtmph1, xtmpl1		;GF add high and low partials
	pand	xtmph1, xtmph3
	pxor	xd1, xtmph1

	; dest2
	pshufb	xtmph2, x0		;Lookup mul table of high nibble
	pshufb	xtmpl2, xtmpa		;Lookup mul table of low nibble
	pxor	xtmph2, xtmpl2		;GF add high and low partials
	pand	xtmph2, xtmph3
	pxor	xd2, xtmph2

	; dest3
	pshufb	xgft3_hi, x0		;Lookup mul table of high nibble
	pshufb	xtmpl3, xtmpa		;Lookup mul table of low nibble
	pxor	xgft3_hi, xtmpl3	;GF add high and low partials
	pand	xgft3_hi, xtmph3
	pxor	xtmph4, xgft3_hi

	; dest4
	pshufb	xgft4_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft4_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft4_hi, xgft4_lo	;GF add high and low partials
	pand	xgft4_hi, xtmph3
	pxor	xtmpl4, xgft4_hi

	XSTR	[dest1+tmp], xd1	;Store result
	XSTR	[dest2+tmp], xd2	;Store result
	XSTR	[dest3+tmp], xtmph4	;Store result
	XSTR	[dest4+tmp], xtmpl4	;Store result

.return_pass:
	FUNC_RESTORE
	mov	return, 0
	ret

.return_fail:
	FUNC_RESTORE
	mov	return, 1
	ret

endproc_frame

section .data

align 16

mask0f:
	ddq 0x0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f
constip16:
	ddq 0xf0f1f2f3f4f5f6f7f8f9fafbfcfdfeff

%macro slversion 4
global %1_slver_%2%3%4
global %1_slver
%1_slver:
%1_slver_%2%3%4:
	dw 0x%4
	db 0x%3, 0x%2
%endmacro
;;;       func             core, ver, snum
slversion gf_4vect_mad_sse, 00,  00,  0209
