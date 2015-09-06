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
;;; gf_5vect_mad_avx(len, vec, vec_i, mul_array, src, dest);
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
 %define tmp4   r14
 %define return rax
 %define return.w eax
 %define stack_size 16*10 + 5*8
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
	save_reg	r14,  10*16 + 2*8
	save_reg	r15,  10*16 + 3*8
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
	mov	r14,  [rsp + 10*16 + 2*8]
	mov	r15,  [rsp + 10*16 + 3*8]
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
 %define tmp4   r13
 %define return rax
 %define return.w eax

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
	push	r13
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r13
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
 %define tmp4   r13
 %define return rax
 %define return.w eax

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

;;; gf_5vect_mad_avx(len, vec, vec_i, mul_array, src, dest)
%define len   arg0
%define len.w arg0.w
%define vec    arg1
%define vec_i    arg2
%define mul_array arg3
%define	src   arg4
%define dest1  arg5
%define pos   return
%define pos.w return.w

%define dest2 tmp4
%define dest3 mul_array
%define dest4 tmp2
%define dest5 vec_i


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

%define xmask0f  xmm15
%define xgft5_hi xmm14
%define xgft4_lo xmm13
%define xgft4_hi xmm12

%define x0      xmm0
%define xtmpa   xmm1
%define xtmph1  xmm2
%define xtmpl1  xmm3
%define xtmph2  xmm4
%define xtmpl2  xmm5
%define xtmph3  xmm6
%define xtmpl3  xmm7
%define xtmph5  xmm8
%define xtmpl5  xmm9
%define xd1     xmm10
%define xd2     xmm11
%define xd3     xtmpl1
%define xd4     xtmph1
%define xd5     xtmpl2


align 16
global gf_5vect_mad_avx:function
func(gf_5vect_mad_avx)
	FUNC_SAVE
	sub	len, 16
	jl	.return_fail
	xor	pos, pos
	vmovdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte
	mov	tmp, vec
	sal	vec_i, 5		;Multiply by 32
	lea	tmp3, [mul_array + vec_i]
	sal	tmp, 6			;Multiply by 64
	vmovdqu	xgft5_hi, [tmp3+2*tmp+16]	;     "     Ex{00}, Ex{10}, ..., Ex{f0}
	sal	vec, 5			;Multiply by 32
	add	tmp, vec
	vmovdqu	xgft4_hi, [tmp3+tmp+16]	; " Dx{00}, Dx{10}, Dx{20}, ... , Dx{f0}
	vmovdqu	xgft4_lo, [tmp3+tmp]	;Load array Dx{00}, Dx{01}, Dx{02}, ...

	mov	dest3, [dest1+2*PS]	; reuse mul_array
	mov	dest4, [dest1+3*PS]
	mov	dest5, [dest1+4*PS]	; reuse vec_i
	mov	dest2, [dest1+PS]
	mov	dest1, [dest1]

.loop16:
	XLDR	x0, [src+pos]		;Get next source vector

	vmovdqu	xtmph1, [tmp3+16]	; " Ax{00}, Ax{10}, Ax{20}, ... , Ax{f0}
	vmovdqu	xtmpl1, [tmp3]		;Load array Ax{00}, Ax{01}, Ax{02}, ...
	vmovdqu	xtmph2, [tmp3+vec+16]	; " Bx{00}, Bx{10}, Bx{20}, ... , Bx{f0}
	vmovdqu	xtmpl2, [tmp3+vec]	;Load array Bx{00}, Bx{01}, Bx{02}, ...
	vmovdqu	xtmph3, [tmp3+2*vec+16]	; " Cx{00}, Cx{10}, Cx{20}, ... , Cx{f0}
	vmovdqu	xtmpl3, [tmp3+2*vec]	;Load array Cx{00}, Cx{01}, Cx{02}, ...
	vmovdqu	xtmpl5, [tmp3+4*vec]	;Load array Ex{00}, Ex{01}, ..., Ex{0f}

	XLDR	xd1, [dest1+pos]	;Get next dest vector
	XLDR	xd2, [dest2+pos]	;Get next dest vector

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	; dest1
	vpshufb	xtmph1, xtmph1, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl1, xtmpl1, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph1, xtmph1, xtmpl1		;GF add high and low partials
	vpxor	xd1, xd1, xtmph1

	XLDR	xd3, [dest3+pos]	;Reuse xtmpl1, Get next dest vector
	XLDR	xd4, [dest4+pos]	;Reuse xtmph1, Get next dest vector

	; dest2
	vpshufb	xtmph2, xtmph2, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl2, xtmpl2, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph2, xtmph2, xtmpl2		;GF add high and low partials
	vpxor	xd2, xd2, xtmph2

	XLDR	xd5, [dest5+pos]	;Reuse xtmpl2. Get next dest vector

	; dest3
	vpshufb	xtmph3, xtmph3, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl3, xtmpl3, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph3, xtmph3, xtmpl3		;GF add high and low partials
	vpxor	xd3, xd3, xtmph3

	; dest4
	vpshufb	xtmph2, xgft4_hi, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl3, xgft4_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph2, xtmph2, xtmpl3		;GF add high and low partials
	vpxor	xd4, xd4, xtmph2

	; dest5
	vpshufb	xtmph5, xgft5_hi, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl5, xtmpl5, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph5, xtmph5, xtmpl5	;GF add high and low partials
	vpxor	xd5, xd5, xtmph5

	XSTR	[dest1+pos], xd1	;Store result into dest1
	XSTR	[dest2+pos], xd2	;Store result into dest2
	XSTR	[dest3+pos], xd3	;Store result into dest3
	XSTR	[dest4+pos], xd4	;Store result into dest4
	XSTR	[dest5+pos], xd5	;Store result into dest5

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

	sub	len, pos

	vmovdqa	xtmph1, [constip16]	;Load const of i + 16
	vpinsrb	xtmph5, len.w, 15
	vpshufb	xtmph5, xmask0f		;Broadcast len to all bytes
	vpcmpgtb	xtmph5, xtmph5, xtmph1

	vmovdqu	xtmph1, [tmp3+16]	; " Ax{00}, Ax{10}, Ax{20}, ... , Ax{f0}
	vmovdqu	xtmpl1, [tmp3]		;Load array Ax{00}, Ax{01}, Ax{02}, ...
	vmovdqu	xtmph2, [tmp3+vec+16]	; " Bx{00}, Bx{10}, Bx{20}, ... , Bx{f0}
	vmovdqu	xtmpl2, [tmp3+vec]	;Load array Bx{00}, Bx{01}, Bx{02}, ...
	vmovdqu	xtmph3, [tmp3+2*vec+16]	; " Cx{00}, Cx{10}, Cx{20}, ... , Cx{f0}
	vmovdqu	xtmpl3, [tmp3+2*vec]	;Load array Cx{00}, Cx{01}, Cx{02}, ...
	vmovdqu	xtmpl5, [tmp3+4*vec]	;Load array Ex{00}, Ex{01}, ..., Ex{0f}

	XLDR	xd1, [dest1+tmp]	;Get next dest vector
	XLDR	xd2, [dest2+tmp]	;Get next dest vector

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	; dest1
	vpshufb	xtmph1, xtmph1, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl1, xtmpl1, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph1, xtmph1, xtmpl1		;GF add high and low partials
	vpand	xtmph1, xtmph1, xtmph5
	vpxor	xd1, xd1, xtmph1

	XLDR	xd3, [dest3+tmp]	;Reuse xtmpl1, Get next dest vector
	XLDR	xd4, [dest4+tmp]	;Reuse xtmph1, Get next dest vector

	; dest2
	vpshufb	xtmph2, xtmph2, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl2, xtmpl2, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph2, xtmph2, xtmpl2		;GF add high and low partials
	vpand	xtmph2, xtmph2, xtmph5
	vpxor	xd2, xd2, xtmph2

	XLDR	xd5, [dest5+tmp]	;Reuse xtmpl2. Get next dest vector

	; dest3
	vpshufb	xtmph3, xtmph3, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl3, xtmpl3, xtmpa		;Lookup mul table of low nibble
	vpxor	xtmph3, xtmph3, xtmpl3		;GF add high and low partials
	vpand	xtmph3, xtmph3, xtmph5
	vpxor	xd3, xd3, xtmph3

	; dest4
	vpshufb	xgft4_hi, xgft4_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft4_lo, xgft4_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft4_hi, xgft4_hi, xgft4_lo		;GF add high and low partials
	vpand	xgft4_hi, xgft4_hi, xtmph5
	vpxor	xd4, xd4, xgft4_hi

	; dest5
	vpshufb	xgft5_hi, xgft5_hi, x0		;Lookup mul table of high nibble
	vpshufb	xtmpl5, xtmpl5, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft5_hi, xgft5_hi, xtmpl5	;GF add high and low partials
	vpand	xgft5_hi, xgft5_hi, xtmph5
	vpxor	xd5, xd5, xgft5_hi

	XSTR	[dest1+tmp], xd1	;Store result into dest1
	XSTR	[dest2+tmp], xd2	;Store result into dest2
	XSTR	[dest3+tmp], xd3	;Store result into dest3
	XSTR	[dest4+tmp], xd4	;Store result into dest4
	XSTR	[dest5+tmp], xd5	;Store result into dest5

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
mask0f:	ddq 0x0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f
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
slversion gf_5vect_mad_avx, 02,  00,  020d
