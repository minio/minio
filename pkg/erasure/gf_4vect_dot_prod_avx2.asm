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
;;; gf_4vect_dot_prod_avx2(len, vec, *g_tbls, **buffs, **dests);
;;;

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
 %define tmp5  r14		; must be saved and restored
 %define tmp6  r15		; must be saved and restored
 %define return rax
 %macro  SLDR   2
 %endmacro
 %define SSTR   SLDR
 %define PS     8
 %define LOG_PS 3

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
	push	r13
	push	r14
	push	r15
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r15
	pop	r14
	pop	r13
	pop	r12
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, macho64
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
 %define tmp5  r14		; must be saved and restored
 %define tmp6  r15		; must be saved and restored
 %define return rax
 %macro  SLDR   2
 %endmacro
 %define SSTR   SLDR
 %define PS     8
 %define LOG_PS 3

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
	push	r13
	push	r14
	push	r15
 %endmacro
 %macro FUNC_RESTORE 0
	pop	r15
	pop	r14
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
 %define tmp5   rdi		; must be saved and restored
 %define tmp6   rsi		; must be saved and restored
 %define return rax
 %macro  SLDR   2
 %endmacro
 %define SSTR   SLDR
 %define PS     8
 %define LOG_PS 3
 %define stack_size  9*16 + 7*8		; must be an odd multiple of 8
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
	vmovdqa	[rsp + 6*16], xmm12
	vmovdqa	[rsp + 7*16], xmm13
	vmovdqa	[rsp + 8*16], xmm14
	save_reg	r12,  9*16 + 0*8
	save_reg	r13,  9*16 + 1*8
	save_reg	r14,  9*16 + 2*8
	save_reg	r15,  9*16 + 3*8
	save_reg	rdi,  9*16 + 4*8
	save_reg	rsi,  9*16 + 5*8
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
	vmovdqa	xmm12, [rsp + 6*16]
	vmovdqa	xmm13, [rsp + 7*16]
	vmovdqa	xmm14, [rsp + 8*16]
	mov	r12,  [rsp + 9*16 + 0*8]
	mov	r13,  [rsp + 9*16 + 1*8]
	mov	r14,  [rsp + 9*16 + 2*8]
	mov	r15,  [rsp + 9*16 + 3*8]
	mov	rdi,  [rsp + 9*16 + 4*8]
	mov	rsi,  [rsp + 9*16 + 5*8]
	add	rsp, stack_size
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, elf32

;;;================== High Address;
;;;	arg4
;;;	arg3
;;;	arg2
;;;	arg1
;;;	arg0
;;;	return
;;;<================= esp of caller
;;;	ebp
;;;<================= ebp = esp
;;;	var0
;;;	var1
;;;	var2
;;;	var3
;;;	esi
;;;	edi
;;;	ebx
;;;<================= esp of callee
;;;
;;;================== Low Address;

 %define PS     4
 %define LOG_PS 2
 %define func(x) x:
 %define arg(x) [ebp + PS*2 + PS*x]
 %define var(x) [ebp - PS - PS*x]

 %define trans	 ecx
 %define trans2  esi
 %define arg0	 trans		;trans and trans2 are for the variables in stack
 %define arg0_m	 arg(0)
 %define arg1	 ebx
 %define arg2	 arg2_m
 %define arg2_m	 arg(2)
 %define arg3	 trans
 %define arg3_m	 arg(3)
 %define arg4	 trans
 %define arg4_m	 arg(4)
 %define arg5	 trans2
 %define tmp	 edx
 %define tmp.w   edx
 %define tmp.b   dl
 %define tmp2	 edi
 %define tmp3	 trans2
 %define tmp3_m	 var(0)
 %define tmp4	 trans2
 %define tmp4_m	 var(1)
 %define tmp5	 trans2
 %define tmp5_m	 var(2)
 %define tmp6	 trans2
 %define tmp6_m	 var(3)
 %define return	 eax
 %macro SLDR 2				;stack load/restore
	mov %1, %2
 %endmacro
 %define SSTR SLDR

 %macro FUNC_SAVE 0
	push	ebp
	mov	ebp, esp
	sub	esp, PS*4		;4 local variables
	push	esi
	push	edi
	push	ebx
	mov	arg1, arg(1)
 %endmacro

 %macro FUNC_RESTORE 0
	pop	ebx
	pop	edi
	pop	esi
	add	esp, PS*4		;4 local variables
	pop	ebp
 %endmacro

%endif	; output formats

%define len    arg0
%define vec    arg1
%define mul_array arg2
%define	src    arg3
%define dest1  arg4
%define ptr    arg5
%define vec_i  tmp2
%define dest2  tmp3
%define dest3  tmp4
%define dest4  tmp5
%define vskip3 tmp6
%define pos    return

 %ifidn PS,4				;32-bit code
	%define  len_m 	arg0_m
	%define  src_m 	arg3_m
	%define  dest1_m arg4_m
	%define  dest2_m tmp3_m
	%define  dest3_m tmp4_m
	%define  dest4_m tmp5_m
	%define  vskip3_m tmp6_m
 %endif

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

%ifidn PS,8				;64-bit code
 default rel
  [bits 64]
%endif


section .text

%ifidn PS,8				;64-bit code
 %define xmask0f   ymm14
 %define xmask0fx  xmm14
 %define xgft1_lo  ymm13
 %define xgft1_hi  ymm12
 %define xgft2_lo  ymm11
 %define xgft2_hi  ymm10
 %define xgft3_lo  ymm9
 %define xgft3_hi  ymm8
 %define xgft4_lo  ymm7
 %define xgft4_hi  ymm6

 %define x0     ymm0
 %define xtmpa  ymm1
 %define xp1    ymm2
 %define xp2    ymm3
 %define xp3    ymm4
 %define xp4    ymm5
%else
 %define ymm_trans ymm7			;reuse xmask0f and xgft1_hi
 %define xmask0f   ymm_trans
 %define xmask0fx  xmm7
 %define xgft1_lo  ymm6
 %define xgft1_hi  ymm_trans
 %define xgft2_lo  xgft1_lo
 %define xgft2_hi  xgft1_hi
 %define xgft3_lo  xgft1_lo
 %define xgft3_hi  xgft1_hi
 %define xgft4_lo  xgft1_lo
 %define xgft4_hi  xgft1_hi

 %define x0     ymm0
 %define xtmpa  ymm1
 %define xp1    ymm2
 %define xp2    ymm3
 %define xp3    ymm4
 %define xp4    ymm5
%endif
align 16
global gf_4vect_dot_prod_avx2:function
func(gf_4vect_dot_prod_avx2)
	FUNC_SAVE
	SLDR	len, len_m
	sub	len, 32
	SSTR	len_m, len
	jl	.return_fail
	xor	pos, pos
	mov	tmp.b, 0x0f
	vpinsrb	xmask0fx, xmask0fx, tmp.w, 0
	vpbroadcastb xmask0f, xmask0fx	;Construct mask 0x0f0f0f...
	mov	vskip3, vec
	imul	vskip3, 96
	SSTR	vskip3_m, vskip3
	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	SLDR	dest1, dest1_m
	mov	dest2, [dest1+PS]
	SSTR	dest2_m, dest2
	mov	dest3, [dest1+2*PS]
	SSTR	dest3_m, dest3
	mov	dest4, [dest1+3*PS]
	SSTR	dest4_m, dest4
	mov	dest1, [dest1]
	SSTR	dest1_m, dest1

.loop32:
	vpxor	xp1, xp1
	vpxor	xp2, xp2
	vpxor	xp3, xp3
	vpxor	xp4, xp4
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect:
	SLDR	src, src_m
	mov	ptr, [src+vec_i]
	XLDR	x0, [ptr+pos]		;Get next source vector

	add	vec_i, PS
 %ifidn PS,8				;64-bit code
	vpand	xgft4_lo, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0
	vperm2i128 xtmpa, xgft4_lo, x0, 0x30 	;swap xtmpa from 1lo|2lo to 1lo|2hi
	vperm2i128 x0, xgft4_lo, x0, 0x12	;swap x0 from    1hi|2hi to 1hi|2lo

	vmovdqu	xgft1_lo, [tmp]			;Load array Ax{00}, Ax{01}, ..., Ax{0f}
						;     "     Ax{00}, Ax{10}, ..., Ax{f0}
	vmovdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
						;     "     Bx{00}, Bx{10}, ..., Bx{f0}
	vmovdqu	xgft3_lo, [tmp+vec*(64/PS)]	;Load array Cx{00}, Cx{01}, ..., Cx{0f}
						;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	vmovdqu	xgft4_lo, [tmp+vskip3]		;Load array Dx{00}, Dx{01}, ..., Dx{0f}
						;     "     Dx{00}, Dx{10}, ..., Dx{f0}

	vperm2i128 xgft1_hi, xgft1_lo, xgft1_lo, 0x01 ; swapped to hi | lo
	vperm2i128 xgft2_hi, xgft2_lo, xgft2_lo, 0x01 ; swapped to hi | lo
	vperm2i128 xgft3_hi, xgft3_lo, xgft3_lo, 0x01 ; swapped to hi | lo
	vperm2i128 xgft4_hi, xgft4_lo, xgft4_lo, 0x01 ; swapped to hi | lo
	add	tmp, 32
 %else					;32-bit code
	mov	cl, 0x0f		;use ecx as a temp variable
	vpinsrb	xmask0fx, xmask0fx, ecx, 0
	vpbroadcastb xmask0f, xmask0fx	;Construct mask 0x0f0f0f...

	vpand	xgft4_lo, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0
	vperm2i128 xtmpa, xgft4_lo, x0, 0x30 	;swap xtmpa from 1lo|2lo to 1lo|2hi
	vperm2i128 x0, xgft4_lo, x0, 0x12	;swap x0 from    1hi|2hi to 1hi|2lo

	vmovdqu	xgft1_lo, [tmp]			;Load array Ax{00}, Ax{01}, ..., Ax{0f}
						;     "     Ax{00}, Ax{10}, ..., Ax{f0}
	vperm2i128 xgft1_hi, xgft1_lo, xgft1_lo, 0x01 ; swapped to hi | lo
 %endif

	vpshufb	xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft1_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft1_hi, xgft1_lo	;GF add high and low partials
	vpxor	xp1, xgft1_hi		;xp1 += partial

 %ifidn PS,4				; 32-bit code
	vmovdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
						;     "     Bx{00}, Bx{10}, ..., Bx{f0}
	vperm2i128 xgft2_hi, xgft2_lo, xgft2_lo, 0x01 ; swapped to hi | lo
 %endif
	vpshufb	xgft2_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft2_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft2_hi, xgft2_lo	;GF add high and low partials
	vpxor	xp2, xgft2_hi		;xp2 += partial

 %ifidn PS,4				; 32-bit code
	sal     vec, 1
	vmovdqu	xgft3_lo, [tmp+vec*(32/PS)]	;Load array Cx{00}, Cx{01}, ..., Cx{0f}
						;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	vperm2i128 xgft3_hi, xgft3_lo, xgft3_lo, 0x01 ; swapped to hi | lo
	sar	vec, 1
 %endif
	vpshufb	xgft3_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft3_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft3_hi, xgft3_lo	;GF add high and low partials
	vpxor	xp3, xgft3_hi		;xp3 += partial

 %ifidn PS,4				; 32-bit code
	SLDR	vskip3, vskip3_m
	vmovdqu	xgft4_lo, [tmp+vskip3]		;Load array Dx{00}, Dx{01}, ..., Dx{0f}
						;     "     DX{00}, Dx{10}, ..., Dx{f0}
	vperm2i128 xgft4_hi, xgft4_lo, xgft4_lo, 0x01 ; swapped to hi | lo
	add	tmp, 32
 %endif
	vpshufb	xgft4_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft4_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft4_hi, xgft4_lo	;GF add high and low partials
	vpxor	xp4, xgft4_hi		;xp4 += partial

	cmp	vec_i, vec
	jl	.next_vect

	SLDR	dest1, dest1_m
	SLDR	dest2, dest2_m
	XSTR	[dest1+pos], xp1
	XSTR	[dest2+pos], xp2
	SLDR	dest3, dest3_m
	XSTR	[dest3+pos], xp3
	SLDR	dest4, dest4_m
	XSTR	[dest4+pos], xp4

	SLDR	len, len_m
	add	pos, 32			;Loop on 32 bytes at a time
	cmp	pos, len
	jle	.loop32

	lea	tmp, [len + 32]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-32
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
slversion gf_4vect_dot_prod_avx2, 04,  04,  0198
