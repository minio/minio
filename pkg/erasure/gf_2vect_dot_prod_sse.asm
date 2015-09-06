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
;;; gf_2vect_dot_prod_sse(len, vec, *g_tbls, **buffs, **dests);
;;;

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
 %define tmp4  r12		; must be saved and restored
 %define return rax
 %macro  SLDR 2
 %endmacro
 %define SSTR SLDR
 %define PS 8
 %define LOG_PS 3

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
 %endmacro
 %macro FUNC_RESTORE 0
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
 %define tmp    r11
 %define tmp2   r10
 %define tmp3   r13		; must be saved and restored
 %define tmp4   r14		; must be saved and restored
 %define return rax
 %macro  SLDR 2
 %endmacro
 %define SSTR SLDR
 %define PS     8
 %define LOG_PS 3
 %define stack_size  3*16 + 3*8 	; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + PS + PS*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	save_xmm128	xmm6, 0*16
	save_xmm128	xmm7, 1*16
	save_xmm128	xmm8, 2*16
	save_reg	r12,  3*16 + 0*8
	save_reg	r13,  3*16 + 1*8
	save_reg	r14,  3*16 + 2*8
	end_prolog
	mov	arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
	movdqa	xmm6, [rsp + 0*16]
	movdqa	xmm7, [rsp + 1*16]
	movdqa	xmm8, [rsp + 2*16]
	mov	r12,  [rsp + 3*16 + 0*8]
	mov	r13,  [rsp + 3*16 + 1*8]
	mov	r14,  [rsp + 3*16 + 2*8]
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
;;;	esi
;;;	edi
;;;	ebx
;;;<================= esp of callee
;;;
;;;================== Low Address;

 %define PS 4
 %define LOG_PS 2
 %define func(x) x:
 %define arg(x) [ebp + PS*2 + PS*x]
 %define var(x) [ebp - PS - PS*x]

 %define trans   ecx
 %define trans2  esi
 %define arg0    trans			;trans and trans2 are for the variables in stack
 %define arg0_m  arg(0)
 %define arg1    ebx
 %define arg2    arg2_m
 %define arg2_m  arg(2)
 %define arg3    trans
 %define arg3_m  arg(3)
 %define arg4    trans
 %define arg4_m  arg(4)
 %define tmp	 edx
 %define tmp2    edi
 %define tmp3    trans2
 %define tmp4    trans2
 %define tmp4_m  var(0)
 %define return  eax
 %macro SLDR 2	;; stack load/restore
	mov %1, %2
 %endmacro
 %define SSTR SLDR

 %macro FUNC_SAVE 0
	push	ebp
	mov	ebp, esp
	sub 	esp, PS*1		;1 local variable
	push	esi
	push	edi
	push	ebx
	mov	arg1, arg(1)
 %endmacro

 %macro FUNC_RESTORE 0
	pop	ebx
	pop	edi
	pop	esi
	add	esp, PS*1		;1 local variable
	pop	ebp
 %endmacro

%endif	; output formats

%define len   arg0
%define vec   arg1
%define mul_array arg2
%define	src   arg3
%define dest1  arg4

%define vec_i tmp2
%define ptr   tmp3
%define dest2 tmp4
%define pos   return

 %ifidn PS,4				;32-bit code
	%define  len_m 	 arg0_m
	%define  src_m 	 arg3_m
	%define  dest1_m arg4_m
	%define  dest2_m tmp4_m
 %endif

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

%ifidn PS,8				;64-bit code
 default rel
  [bits 64]
%endif

section .text

%ifidn PS,8				;64-bit code
 %define xmask0f   xmm8
 %define xgft1_lo  xmm7
 %define xgft1_hi  xmm6
 %define xgft2_lo  xmm5
 %define xgft2_hi  xmm4

 %define x0     xmm0
 %define xtmpa  xmm1
 %define xp1    xmm2
 %define xp2    xmm3
%else					;32-bit code
 %define xmask0f   xmm4
 %define xgft1_lo  xmm7
 %define xgft1_hi  xmm6
 %define xgft2_lo  xgft1_lo
 %define xgft2_hi  xgft1_hi

 %define x0     xmm0
 %define xtmpa  xmm1
 %define xp1    xmm2
 %define xp2    xmm3
%endif

align 16
global gf_2vect_dot_prod_sse:function
func(gf_2vect_dot_prod_sse)
	FUNC_SAVE
	SLDR 	len, len_m
	sub	len, 16
	SSTR	len_m, len
	jl	.return_fail
	xor	pos, pos
	movdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte
	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	SLDR 	dest1, dest1_m
	mov	dest2, [dest1+PS]
	SSTR 	dest2_m, dest2
	mov	dest1, [dest1]
	SSTR 	dest1_m, dest1

.loop16
	pxor	xp1, xp1
	pxor	xp2, xp2
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect
	SLDR 	src, src_m
	mov	ptr, [src+vec_i]

	movdqu	xgft1_lo, [tmp]		;Load array Ax{00}, Ax{01}, ..., Ax{0f}
	movdqu	xgft1_hi, [tmp+16]	;     "     Ax{00}, Ax{10}, ..., Ax{f0}
 %ifidn PS,8				;64-bit code
	movdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
	movdqu	xgft2_hi, [tmp+vec*(32/PS)+16]	;     "     Bx{00}, Bx{10}, ..., Bx{f0}
	add	tmp, 32
	add	vec_i, PS
 %endif
	XLDR	x0, [ptr+pos]		;Get next source vector

	movdqa	xtmpa, x0		;Keep unshifted copy of src
	psraw	x0, 4			;Shift to put high nibble into bits 4-0
	pand	x0, xmask0f		;Mask high src nibble in bits 4-0
	pand	xtmpa, xmask0f		;Mask low src nibble in bits 4-0

	pshufb	xgft1_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft1_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft1_hi, xgft1_lo	;GF add high and low partials
	pxor	xp1, xgft1_hi		;xp1 += partial

 %ifidn PS,4				;32-bit code
	movdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
	movdqu	xgft2_hi, [tmp+vec*(32/PS)+16]	;     "     Bx{00}, Bx{10}, ..., Bx{f0}

	add	tmp, 32
	add	vec_i, PS
 %endif
	pshufb	xgft2_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft2_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft2_hi, xgft2_lo	;GF add high and low partials
	pxor	xp2, xgft2_hi		;xp2 += partial

	cmp	vec_i, vec
	jl	.next_vect

	SLDR 	dest1, dest1_m
	SLDR 	dest2, dest2_m
	XSTR	[dest1+pos], xp1
	XSTR	[dest2+pos], xp2

	SLDR 	len, len_m
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
mask0f: ddq 0x0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f

%macro slversion 4
global %1_slver_%2%3%4
global %1_slver
%1_slver:
%1_slver_%2%3%4:
	dw 0x%4
	db 0x%3, 0x%2
%endmacro
;;;       func                  core, ver, snum
slversion gf_2vect_dot_prod_sse, 00,  03,  0062
