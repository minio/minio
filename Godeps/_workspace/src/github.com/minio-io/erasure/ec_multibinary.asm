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

%ifidn __OUTPUT_FORMAT__, elf64
 %define WRT_OPT		wrt ..plt
%else
 %define WRT_OPT
%endif

%ifidn __OUTPUT_FORMAT__, macho64
 %define EC_ENCODE_DATA_SSE _ec_encode_data_sse
 %define EC_ENCODE_DATA_AVX _ec_encode_data_avx
 %define EC_ENCODE_DATA_AVX2 _ec_encode_data_avx2
 %define EC_ENCODE_DATA_BASE _ec_encode_data_base
 %define EC_ENCODE_DATA _ec_encode_data

 %define EC_ENCODE_DATA_UPDATE_BASE _ec_encode_data_update_base
 %define EC_ENCODE_DATA_UPDATE_SSE _ec_encode_data_update_sse
 %define EC_ENCODE_DATA_UPDATE_AVX _ec_encode_data_update_avx
 %define EC_ENCODE_DATA_UPDATE_AVX2 _ec_encode_data_update_avx2

 %define GF_VECT_MAD_BASE _gf_vect_mad_base
 %define GF_VECT_MAD_SSE _gf_vect_mad_sse
 %define GF_VECT_MAD_AVX _gf_vect_mad_avx
 %define GF_VECT_MAD_AVX2 _gf_vect_mad_avx2

 %define GF_VECT_MUL_SSE _gf_vect_mul_sse
 %define GF_VECT_MUL_AVX _gf_vect_mul_avx
 %define GF_VECT_MUL_BASE _gf_vect_mul_base
 %define GF_VECT_MUL _gf_vect_mul

 %define GF_VECT_DOT_PROD_SSE _gf_vect_dot_prod_sse
 %define GF_VECT_DOT_PROD_AVX _gf_vect_dot_prod_avx
 %define GF_VECT_DOT_PROD_AVX2 _gf_vect_dot_prod_avx2
 %define GF_VECT_DOT_PROD_BASE _gf_vect_dot_prod_base
 %define GF_VECT_DOT_PROD _gf_vect_dot_prod

%else
 %define EC_ENCODE_DATA_SSE ec_encode_data_sse
 %define EC_ENCODE_DATA_AVX ec_encode_data_avx
 %define EC_ENCODE_DATA_AVX2 ec_encode_data_avx2
 %define EC_ENCODE_DATA_BASE ec_encode_data_base
 %define EC_ENCODE_DATA ec_encode_data

 %define EC_ENCODE_DATA_UPDATE_BASE ec_encode_data_update_base
 %define EC_ENCODE_DATA_UPDATE_SSE ec_encode_data_update_sse
 %define EC_ENCODE_DATA_UPDATE_AVX ec_encode_data_update_avx
 %define EC_ENCODE_DATA_UPDATE_AVX2 ec_encode_data_update_avx2

 %define GF_VECT_MAD_BASE gf_vect_mad_base
 %define GF_VECT_MAD_SSE gf_vect_mad_sse
 %define GF_VECT_MAD_AVX gf_vect_mad_avx
 %define GF_VECT_MAD_AVX2 gf_vect_mad_avx2

 %define GF_VECT_MUL_SSE gf_vect_mul_sse
 %define GF_VECT_MUL_AVX gf_vect_mul_avx
 %define GF_VECT_MUL_BASE gf_vect_mul_base
 %define GF_VECT_MUL gf_vect_mul

 %define GF_VECT_DOT_PROD_SSE gf_vect_dot_prod_sse
 %define GF_VECT_DOT_PROD_AVX gf_vect_dot_prod_avx
 %define GF_VECT_DOT_PROD_AVX2 gf_vect_dot_prod_avx2
 %define GF_VECT_DOT_PROD_BASE gf_vect_dot_prod_base
 %define GF_VECT_DOT_PROD gf_vect_dot_prod

%endif

%include "ec_reg_sizes.asm"

%ifidn __OUTPUT_FORMAT__, elf32

[bits 32]

 %define def_wrd		dd
 %define wrd_sz  	dword
 %define arg1		esi
 %define arg2		eax
 %define arg3		ebx
 %define arg4		ecx
 %define arg5		edx

%else

 default rel
 [bits 64]

 %define def_wrd 	dq
 %define wrd_sz  	qword
 %define arg1		rsi
 %define arg2		rax
 %define arg3		rbx
 %define arg4		rcx
 %define arg5		rdx


 extern EC_ENCODE_DATA_UPDATE_SSE
 extern EC_ENCODE_DATA_UPDATE_AVX
 extern EC_ENCODE_DATA_UPDATE_AVX2
 extern GF_VECT_MUL_SSE
 extern GF_VECT_MUL_AVX

 extern GF_VECT_MAD_SSE
 extern GF_VECT_MAD_AVX
 extern GF_VECT_MAD_AVX2
%endif

extern GF_VECT_MUL_BASE
extern EC_ENCODE_DATA_BASE
extern EC_ENCODE_DATA_UPDATE_BASE
extern GF_VECT_DOT_PROD_BASE
extern GF_VECT_MAD_BASE

extern GF_VECT_DOT_PROD_SSE
extern GF_VECT_DOT_PROD_AVX
extern GF_VECT_DOT_PROD_AVX2
extern EC_ENCODE_DATA_SSE
extern EC_ENCODE_DATA_AVX
extern EC_ENCODE_DATA_AVX2


section .data
;;; *_mbinit are initial values for *_dispatched; is updated on first call.
;;; Therefore, *_dispatch_init is only executed on first call.

ec_encode_data_dispatched:
	def_wrd      ec_encode_data_mbinit

gf_vect_mul_dispatched:
	def_wrd      gf_vect_mul_mbinit

gf_vect_dot_prod_dispatched:
	def_wrd      gf_vect_dot_prod_mbinit

ec_encode_data_update_dispatched:
	def_wrd      ec_encode_data_update_mbinit

gf_vect_mad_dispatched:
	def_wrd      gf_vect_mad_mbinit

section .text
;;;;
; ec_encode_data multibinary function
;;;;
global EC_ENCODE_DATA:function
ec_encode_data_mbinit:
	call	ec_encode_data_dispatch_init

EC_ENCODE_DATA:
	jmp	wrd_sz [ec_encode_data_dispatched]

ec_encode_data_dispatch_init:
	push    arg1
	push    arg2
	push    arg3
	push    arg4
	push    arg5
	lea     arg1, [EC_ENCODE_DATA_BASE WRT_OPT] ; Default

	mov     eax, 1
	cpuid
	lea     arg3, [EC_ENCODE_DATA_SSE WRT_OPT]
	test    ecx, FLAG_CPUID1_ECX_SSE4_1
	cmovne  arg1, arg3

	and	ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	cmp	ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	lea	arg3, [EC_ENCODE_DATA_AVX WRT_OPT]

	jne	_done_ec_encode_data_init
	mov	arg1, arg3

	;; Try for AVX2
	xor	ecx, ecx
	mov	eax, 7
	cpuid
	test	ebx, FLAG_CPUID1_EBX_AVX2
	lea     arg3, [EC_ENCODE_DATA_AVX2 WRT_OPT]
	cmovne	arg1, arg3
	;; Does it have xmm and ymm support
	xor	ecx, ecx
	xgetbv
	and	eax, FLAG_XGETBV_EAX_XMM_YMM
	cmp	eax, FLAG_XGETBV_EAX_XMM_YMM
	je	_done_ec_encode_data_init
	lea     arg1, [EC_ENCODE_DATA_SSE WRT_OPT]

_done_ec_encode_data_init:
	pop     arg5
	pop     arg4
	pop     arg3
	pop     arg2
	mov     [ec_encode_data_dispatched], arg1
	pop     arg1
	ret

;;;;
; gf_vect_mul multibinary function
;;;;
global GF_VECT_MUL:function
gf_vect_mul_mbinit:
	call    gf_vect_mul_dispatch_init

GF_VECT_MUL:
	jmp	wrd_sz [gf_vect_mul_dispatched]

gf_vect_mul_dispatch_init:
	push    arg1
%ifidn __OUTPUT_FORMAT__, elf32		;; 32-bit check
	lea     arg1, [GF_VECT_MUL_BASE]
%else
	push    rax
	push    rbx
	push    rcx
	push    rdx
	lea     arg1, [GF_VECT_MUL_BASE WRT_OPT] ; Default

	mov     eax, 1
	cpuid
	test    ecx, FLAG_CPUID1_ECX_SSE4_2
	lea     rbx, [GF_VECT_MUL_SSE WRT_OPT]
	je	_done_gf_vect_mul_dispatch_init
	mov  	arg1, rbx

	;; Try for AVX
	and     ecx, (FLAG_CPUID1_ECX_OSXSAVE | FLAG_CPUID1_ECX_AVX)
	cmp     ecx, (FLAG_CPUID1_ECX_OSXSAVE | FLAG_CPUID1_ECX_AVX)
	jne     _done_gf_vect_mul_dispatch_init

	;; Does it have xmm and ymm support
	xor     ecx, ecx
	xgetbv
	and     eax, FLAG_XGETBV_EAX_XMM_YMM
	cmp     eax, FLAG_XGETBV_EAX_XMM_YMM
	jne     _done_gf_vect_mul_dispatch_init
	lea     arg1, [GF_VECT_MUL_AVX WRT_OPT]

_done_gf_vect_mul_dispatch_init:
	pop     rdx
	pop     rcx
	pop     rbx
	pop     rax
%endif			;; END 32-bit check
	mov     [gf_vect_mul_dispatched], arg1
	pop     arg1
	ret

;;;;
; ec_encode_data_update multibinary function
;;;;
global EC_ENCODE_DATA_UPDATE:function
ec_encode_data_update_mbinit:
	call	ec_encode_data_update_dispatch_init

EC_ENCODE_DATA_UPDATE:
	jmp	wrd_sz [ec_encode_data_update_dispatched]

ec_encode_data_update_dispatch_init:
	push    arg1
%ifidn __OUTPUT_FORMAT__, elf32		;; 32-bit check
	lea     arg1, [ec_encode_data_update_base]
%else
	push    rax
	push    rbx
	push    rcx
	push    rdx
	lea     arg1, [EC_ENCODE_DATA_UPDATE_BASE WRT_OPT] ; Default

	mov     eax, 1
	cpuid
	lea     rbx, [EC_ENCODE_DATA_UPDATE_SSE WRT_OPT]
	test    ecx, FLAG_CPUID1_ECX_SSE4_1
	cmovne  arg1, rbx

	and	ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	cmp	ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	lea	rbx, [EC_ENCODE_DATA_UPDATE_AVX WRT_OPT]

	jne	_done_ec_encode_data_update_init
	mov	rsi, rbx

	;; Try for AVX2
	xor	ecx, ecx
	mov	eax, 7
	cpuid
	test	ebx, FLAG_CPUID1_EBX_AVX2
	lea     rbx, [EC_ENCODE_DATA_UPDATE_AVX2 WRT_OPT]
	cmovne	rsi, rbx

	;; Does it have xmm and ymm support
	xor	ecx, ecx
	xgetbv
	and	eax, FLAG_XGETBV_EAX_XMM_YMM
	cmp	eax, FLAG_XGETBV_EAX_XMM_YMM
	je	_done_ec_encode_data_update_init
	lea     rsi, [EC_ENCODE_DATA_UPDATE_SSE WRT_OPT]

_done_ec_encode_data_update_init:
	pop     rdx
	pop     rcx
	pop     rbx
	pop     rax
%endif			;; END 32-bit check
	mov     [ec_encode_data_update_dispatched], arg1
	pop     arg1
	ret

;;;;
; gf_vect_dot_prod multibinary function
;;;;
global GF_VECT_DOT_PROD:function
gf_vect_dot_prod_mbinit:
	call    gf_vect_dot_prod_dispatch_init

GF_VECT_DOT_PROD:
	jmp     wrd_sz [gf_vect_dot_prod_dispatched]

gf_vect_dot_prod_dispatch_init:
	push    arg1
	push    arg2
	push    arg3
	push    arg4
	push    arg5
	lea     arg1, [GF_VECT_DOT_PROD_BASE WRT_OPT] ; Default

	mov     eax, 1
	cpuid
	lea     arg3, [GF_VECT_DOT_PROD_SSE WRT_OPT]
	test    ecx, FLAG_CPUID1_ECX_SSE4_1
	cmovne  arg1, arg3

	and		ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	cmp		ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	lea     arg3, [GF_VECT_DOT_PROD_AVX WRT_OPT]

	jne     _done_gf_vect_dot_prod_init
	mov		arg1, arg3

	;; Try for AVX2
	xor		ecx, ecx
	mov		eax, 7
	cpuid
	test	ebx, FLAG_CPUID1_EBX_AVX2
	lea     arg3, [GF_VECT_DOT_PROD_AVX2 WRT_OPT]
	cmovne	arg1, arg3
	;; Does it have xmm and ymm support
	xor	ecx, ecx
	xgetbv
	and	eax, FLAG_XGETBV_EAX_XMM_YMM
	cmp	eax, FLAG_XGETBV_EAX_XMM_YMM
	je	_done_gf_vect_dot_prod_init
	lea     arg1, [GF_VECT_DOT_PROD_SSE WRT_OPT]

_done_gf_vect_dot_prod_init:
	pop     arg5
	pop     arg4
	pop     arg3
	pop     arg2
	mov     [gf_vect_dot_prod_dispatched], arg1
	pop	arg1
	ret

;;;;
; gf_vect_mad multibinary function
;;;;
global GF_VECT_MAD:function
gf_vect_mad_mbinit:
	call    gf_vect_mad_dispatch_init

GF_VECT_MAD:
	jmp     wrd_sz [gf_vect_mad_dispatched]

gf_vect_mad_dispatch_init:
	push    arg1
%ifidn __OUTPUT_FORMAT__, elf32         ;; 32-bit check
	lea     arg1, [gf_vect_mad_base]
%else
	push	rax
	push	rbx
	push	rcx
	push	rdx
	lea     arg1, [GF_VECT_MAD_BASE WRT_OPT] ; Default

	mov     eax, 1
	cpuid
	lea     rbx, [GF_VECT_MAD_SSE WRT_OPT]
	test    ecx, FLAG_CPUID1_ECX_SSE4_1
	cmovne  arg1, rbx

	and	ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	cmp	ecx, (FLAG_CPUID1_ECX_AVX | FLAG_CPUID1_ECX_OSXSAVE)
	lea     rbx, [GF_VECT_MAD_AVX WRT_OPT]

	jne     _done_gf_vect_mad_init
	mov	rsi, rbx

	;; Try for AVX2
	xor	ecx, ecx
	mov	eax, 7
	cpuid
	test	ebx, FLAG_CPUID1_EBX_AVX2
	lea     rbx, [GF_VECT_MAD_AVX2 WRT_OPT]
	cmovne	rsi, rbx

	;; Does it have xmm and ymm support
	xor	ecx, ecx
	xgetbv
	and	eax, FLAG_XGETBV_EAX_XMM_YMM
	cmp	eax, FLAG_XGETBV_EAX_XMM_YMM
	je	_done_gf_vect_mad_init
	lea     rsi, [GF_VECT_MAD_SSE WRT_OPT]

_done_gf_vect_mad_init:
	pop     rdx
	pop     rcx
	pop     rbx
	pop     rax
%endif			;; END 32-bit check
	mov     [gf_vect_mad_dispatched], arg1
	pop	arg1
	ret

%macro slversion 4
global %1_slver_%2%3%4
global %1_slver
%1_slver:
%1_slver_%2%3%4:
	dw 0x%4
	db 0x%3, 0x%2
%endmacro

;;;       func                 		core, ver, snum
slversion EC_ENCODE_DATA,		00,   03,  0133
slversion GF_VECT_MUL,			00,   02,  0134
slversion EC_ENCODE_DATA_UPDATE,	00,   02,  0212
slversion GF_VECT_DOT_PROD,		00,   02,  0138
slversion GF_VECT_MAD,			00,   01,  0213
