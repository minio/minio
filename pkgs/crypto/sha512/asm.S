#ifndef ENTRY
#define ENTRY(name) \
        .globl name             ; \
        .align 4,0x90           ; \
        name:
#endif

#ifndef END
#define END(name) \
        .size name, .-name
#endif

#ifndef ENDPROC
#define ENDPROC(name) \
        .type name, @function   ; \
        END(name)
#endif

#define NUM_INVALID		100

#define TYPE_R32		0
#define TYPE_R64		1
#define TYPE_XMM		2
#define TYPE_INVALID	100

	.macro R32_NUM opd r32
	\opd = NUM_INVALID
	.ifc \r32,%eax
	\opd = 0
	.endif
	.ifc \r32,%ecx
	\opd = 1
	.endif
	.ifc \r32,%edx
	\opd = 2
	.endif
	.ifc \r32,%ebx
	\opd = 3
	.endif
	.ifc \r32,%esp
	\opd = 4
	.endif
	.ifc \r32,%ebp
	\opd = 5
	.endif
	.ifc \r32,%esi
	\opd = 6
	.endif
	.ifc \r32,%edi
	\opd = 7
	.endif
#ifdef X86_64
	.ifc \r32,%r8d
	\opd = 8
	.endif
	.ifc \r32,%r9d
	\opd = 9
	.endif
	.ifc \r32,%r10d
	\opd = 10
	.endif
	.ifc \r32,%r11d
	\opd = 11
	.endif
	.ifc \r32,%r12d
	\opd = 12
	.endif
	.ifc \r32,%r13d
	\opd = 13
	.endif
	.ifc \r32,%r14d
	\opd = 14
	.endif
	.ifc \r32,%r15d
	\opd = 15
	.endif
#endif
	.endm

	.macro R64_NUM opd r64
	\opd = NUM_INVALID
#ifdef X86_64
	.ifc \r64,%rax
	\opd = 0
	.endif
	.ifc \r64,%rcx
	\opd = 1
	.endif
	.ifc \r64,%rdx
	\opd = 2
	.endif
	.ifc \r64,%rbx
	\opd = 3
	.endif
	.ifc \r64,%rsp
	\opd = 4
	.endif
	.ifc \r64,%rbp
	\opd = 5
	.endif
	.ifc \r64,%rsi
	\opd = 6
	.endif
	.ifc \r64,%rdi
	\opd = 7
	.endif
	.ifc \r64,%r8
	\opd = 8
	.endif
	.ifc \r64,%r9
	\opd = 9
	.endif
	.ifc \r64,%r10
	\opd = 10
	.endif
	.ifc \r64,%r11
	\opd = 11
	.endif
	.ifc \r64,%r12
	\opd = 12
	.endif
	.ifc \r64,%r13
	\opd = 13
	.endif
	.ifc \r64,%r14
	\opd = 14
	.endif
	.ifc \r64,%r15
	\opd = 15
	.endif
#endif
	.endm

	.macro XMM_NUM opd xmm
	\opd = NUM_INVALID
	.ifc \xmm,%xmm0
	\opd = 0
	.endif
	.ifc \xmm,%xmm1
	\opd = 1
	.endif
	.ifc \xmm,%xmm2
	\opd = 2
	.endif
	.ifc \xmm,%xmm3
	\opd = 3
	.endif
	.ifc \xmm,%xmm4
	\opd = 4
	.endif
	.ifc \xmm,%xmm5
	\opd = 5
	.endif
	.ifc \xmm,%xmm6
	\opd = 6
	.endif
	.ifc \xmm,%xmm7
	\opd = 7
	.endif
	.ifc \xmm,%xmm8
	\opd = 8
	.endif
	.ifc \xmm,%xmm9
	\opd = 9
	.endif
	.ifc \xmm,%xmm10
	\opd = 10
	.endif
	.ifc \xmm,%xmm11
	\opd = 11
	.endif
	.ifc \xmm,%xmm12
	\opd = 12
	.endif
	.ifc \xmm,%xmm13
	\opd = 13
	.endif
	.ifc \xmm,%xmm14
	\opd = 14
	.endif
	.ifc \xmm,%xmm15
	\opd = 15
	.endif
	.endm

	.macro TYPE type reg
	R32_NUM reg_type_r32 \reg
	R64_NUM reg_type_r64 \reg
	XMM_NUM reg_type_xmm \reg
	.if reg_type_r64 <> NUM_INVALID
	\type = TYPE_R64
	.elseif reg_type_r32 <> NUM_INVALID
	\type = TYPE_R32
	.elseif reg_type_xmm <> NUM_INVALID
	\type = TYPE_XMM
	.else
	\type = TYPE_INVALID
	.endif
	.endm

	.macro PFX_OPD_SIZE
	.byte 0x66
	.endm

	.macro PFX_REX opd1 opd2 W=0
	.if ((\opd1 | \opd2) & 8) || \W
	.byte 0x40 | ((\opd1 & 8) >> 3) | ((\opd2 & 8) >> 1) | (\W << 3)
	.endif
	.endm

	.macro MODRM mod opd1 opd2
	.byte \mod | (\opd1 & 7) | ((\opd2 & 7) << 3)
	.endm

	.macro PSHUFB_XMM xmm1 xmm2
	XMM_NUM pshufb_opd1 \xmm1
	XMM_NUM pshufb_opd2 \xmm2
	PFX_OPD_SIZE
	PFX_REX pshufb_opd1 pshufb_opd2
	.byte 0x0f, 0x38, 0x00
	MODRM 0xc0 pshufb_opd1 pshufb_opd2
	.endm

	.macro PCLMULQDQ imm8 xmm1 xmm2
	XMM_NUM clmul_opd1 \xmm1
	XMM_NUM clmul_opd2 \xmm2
	PFX_OPD_SIZE
	PFX_REX clmul_opd1 clmul_opd2
	.byte 0x0f, 0x3a, 0x44
	MODRM 0xc0 clmul_opd1 clmul_opd2
	.byte \imm8
	.endm

	.macro PEXTRD imm8 xmm gpr
	R32_NUM extrd_opd1 \gpr
	XMM_NUM extrd_opd2 \xmm
	PFX_OPD_SIZE
	PFX_REX extrd_opd1 extrd_opd2
	.byte 0x0f, 0x3a, 0x16
	MODRM 0xc0 extrd_opd1 extrd_opd2
	.byte \imm8
	.endm

	.macro MOVQ_R64_XMM opd1 opd2
	TYPE movq_r64_xmm_opd1_type \opd1
	.if movq_r64_xmm_opd1_type == TYPE_XMM
	XMM_NUM movq_r64_xmm_opd1 \opd1
	R64_NUM movq_r64_xmm_opd2 \opd2
	.else
	R64_NUM movq_r64_xmm_opd1 \opd1
	XMM_NUM movq_r64_xmm_opd2 \opd2
	.endif
	PFX_OPD_SIZE
	PFX_REX movq_r64_xmm_opd1 movq_r64_xmm_opd2 1
	.if movq_r64_xmm_opd1_type == TYPE_XMM
	.byte 0x0f, 0x7e
	.else
	.byte 0x0f, 0x6e
	.endif
	MODRM 0xc0 movq_r64_xmm_opd1 movq_r64_xmm_opd2
	.endm
