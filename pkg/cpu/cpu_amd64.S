/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") ;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef __APPLE__
#define HAS_SSE _has_sse41
#define HAS_AVX _has_avx
#define HAS_AVX2 _has_avx2
#else
#define HAS_SSE has_sse41
#define HAS_AVX has_avx
#define HAS_AVX2 has_avx2
#endif

	.file	"cpufeatures.c"
	.text
cpuid:
.LFB2:
	.cfi_startproc
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	pushq	%rbx
	.cfi_offset 3, -24
	movq	%rdi, -16(%rbp)
	movl	%esi, -20(%rbp)
	movq	-16(%rbp), %rax
	leaq	4(%rax), %r10
	movq	-16(%rbp), %rax
	leaq	8(%rax), %r9
	movq	-16(%rbp), %rax
	leaq	12(%rax), %r8
	movl	-20(%rbp), %eax
	movl	$0, %edx
	movl	%edx, %ecx
#APP
# 21 "cpufeatures.c" 1
	cpuid
# 0 "" 2
#NO_APP
	movl	%ebx, %esi
	movl	%eax, %edi
	movq	-16(%rbp), %rax
	movl	%edi, (%rax)
	movl	%esi, (%r10)
	movl	%ecx, (%r9)
	movl	%edx, (%r8)
	popq	%rbx
	popq	%rbp
	.cfi_def_cfa 7, 8
	ret
	.cfi_endproc
.LFE2:
	.globl	HAS_SSE
HAS_SSE:
.LFB3:
	.cfi_startproc
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	subq	$16, %rsp
	leaq	-16(%rbp), %rax
	movl	$1, %esi
	movq	%rax, %rdi
	call	cpuid
	movl	-8(%rbp), %eax
	andl	$524288, %eax
	testl	%eax, %eax
	setne	%al
	movzbl	%al, %eax
	leave
	.cfi_def_cfa 7, 8
	ret
	.cfi_endproc
.LFE3:
	.globl	HAS_AVX
HAS_AVX:
.LFB4:
	.cfi_startproc
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	subq	$16, %rsp
	leaq	-16(%rbp), %rax
	movl	$1, %esi
	movq	%rax, %rdi
	call	cpuid
	movl	-8(%rbp), %eax
	andl	$268435456, %eax
	testl	%eax, %eax
	setne	%al
	movzbl	%al, %eax
	leave
	.cfi_def_cfa 7, 8
	ret
	.cfi_endproc
.LFE4:
	.globl	HAS_AVX2
HAS_AVX2:
.LFB5:
	.cfi_startproc
	pushq	%rbp
	.cfi_def_cfa_offset 16
	.cfi_offset 6, -16
	movq	%rsp, %rbp
	.cfi_def_cfa_register 6
	subq	$16, %rsp
	leaq	-16(%rbp), %rax
	movl	$7, %esi
	movq	%rax, %rdi
	call	cpuid
	movl	-12(%rbp), %eax
	andl	$32, %eax
	testl	%eax, %eax
	setne	%al
	movzbl	%al, %eax
	leave
	.cfi_def_cfa 7, 8
	ret
	.cfi_endproc
