########################################################################
#  Copyright(c) 2011-2014 Intel Corporation All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in
#      the documentation and/or other materials provided with the
#      distribution.
#    * Neither the name of Intel Corporation nor the names of its
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
########################################################################


lib_name    := 	erasure_code.a

lsrc        += 	ec-highlevel-func.c \
		ec-base.c \
		gf-vect-mul-sse.asm \
		gf-vect-mul-avx.asm \
		gf-vect-dot-prod-sse.asm  \
		gf-vect-dot-prod-avx.asm \
		gf-vect-dot-prod-avx2.asm \
		gf-2vect-dot-prod-sse.asm \
		gf-3vect-dot-prod-sse.asm \
		gf-4vect-dot-prod-sse.asm \
		gf-5vect-dot-prod-sse.asm \
		gf-6vect-dot-prod-sse.asm \
		gf-2vect-dot-prod-avx.asm \
		gf-3vect-dot-prod-avx.asm \
		gf-4vect-dot-prod-avx.asm \
		gf-5vect-dot-prod-avx.asm \
		gf-6vect-dot-prod-avx.asm \
		gf-2vect-dot-prod-avx2.asm \
		gf-3vect-dot-prod-avx2.asm \
		gf-4vect-dot-prod-avx2.asm \
		gf-5vect-dot-prod-avx2.asm \
		gf-6vect-dot-prod-avx2.asm \
		ec-multibinary.asm

lsrc32	     += ec-highlevel-func.c ec-multibinary.asm ec-base.c

unit_tests32 += erasure-code-base-test erasure-code-test \
		gf-vect-mul-test gf-vect-mul-base-test \
		gf-vect-dot-prod-base-test gf-vect-dot-prod-test

perf_tests32 += gf-vect-mul-perf gf-vect-dot-prod-perf erasure-code-perf \
		erasure-code-base-perf gf-vect-dot-prod-1tbl

extern_hdrs +=	erasure-code.h ec_base.h gf-vect-mul.h \
		erasure/tests.h erausre/types.h

unit_tests  += 	gf-vect-mul-test \
		gf-vect-mul-sse-test \
		gf-vect-mul-avx-test \
		gf-vect-mul-base-test \
		gf-vect-dot-prod-sse-test \
		gf-vect-dot-prod-avx-test \
		gf-2vect-dot-prod-sse-test \
		gf-3vect-dot-prod-sse-test \
		gf-4vect-dot-prod-sse-test \
		gf-5vect-dot-prod-sse-test \
		gf-6vect-dot-prod-sse-test \
		gf-inverse-test \
		gf-vect-dot-prod-base-test \
		gf-vect-dot-prod-test \
		erasure-code-test \
		erasure-code-base-test \
		erasure-code-sse-test

perf_tests  += 	gf-vect-mul-perf \
		gf-vect-mul-sse-perf \
		gf-vect-mul-avx-perf \
		gf-vect-dot-prod-sse-perf \
		gf-vect-dot-prod-avx-perf \
		gf-2vect-dot-prod-sse-perf \
		gf-3vect-dot-prod-sse-perf \
		gf-4vect-dot-prod-sse-perf \
		gf-5vect-dot-prod-sse-perf \
		gf-6vect-dot-prod-sse-perf \
		gf-vect-dot-prod-perf \
		gf-vect-dot-prod-1tbl \
		erasure-code-perf \
		erasure-code-base-perf \
		erasure-code-sse-perf

other_src   += reg-sizes.asm

VPATH = .. ../include

-include ../make.inc
