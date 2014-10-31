/**********************************************************************
  Copyright(c) 2011-2014 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions 
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>		// for memset, memcmp
#include "erasure-code.h"
#include "erasure/types.h"

#ifndef FUNCTION_UNDER_TEST
# define FUNCTION_UNDER_TEST gf_4vect_dot_prod_sse
#endif
#ifndef TEST_MIN_SIZE
# define TEST_MIN_SIZE  16
#endif

#define str(s) #s
#define xstr(s) str(s)

#define TEST_LEN 8192
#define TEST_SIZE (TEST_LEN/2)
#define TEST_MEM  TEST_SIZE
#define TEST_LOOPS 10000
#define TEST_TYPE_STR ""

#ifndef TEST_SOURCES
# define TEST_SOURCES  16
#endif
#ifndef RANDOMS
# define RANDOMS 20
#endif

#ifdef EC_ALIGNED_ADDR
// Define power of 2 range to check ptr, len alignment
# define PTR_ALIGN_CHK_B 0
# define LEN_ALIGN_CHK_B 0	// 0 for aligned only
#else
// Define power of 2 range to check ptr, len alignment
# define PTR_ALIGN_CHK_B 32
# define LEN_ALIGN_CHK_B 32	// 0 for aligned only
#endif

typedef unsigned char u8;

void dump(unsigned char *buf, int len)
{
	int i;
	for (i = 0; i < len;) {
		printf(" %2x", 0xff & buf[i++]);
		if (i % 32 == 0)
			printf("\n");
	}
	printf("\n");
}

void dump_matrix(unsigned char **s, int k, int m)
{
	int i, j;
	for (i = 0; i < k; i++) {
		for (j = 0; j < m; j++) {
			printf(" %2x", s[i][j]);
		}
		printf("\n");
	}
	printf("\n");
}

void dump_u8xu8(unsigned char *s, int k, int m)
{
	int i, j;
	for (i = 0; i < k; i++) {
		for (j = 0; j < m; j++) {
			printf(" %2x", 0xff & s[j + (i * m)]);
		}
		printf("\n");
	}
	printf("\n");
}

int main(int argc, char *argv[])
{
	int i, j, rtest, srcs;
	void *buf;
	u8 g1[TEST_SOURCES], g2[TEST_SOURCES], g3[TEST_SOURCES];
	u8 g4[TEST_SOURCES], g_tbls[4 * TEST_SOURCES * 32], *buffs[TEST_SOURCES];
	u8 *dest1, *dest2, *dest3, *dest4, *dest_ref1, *dest_ref2, *dest_ref3;
	u8 *dest_ref4, *dest_ptrs[4];

	int align, size;
	unsigned char *efence_buffs[TEST_SOURCES];
	unsigned int offset;
	u8 *ubuffs[TEST_SOURCES];
	u8 *udest_ptrs[4];
	printf(xstr(FUNCTION_UNDER_TEST) ": %dx%d ", TEST_SOURCES, TEST_LEN);

	// Allocate the arrays
	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			return -1;
		}
		buffs[i] = buf;
	}

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest1 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest2 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest3 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest4 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest_ref1 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest_ref2 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest_ref3 = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest_ref4 = buf;

	dest_ptrs[0] = dest1;
	dest_ptrs[1] = dest2;
	dest_ptrs[2] = dest3;
	dest_ptrs[3] = dest4;

	// Test of all zeros
	for (i = 0; i < TEST_SOURCES; i++)
		memset(buffs[i], 0, TEST_LEN);

	memset(dest1, 0, TEST_LEN);
	memset(dest2, 0, TEST_LEN);
	memset(dest3, 0, TEST_LEN);
	memset(dest4, 0, TEST_LEN);
	memset(dest_ref1, 0, TEST_LEN);
	memset(dest_ref2, 0, TEST_LEN);
	memset(dest_ref3, 0, TEST_LEN);
	memset(dest_ref4, 0, TEST_LEN);
	memset(g1, 2, TEST_SOURCES);
	memset(g2, 1, TEST_SOURCES);
	memset(g3, 7, TEST_SOURCES);
	memset(g4, 3, TEST_SOURCES);

	for (i = 0; i < TEST_SOURCES; i++) {
		gf_vect_mul_init(g1[i], &g_tbls[i * 32]);
		gf_vect_mul_init(g2[i], &g_tbls[32 * TEST_SOURCES + i * 32]);
		gf_vect_mul_init(g3[i], &g_tbls[64 * TEST_SOURCES + i * 32]);
		gf_vect_mul_init(g4[i], &g_tbls[96 * TEST_SOURCES + i * 32]);
	}

	gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[0], buffs, dest_ref1);
	gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[32 * TEST_SOURCES], buffs,
			      dest_ref2);
	gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[64 * TEST_SOURCES], buffs,
			      dest_ref3);
	gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[96 * TEST_SOURCES], buffs,
			      dest_ref4);

	FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, g_tbls, buffs, dest_ptrs);

	if (0 != memcmp(dest_ref1, dest1, TEST_LEN)) {
		printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test1\n");
		dump_matrix(buffs, 5, TEST_SOURCES);
		printf("dprod_base:");
		dump(dest_ref1, 25);
		printf("dprod_dut:");
		dump(dest1, 25);
		return -1;
	}
	if (0 != memcmp(dest_ref2, dest2, TEST_LEN)) {
		printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test2\n");
		dump_matrix(buffs, 5, TEST_SOURCES);
		printf("dprod_base:");
		dump(dest_ref2, 25);
		printf("dprod_dut:");
		dump(dest2, 25);
		return -1;
	}
	if (0 != memcmp(dest_ref3, dest3, TEST_LEN)) {
		printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test3\n");
		dump_matrix(buffs, 5, TEST_SOURCES);
		printf("dprod_base:");
		dump(dest_ref3, 25);
		printf("dprod_dut:");
		dump(dest3, 25);
		return -1;
	}
	if (0 != memcmp(dest_ref4, dest4, TEST_LEN)) {
		printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test4\n");
		dump_matrix(buffs, 5, TEST_SOURCES);
		printf("dprod_base:");
		dump(dest_ref4, 25);
		printf("dprod_dut:");
		dump(dest4, 25);
		return -1;
	}

	putchar('.');

	// Rand data test

	for (rtest = 0; rtest < RANDOMS; rtest++) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		for (i = 0; i < TEST_SOURCES; i++) {
			g1[i] = rand();
			g2[i] = rand();
			g3[i] = rand();
			g4[i] = rand();
		}

		for (i = 0; i < TEST_SOURCES; i++) {
			gf_vect_mul_init(g1[i], &g_tbls[i * 32]);
			gf_vect_mul_init(g2[i], &g_tbls[(32 * TEST_SOURCES) + (i * 32)]);
			gf_vect_mul_init(g3[i], &g_tbls[(64 * TEST_SOURCES) + (i * 32)]);
			gf_vect_mul_init(g4[i], &g_tbls[(96 * TEST_SOURCES) + (i * 32)]);
		}

		gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[0], buffs, dest_ref1);
		gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[32 * TEST_SOURCES],
				      buffs, dest_ref2);
		gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[64 * TEST_SOURCES],
				      buffs, dest_ref3);
		gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[96 * TEST_SOURCES],
				      buffs, dest_ref4);

		FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, g_tbls, buffs, dest_ptrs);

		if (0 != memcmp(dest_ref1, dest1, TEST_LEN)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test1 %d\n", rtest);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref1, 25);
			printf("dprod_dut:");
			dump(dest1, 25);
			return -1;
		}
		if (0 != memcmp(dest_ref2, dest2, TEST_LEN)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test2 %d\n", rtest);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref2, 25);
			printf("dprod_dut:");
			dump(dest2, 25);
			return -1;
		}
		if (0 != memcmp(dest_ref3, dest3, TEST_LEN)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test3 %d\n", rtest);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref3, 25);
			printf("dprod_dut:");
			dump(dest3, 25);
			return -1;
		}
		if (0 != memcmp(dest_ref4, dest4, TEST_LEN)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test4 %d\n", rtest);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref4, 25);
			printf("dprod_dut:");
			dump(dest4, 25);
			return -1;
		}

		putchar('.');
	}

	// Rand data test with varied parameters
	for (rtest = 0; rtest < RANDOMS; rtest++) {
		for (srcs = TEST_SOURCES; srcs > 0; srcs--) {
			for (i = 0; i < srcs; i++)
				for (j = 0; j < TEST_LEN; j++)
					buffs[i][j] = rand();

			for (i = 0; i < srcs; i++) {
				g1[i] = rand();
				g2[i] = rand();
				g3[i] = rand();
				g4[i] = rand();
			}

			for (i = 0; i < srcs; i++) {
				gf_vect_mul_init(g1[i], &g_tbls[i * 32]);
				gf_vect_mul_init(g2[i], &g_tbls[(32 * srcs) + (i * 32)]);
				gf_vect_mul_init(g3[i], &g_tbls[(64 * srcs) + (i * 32)]);
				gf_vect_mul_init(g4[i], &g_tbls[(96 * srcs) + (i * 32)]);
			}

			gf_vect_dot_prod_base(TEST_LEN, srcs, &g_tbls[0], buffs, dest_ref1);
			gf_vect_dot_prod_base(TEST_LEN, srcs, &g_tbls[32 * srcs], buffs,
					      dest_ref2);
			gf_vect_dot_prod_base(TEST_LEN, srcs, &g_tbls[64 * srcs], buffs,
					      dest_ref3);
			gf_vect_dot_prod_base(TEST_LEN, srcs, &g_tbls[96 * srcs], buffs,
					      dest_ref4);

			FUNCTION_UNDER_TEST(TEST_LEN, srcs, g_tbls, buffs, dest_ptrs);

			if (0 != memcmp(dest_ref1, dest1, TEST_LEN)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test1 srcs=%d\n", srcs);
				dump_matrix(buffs, 5, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref1, 25);
				printf("dprod_dut:");
				dump(dest1, 25);
				return -1;
			}
			if (0 != memcmp(dest_ref2, dest2, TEST_LEN)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test2 srcs=%d\n", srcs);
				dump_matrix(buffs, 5, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref2, 25);
				printf("dprod_dut:");
				dump(dest2, 25);
				return -1;
			}
			if (0 != memcmp(dest_ref3, dest3, TEST_LEN)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test3 srcs=%d\n", srcs);
				dump_matrix(buffs, 5, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref3, 25);
				printf("dprod_dut:");
				dump(dest3, 25);
				return -1;
			}
			if (0 != memcmp(dest_ref4, dest4, TEST_LEN)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test4 srcs=%d\n", srcs);
				dump_matrix(buffs, 5, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref4, 25);
				printf("dprod_dut:");
				dump(dest4, 25);
				return -1;
			}

			putchar('.');
		}
	}

	// Run tests at end of buffer for Electric Fence
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : 32;
	for (size = TEST_MIN_SIZE; size <= TEST_SIZE; size += align) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		for (i = 0; i < TEST_SOURCES; i++)	// Line up TEST_SIZE from end
			efence_buffs[i] = buffs[i] + TEST_LEN - size;

		for (i = 0; i < TEST_SOURCES; i++) {
			g1[i] = rand();
			g2[i] = rand();
			g3[i] = rand();
			g4[i] = rand();
		}

		for (i = 0; i < TEST_SOURCES; i++) {
			gf_vect_mul_init(g1[i], &g_tbls[i * 32]);
			gf_vect_mul_init(g2[i], &g_tbls[(32 * TEST_SOURCES) + (i * 32)]);
			gf_vect_mul_init(g3[i], &g_tbls[(64 * TEST_SOURCES) + (i * 32)]);
			gf_vect_mul_init(g4[i], &g_tbls[(96 * TEST_SOURCES) + (i * 32)]);
		}

		gf_vect_dot_prod_base(size, TEST_SOURCES, &g_tbls[0], efence_buffs, dest_ref1);
		gf_vect_dot_prod_base(size, TEST_SOURCES, &g_tbls[32 * TEST_SOURCES],
				      efence_buffs, dest_ref2);
		gf_vect_dot_prod_base(size, TEST_SOURCES, &g_tbls[64 * TEST_SOURCES],
				      efence_buffs, dest_ref3);
		gf_vect_dot_prod_base(size, TEST_SOURCES, &g_tbls[96 * TEST_SOURCES],
				      efence_buffs, dest_ref4);

		FUNCTION_UNDER_TEST(size, TEST_SOURCES, g_tbls, efence_buffs, dest_ptrs);

		if (0 != memcmp(dest_ref1, dest1, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test1 %d\n", rtest);
			dump_matrix(efence_buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref1, align);
			printf("dprod_dut:");
			dump(dest1, align);
			return -1;
		}

		if (0 != memcmp(dest_ref2, dest2, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test2 %d\n", rtest);
			dump_matrix(efence_buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref2, align);
			printf("dprod_dut:");
			dump(dest2, align);
			return -1;
		}

		if (0 != memcmp(dest_ref3, dest3, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test3 %d\n", rtest);
			dump_matrix(efence_buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref3, align);
			printf("dprod_dut:");
			dump(dest3, align);
			return -1;
		}

		if (0 != memcmp(dest_ref4, dest4, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test4 %d\n", rtest);
			dump_matrix(efence_buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref4, align);
			printf("dprod_dut:");
			dump(dest4, align);
			return -1;
		}

		putchar('.');
	}

	// Test rand ptr alignment if available

	for (rtest = 0; rtest < RANDOMS; rtest++) {
		size = (TEST_LEN - PTR_ALIGN_CHK_B) & ~(TEST_MIN_SIZE - 1);
		srcs = rand() % TEST_SOURCES;
		if (srcs == 0)
			continue;

		offset = (PTR_ALIGN_CHK_B != 0) ? 1 : PTR_ALIGN_CHK_B;
		// Add random offsets
		for (i = 0; i < srcs; i++)
			ubuffs[i] = buffs[i] + (rand() & (PTR_ALIGN_CHK_B - offset));

		udest_ptrs[0] = dest1 + (rand() & (PTR_ALIGN_CHK_B - offset));
		udest_ptrs[1] = dest2 + (rand() & (PTR_ALIGN_CHK_B - offset));
		udest_ptrs[2] = dest3 + (rand() & (PTR_ALIGN_CHK_B - offset));
		udest_ptrs[3] = dest4 + (rand() & (PTR_ALIGN_CHK_B - offset));

		memset(dest1, 0, TEST_LEN);	// zero pad to check write-over
		memset(dest2, 0, TEST_LEN);
		memset(dest3, 0, TEST_LEN);
		memset(dest4, 0, TEST_LEN);

		for (i = 0; i < srcs; i++)
			for (j = 0; j < size; j++)
				ubuffs[i][j] = rand();

		for (i = 0; i < srcs; i++) {
			g1[i] = rand();
			g2[i] = rand();
			g3[i] = rand();
			g4[i] = rand();
		}

		for (i = 0; i < srcs; i++) {
			gf_vect_mul_init(g1[i], &g_tbls[i * 32]);
			gf_vect_mul_init(g2[i], &g_tbls[(32 * srcs) + (i * 32)]);
			gf_vect_mul_init(g3[i], &g_tbls[(64 * srcs) + (i * 32)]);
			gf_vect_mul_init(g4[i], &g_tbls[(96 * srcs) + (i * 32)]);
		}

		gf_vect_dot_prod_base(size, srcs, &g_tbls[0], ubuffs, dest_ref1);
		gf_vect_dot_prod_base(size, srcs, &g_tbls[32 * srcs], ubuffs, dest_ref2);
		gf_vect_dot_prod_base(size, srcs, &g_tbls[64 * srcs], ubuffs, dest_ref3);
		gf_vect_dot_prod_base(size, srcs, &g_tbls[96 * srcs], ubuffs, dest_ref4);

		FUNCTION_UNDER_TEST(size, srcs, g_tbls, ubuffs, udest_ptrs);

		if (memcmp(dest_ref1, udest_ptrs[0], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign srcs=%d\n",
			       srcs);
			dump_matrix(ubuffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref1, 25);
			printf("dprod_dut:");
			dump(udest_ptrs[0], 25);
			return -1;
		}
		if (memcmp(dest_ref2, udest_ptrs[1], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign srcs=%d\n",
			       srcs);
			dump_matrix(ubuffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref2, 25);
			printf("dprod_dut:");
			dump(udest_ptrs[1], 25);
			return -1;
		}
		if (memcmp(dest_ref3, udest_ptrs[2], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign srcs=%d\n",
			       srcs);
			dump_matrix(ubuffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref3, 25);
			printf("dprod_dut:");
			dump(udest_ptrs[2], 25);
			return -1;
		}
		if (memcmp(dest_ref4, udest_ptrs[3], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign srcs=%d\n",
			       srcs);
			dump_matrix(ubuffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref4, 25);
			printf("dprod_dut:");
			dump(udest_ptrs[3], 25);
			return -1;
		}
		// Confirm that padding around dests is unchanged
		memset(dest_ref1, 0, PTR_ALIGN_CHK_B);	// Make reference zero buff
		offset = udest_ptrs[0] - dest1;

		if (memcmp(dest1, dest_ref1, offset)) {
			printf("Fail rand ualign pad1 start\n");
			return -1;
		}
		if (memcmp(dest1 + offset + size, dest_ref1, PTR_ALIGN_CHK_B - offset)) {
			printf("Fail rand ualign pad1 end\n");
			printf("size=%d offset=%d srcs=%d\n", size, offset, srcs);
			return -1;
		}

		offset = udest_ptrs[1] - dest2;
		if (memcmp(dest2, dest_ref1, offset)) {
			printf("Fail rand ualign pad2 start\n");
			return -1;
		}
		if (memcmp(dest2 + offset + size, dest_ref1, PTR_ALIGN_CHK_B - offset)) {
			printf("Fail rand ualign pad2 end\n");
			return -1;
		}

		offset = udest_ptrs[2] - dest3;
		if (memcmp(dest3, dest_ref1, offset)) {
			printf("Fail rand ualign pad3 start\n");
			return -1;
		}
		if (memcmp(dest3 + offset + size, dest_ref1, PTR_ALIGN_CHK_B - offset)) {
			printf("Fail rand ualign pad3 end\n");
			return -1;
		}

		offset = udest_ptrs[3] - dest4;
		if (memcmp(dest4, dest_ref1, offset)) {
			printf("Fail rand ualign pad4 start\n");
			return -1;
		}
		if (memcmp(dest4 + offset + size, dest_ref1, PTR_ALIGN_CHK_B - offset)) {
			printf("Fail rand ualign pad4 end\n");
			return -1;
		}

		putchar('.');
	}

	// Test all size alignment
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : 32;

	for (size = TEST_LEN; size >= TEST_MIN_SIZE; size -= align) {
		srcs = TEST_SOURCES;

		for (i = 0; i < srcs; i++)
			for (j = 0; j < size; j++)
				buffs[i][j] = rand();

		for (i = 0; i < srcs; i++) {
			g1[i] = rand();
			g2[i] = rand();
			g3[i] = rand();
			g4[i] = rand();
		}

		for (i = 0; i < srcs; i++) {
			gf_vect_mul_init(g1[i], &g_tbls[i * 32]);
			gf_vect_mul_init(g2[i], &g_tbls[(32 * srcs) + (i * 32)]);
			gf_vect_mul_init(g3[i], &g_tbls[(64 * srcs) + (i * 32)]);
			gf_vect_mul_init(g4[i], &g_tbls[(96 * srcs) + (i * 32)]);
		}

		gf_vect_dot_prod_base(size, srcs, &g_tbls[0], buffs, dest_ref1);
		gf_vect_dot_prod_base(size, srcs, &g_tbls[32 * srcs], buffs, dest_ref2);
		gf_vect_dot_prod_base(size, srcs, &g_tbls[64 * srcs], buffs, dest_ref3);
		gf_vect_dot_prod_base(size, srcs, &g_tbls[96 * srcs], buffs, dest_ref4);

		FUNCTION_UNDER_TEST(size, srcs, g_tbls, buffs, dest_ptrs);

		if (memcmp(dest_ref1, dest_ptrs[0], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign len=%d\n",
			       size);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref1, 25);
			printf("dprod_dut:");
			dump(dest_ptrs[0], 25);
			return -1;
		}
		if (memcmp(dest_ref2, dest_ptrs[1], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign len=%d\n",
			       size);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref2, 25);
			printf("dprod_dut:");
			dump(dest_ptrs[1], 25);
			return -1;
		}
		if (memcmp(dest_ref3, dest_ptrs[2], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign len=%d\n",
			       size);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref3, 25);
			printf("dprod_dut:");
			dump(dest_ptrs[2], 25);
			return -1;
		}
		if (memcmp(dest_ref4, dest_ptrs[3], size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test ualign len=%d\n",
			       size);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref4, 25);
			printf("dprod_dut:");
			dump(dest_ptrs[3], 25);
			return -1;
		}
	}

	printf("Pass\n");
	return 0;

}
