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
# define FUNCTION_UNDER_TEST gf_vect_dot_prod
#endif
#ifndef TEST_MIN_SIZE
# define TEST_MIN_SIZE  32
#endif

#define str(s) #s
#define xstr(s) str(s)

#define TEST_LEN 8192
#define TEST_SIZE (TEST_LEN/2)

#ifndef TEST_SOURCES
# define TEST_SOURCES  16
#endif
#ifndef RANDOMS
# define RANDOMS 20
#endif

#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES

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
	int i, j, rtest, srcs, m, k, nerrs, r, err;
	void *buf;
	u8 g[TEST_SOURCES], g_tbls[TEST_SOURCES * 32], src_in_err[TEST_SOURCES];
	u8 *dest, *dest_ref, *temp_buff, *buffs[TEST_SOURCES];
	u8 a[MMAX * KMAX], b[MMAX * KMAX], d[MMAX * KMAX];
	u8 src_err_list[TEST_SOURCES], *recov[TEST_SOURCES];

	int align, size;
	unsigned char *efence_buffs[TEST_SOURCES];
	unsigned int offset;
	u8 *ubuffs[TEST_SOURCES];
	u8 *udest_ptr;

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
	dest = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest_ref = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	temp_buff = buf;

	// Test of all zeros
	for (i = 0; i < TEST_SOURCES; i++)
		memset(buffs[i], 0, TEST_LEN);

	memset(dest, 0, TEST_LEN);
	memset(temp_buff, 0, TEST_LEN);
	memset(dest_ref, 0, TEST_LEN);
	memset(g, 0, TEST_SOURCES);

	for (i = 0; i < TEST_SOURCES; i++)
		gf_vect_mul_init(g[i], &g_tbls[i * 32]);

	gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[0], buffs, dest_ref);

	FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, g_tbls, buffs, dest);

	if (0 != memcmp(dest_ref, dest, TEST_LEN)) {
		printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " \n");
		dump_matrix(buffs, 5, TEST_SOURCES);
		printf("dprod_base:");
		dump(dest_ref, 25);
		printf("dprod:");
		dump(dest, 25);
		return -1;
	} else
		putchar('.');

	// Rand data test
	for (rtest = 0; rtest < RANDOMS; rtest++) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		for (i = 0; i < TEST_SOURCES; i++)
			g[i] = rand();

		for (i = 0; i < TEST_SOURCES; i++)
			gf_vect_mul_init(g[i], &g_tbls[i * 32]);

		gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[0], buffs, dest_ref);
		FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, g_tbls, buffs, dest);

		if (0 != memcmp(dest_ref, dest, TEST_LEN)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " 1\n");
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref, 25);
			printf("dprod:");
			dump(dest, 25);
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

			for (i = 0; i < srcs; i++)
				g[i] = rand();

			for (i = 0; i < srcs; i++)
				gf_vect_mul_init(g[i], &g_tbls[i * 32]);

			gf_vect_dot_prod_base(TEST_LEN, srcs, &g_tbls[0], buffs, dest_ref);
			FUNCTION_UNDER_TEST(TEST_LEN, srcs, g_tbls, buffs, dest);

			if (0 != memcmp(dest_ref, dest, TEST_LEN)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test 2\n");
				dump_matrix(buffs, 5, srcs);
				printf("dprod_base:");
				dump(dest_ref, 5);
				printf("dprod:");
				dump(dest, 5);
				return -1;
			}

			putchar('.');
		}
	}

	// Test erasure code using gf_vect_dot_prod

	// Pick a first test
	m = 9;
	k = 5;
	if (m > MMAX || k > KMAX)
		return -1;

	gf_gen_rs_matrix(a, m, k);

	// Make random data
	for (i = 0; i < k; i++)
		for (j = 0; j < TEST_LEN; j++)
			buffs[i][j] = rand();

	// Make parity vects
	for (i = k; i < m; i++) {
		for (j = 0; j < k; j++)
			gf_vect_mul_init(a[k * i + j], &g_tbls[j * 32]);
#ifndef USEREF
		FUNCTION_UNDER_TEST(TEST_LEN, k, g_tbls, buffs, buffs[i]);
#else
		gf_vect_dot_prod_base(TEST_LEN, k, &g_tbls[0], buffs, buffs[i]);
#endif
	}

	// Random buffers in erasure
	memset(src_in_err, 0, TEST_SOURCES);
	for (i = 0, nerrs = 0; i < k && nerrs < m - k; i++) {
		err = 1 & rand();
		src_in_err[i] = err;
		if (err)
			src_err_list[nerrs++] = i;
	}

	// construct b by removing error rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (src_in_err[r]) {
			r++;
			continue;
		}
		for (j = 0; j < k; j++)
			b[k * i + j] = a[k * r + j];
	}

	if (gf_invert_matrix((u8 *) b, (u8 *) d, k) < 0)
		printf("BAD MATRIX\n");

	for (i = 0, r = 0; i < k; i++, r++) {
		while (src_in_err[r]) {
			r++;
			continue;
		}
		recov[i] = buffs[r];
	}

	// Recover data
	for (i = 0; i < nerrs; i++) {
		for (j = 0; j < k; j++)
			gf_vect_mul_init(d[k * src_err_list[i] + j], &g_tbls[j * 32]);
#ifndef USEREF
		FUNCTION_UNDER_TEST(TEST_LEN, k, g_tbls, recov, temp_buff);
#else
		gf_vect_dot_prod_base(TEST_LEN, k, &g_tbls[0], recov, temp_buff);
#endif

		if (0 != memcmp(temp_buff, buffs[src_err_list[i]], TEST_LEN)) {
			printf("Fail error recovery (%d, %d, %d)\n", m, k, nerrs);
			printf("recov %d:", src_err_list[i]);
			dump(temp_buff, 25);
			printf("orig   :");
			dump(buffs[src_err_list[i]], 25);
			return -1;
		}
	}

	// Do more random tests

	for (rtest = 0; rtest < RANDOMS; rtest++) {
		while ((m = (rand() % MMAX)) < 2) ;
		while ((k = (rand() % KMAX)) >= m || k < 1) ;

		if (m > MMAX || k > KMAX)
			continue;

		gf_gen_rs_matrix(a, m, k);

		// Make random data
		for (i = 0; i < k; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		// Make parity vects
		for (i = k; i < m; i++) {
			for (j = 0; j < k; j++)
				gf_vect_mul_init(a[k * i + j], &g_tbls[j * 32]);
#ifndef USEREF
			FUNCTION_UNDER_TEST(TEST_LEN, k, g_tbls, buffs, buffs[i]);
#else
			gf_vect_dot_prod_base(TEST_LEN, k, &g_tbls[0], buffs, buffs[i]);
#endif
		}

		// Random errors
		memset(src_in_err, 0, TEST_SOURCES);
		for (i = 0, nerrs = 0; i < k && nerrs < m - k; i++) {
			err = 1 & rand();
			src_in_err[i] = err;
			if (err)
				src_err_list[nerrs++] = i;
		}
		if (nerrs == 0) {	// should have at least one error
			while ((err = (rand() % KMAX)) >= k) ;
			src_err_list[nerrs++] = err;
			src_in_err[err] = 1;
		}
		// construct b by removing error rows
		for (i = 0, r = 0; i < k; i++, r++) {
			while (src_in_err[r]) {
				r++;
				continue;
			}
			for (j = 0; j < k; j++)
				b[k * i + j] = a[k * r + j];
		}

		if (gf_invert_matrix((u8 *) b, (u8 *) d, k) < 0)
			printf("BAD MATRIX\n");

		for (i = 0, r = 0; i < k; i++, r++) {
			while (src_in_err[r]) {
				r++;
				continue;
			}
			recov[i] = buffs[r];
		}

		// Recover data
		for (i = 0; i < nerrs; i++) {
			for (j = 0; j < k; j++)
				gf_vect_mul_init(d[k * src_err_list[i] + j], &g_tbls[j * 32]);
#ifndef USEREF
			FUNCTION_UNDER_TEST(TEST_LEN, k, g_tbls, recov, temp_buff);
#else
			gf_vect_dot_prod_base(TEST_LEN, k, &g_tbls[0], recov, temp_buff);
#endif
			if (0 != memcmp(temp_buff, buffs[src_err_list[i]], TEST_LEN)) {
				printf("Fail error recovery (%d, %d, %d) - ", m, k, nerrs);
				printf(" - erase list = ");
				for (i = 0; i < nerrs; i++)
					printf(" %d", src_err_list[i]);
				printf("\na:\n");
				dump_u8xu8((u8 *) a, m, k);
				printf("inv b:\n");
				dump_u8xu8((u8 *) d, k, k);
				printf("orig data:\n");
				dump_matrix(buffs, m, 25);
				printf("orig   :");
				dump(buffs[src_err_list[i]], 25);
				printf("recov %d:", src_err_list[i]);
				dump(temp_buff, 25);
				return -1;
			}
		}
		putchar('.');
	}

	// Run tests at end of buffer for Electric Fence
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : 16;
	for (size = TEST_MIN_SIZE; size <= TEST_SIZE; size += align) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		for (i = 0; i < TEST_SOURCES; i++)	// Line up TEST_SIZE from end
			efence_buffs[i] = buffs[i] + TEST_LEN - size;

		for (i = 0; i < TEST_SOURCES; i++)
			g[i] = rand();

		for (i = 0; i < TEST_SOURCES; i++)
			gf_vect_mul_init(g[i], &g_tbls[i * 32]);

		gf_vect_dot_prod_base(size, TEST_SOURCES, &g_tbls[0], efence_buffs, dest_ref);
		FUNCTION_UNDER_TEST(size, TEST_SOURCES, g_tbls, efence_buffs, dest);

		if (0 != memcmp(dest_ref, dest, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test 3\n");
			dump_matrix(efence_buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref, align);
			printf("dprod:");
			dump(dest, align);
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

		udest_ptr = dest + (rand() & (PTR_ALIGN_CHK_B - offset));

		memset(dest, 0, TEST_LEN);	// zero pad to check write-over

		for (i = 0; i < srcs; i++)
			for (j = 0; j < size; j++)
				ubuffs[i][j] = rand();

		for (i = 0; i < srcs; i++)
			g[i] = rand();

		for (i = 0; i < srcs; i++)
			gf_vect_mul_init(g[i], &g_tbls[i * 32]);

		gf_vect_dot_prod_base(size, srcs, &g_tbls[0], ubuffs, dest_ref);

		FUNCTION_UNDER_TEST(size, srcs, g_tbls, ubuffs, udest_ptr);

		if (memcmp(dest_ref, udest_ptr, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " ualign srcs=%d\n",
			       srcs);
			dump_matrix(ubuffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref, 25);
			printf("dprod:");
			dump(udest_ptr, 25);
			return -1;
		}
		// Confirm that padding around dests is unchanged
		memset(dest_ref, 0, PTR_ALIGN_CHK_B);	// Make reference zero buff
		offset = udest_ptr - dest;

		if (memcmp(dest, dest_ref, offset)) {
			printf("Fail rand ualign pad start\n");
			return -1;
		}
		if (memcmp(dest + offset + size, dest_ref, PTR_ALIGN_CHK_B - offset)) {
			printf("Fail rand ualign pad end\n");
			return -1;
		}

		putchar('.');
	}

	// Test all size alignment
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : 16;

	for (size = TEST_LEN; size >= TEST_MIN_SIZE; size -= align) {
		srcs = TEST_SOURCES;

		for (i = 0; i < srcs; i++)
			for (j = 0; j < size; j++)
				buffs[i][j] = rand();

		for (i = 0; i < srcs; i++)
			g[i] = rand();

		for (i = 0; i < srcs; i++)
			gf_vect_mul_init(g[i], &g_tbls[i * 32]);

		gf_vect_dot_prod_base(size, srcs, &g_tbls[0], buffs, dest_ref);

		FUNCTION_UNDER_TEST(size, srcs, g_tbls, buffs, dest);

		if (memcmp(dest_ref, dest, size)) {
			printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " ualign len=%d\n",
			       size);
			dump_matrix(buffs, 5, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref, 25);
			printf("dprod:");
			dump(dest, 25);
			return -1;
		}
	}

	printf("done all: Pass\n");
	return 0;
}
