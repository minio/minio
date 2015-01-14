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

#define TEST_LEN 8192
#define TEST_SIZE (TEST_LEN/2)

#ifndef TEST_SOURCES
# define TEST_SOURCES  250
#endif
#ifndef RANDOMS
# define RANDOMS 20
#endif

#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES

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
	int i, j, rtest, m, k, nerrs, r, err;
	void *buf;
	u8 g[TEST_SOURCES], g_tbls[TEST_SOURCES * 32], src_in_err[TEST_SOURCES];
	u8 *dest, *dest_ref, *temp_buff, *buffs[TEST_SOURCES];
	u8 a[MMAX * KMAX], b[MMAX * KMAX], d[MMAX * KMAX];
	u8 src_err_list[TEST_SOURCES], *recov[TEST_SOURCES];

	printf("gf_vect_dot_prod_base: %dx%d ", TEST_SOURCES, TEST_LEN);

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

	// Init
	for (i = 0; i < TEST_SOURCES; i++)
		memset(buffs[i], 0, TEST_LEN);

	memset(dest, 0, TEST_LEN);
	memset(temp_buff, 0, TEST_LEN);
	memset(dest_ref, 0, TEST_LEN);
	memset(g, 0, TEST_SOURCES);

	// Test erasure code using gf_vect_dot_prod
	// Pick a first test
	m = 9;
	k = 5;
	if (m > MMAX || k > KMAX)
		return -1;

	gf_gen_cauchy1_matrix(a, m, k);

	// Make random data
	for (i = 0; i < k; i++)
		for (j = 0; j < TEST_LEN; j++)
			buffs[i][j] = rand();

	// Make parity vects
	for (i = k; i < m; i++) {
		for (j = 0; j < k; j++)
			gf_vect_mul_init(a[k * i + j], &g_tbls[j * 32]);

		gf_vect_dot_prod_base(TEST_LEN, k, g_tbls, buffs, buffs[i]);
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

		gf_vect_dot_prod_base(TEST_LEN, k, g_tbls, recov, temp_buff);

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

		gf_gen_cauchy1_matrix(a, m, k);

		// Make random data
		for (i = 0; i < k; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		// Make parity vects
		for (i = k; i < m; i++) {
			for (j = 0; j < k; j++)
				gf_vect_mul_init(a[k * i + j], &g_tbls[j * 32]);

			gf_vect_dot_prod_base(TEST_LEN, k, g_tbls, buffs, buffs[i]);
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

			gf_vect_dot_prod_base(TEST_LEN, k, g_tbls, recov, temp_buff);

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

	printf("done all: Pass\n");
	return 0;
}
