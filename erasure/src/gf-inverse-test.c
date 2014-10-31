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
#include <assert.h>

#include "erasure-code.h"

#define TEST_LEN 8192

#ifndef TEST_SOURCES
# define TEST_SOURCES  128
#endif
#ifndef RANDOMS
# define RANDOMS 200
#endif

#define KMAX TEST_SOURCES

typedef unsigned char u8;

void matrix_mult(u8 * a, u8 * b, u8 * c, int n)
{
	int i, j, k;
	u8 d;

	for (i = 0; i < n; i++) {
		for (j = 0; j < n; j++) {
			d = 0;
			for (k = 0; k < n; k++) {
				d ^= gf_mul(a[n * i + k], b[n * k + j]);
			}
			c[i * n + j] = d;
		}
	}
}

void print_matrix(u8 * a, int n)
{
	int i, j;

	for (i = 0; i < n; i++) {
		for (j = 0; j < n; j++) {
			printf(" %2x", a[i * n + j]);
		}
		printf("\n");
	}
	printf("\n");
}

int is_ident(u8 * a, const int n)
{
	int i, j;
	u8 c;
	for (i = 0; i < n; i++) {
		for (j = 0; j < n; j++) {
			c = *a++;
			if (i == j)
				c--;
			if (c != 0)
				return -1;
		}
	}
	return 0;
}

int inv_test(u8 * in, u8 * inv, u8 * sav, int n)
{
	memcpy(sav, in, n * n);

	if (gf_invert_matrix(in, inv, n)) {
		printf("Given singular matrix\n");
		print_matrix(sav, n);
		return -1;
	}

	matrix_mult(inv, sav, in, n);

	if (is_ident(in, n)) {
		printf("fail\n");
		print_matrix(sav, n);
		print_matrix(inv, n);
		print_matrix(in, n);
		return -1;
	}
	putchar('.');

	return 0;
}

int main(int argc, char *argv[])
{
	int i, k, t;
	u8 *test_mat, *save_mat, *invr_mat;

	u8 test1[] = { 1, 1, 6,
		1, 1, 1,
		7, 1, 9
	};

	u8 test2[] = { 0, 1, 6,
		1, 0, 1,
		0, 1, 9
	};

	u8 test3[] = { 0, 0, 1,
		1, 0, 0,
		0, 1, 1
	};

	u8 test4[] = { 0, 1, 6, 7,
		1, 1, 0, 0,
		0, 1, 2, 3,
		3, 2, 2, 3
	};			// = row3+3*row2

	printf("gf_inverse_test: max=%d ", KMAX);

	test_mat = malloc(KMAX * KMAX);
	save_mat = malloc(KMAX * KMAX);
	invr_mat = malloc(KMAX * KMAX);

	if (NULL == test_mat || NULL == save_mat || NULL == invr_mat)
		return -1;

	// Test with lots of leading 1's
	k = 3;
	memcpy(test_mat, test1, k * k);
	if (inv_test(test_mat, invr_mat, save_mat, k))
		return -1;

	// Test with leading zeros
	k = 3;
	memcpy(test_mat, test2, k * k);
	if (inv_test(test_mat, invr_mat, save_mat, k))
		return -1;

	// Test 3
	k = 3;
	memcpy(test_mat, test3, k * k);
	if (inv_test(test_mat, invr_mat, save_mat, k))
		return -1;

	// Test 4 - try a singular matrix
	k = 4;
	memcpy(test_mat, test4, k * k);
	if (!gf_invert_matrix(test_mat, invr_mat, k)) {
		printf("Fail: didn't catch singular matrix\n");
		print_matrix(test4, 4);
		return -1;
	}
	// Do random test of size KMAX
	k = KMAX;

	for (i = 0; i < k * k; i++)
		test_mat[i] = save_mat[i] = rand();

	if (gf_invert_matrix(test_mat, invr_mat, k)) {
		printf("rand picked a singular matrix, try again\n");
		return -1;
	}

	matrix_mult(invr_mat, save_mat, test_mat, k);

	if (is_ident(test_mat, k)) {
		printf("fail\n");
		print_matrix(save_mat, k);
		print_matrix(invr_mat, k);
		print_matrix(test_mat, k);
		return -1;
	}
	// Do Randoms.  Random size and coefficients
	for (t = 0; t < RANDOMS; t++) {
		k = rand() % KMAX;

		for (i = 0; i < k * k; i++)
			test_mat[i] = save_mat[i] = rand();

		if (gf_invert_matrix(test_mat, invr_mat, k))
			continue;

		matrix_mult(invr_mat, save_mat, test_mat, k);

		if (is_ident(test_mat, k)) {
			printf("fail rand k=%d\n", k);
			print_matrix(save_mat, k);
			print_matrix(invr_mat, k);
			print_matrix(test_mat, k);
			return -1;
		}
		if (0 == (t % 8))
			putchar('.');
	}

	printf(" Pass\n");
	return 0;
}
