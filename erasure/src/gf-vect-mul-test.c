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
#include <string.h>		// for memset
#include "erasure-code.h"

#define TEST_SIZE 8192
#define TEST_MEM  TEST_SIZE
#define TEST_LOOPS 100000
#define TEST_TYPE_STR ""

typedef unsigned char u8;

int main(int argc, char *argv[])
{
	int i;
	u8 *buff1, *buff2, *buff3, gf_const_tbl[64], a = 2;
	int align, size;
	unsigned char *efence_buff1;
	unsigned char *efence_buff2;
	unsigned char *efence_buff3;

	printf("gf_vect_mul_test:\n");

	gf_vect_mul_init(a, gf_const_tbl);

	buff1 = (u8 *) malloc(TEST_SIZE);
	buff2 = (u8 *) malloc(TEST_SIZE);
	buff3 = (u8 *) malloc(TEST_SIZE);

	if (NULL == buff1 || NULL == buff2 || NULL == buff3) {
		printf("buffer alloc error\n");
		return -1;
	}
	// Fill with rand data
	for (i = 0; i < TEST_SIZE; i++)
		buff1[i] = rand();

	gf_vect_mul(TEST_SIZE, gf_const_tbl, buff1, buff2);

	for (i = 0; i < TEST_SIZE; i++)
		if (gf_mul(a, buff1[i]) != buff2[i]) {
			printf("fail at %d, 0x%x x 2 = 0x%x (0x%x)\n", i, buff1[i], buff2[i],
			       gf_mul(2, buff1[i]));
			return 1;
		}

	gf_vect_mul_base(TEST_SIZE, gf_const_tbl, buff1, buff3);

	// Check reference function
	for (i = 0; i < TEST_SIZE; i++)
		if (buff2[i] != buff3[i]) {
			printf("fail at %d, 0x%x x 0x%d = 0x%x (0x%x)\n",
			       i, a, buff1[i], buff2[i], gf_mul(a, buff1[i]));
			return 1;
		}

	for (i = 0; i < TEST_SIZE; i++)
		buff1[i] = rand();

	// Check each possible constant
	printf("Random tests ");
	for (a = 0; a != 255; a++) {
		gf_vect_mul_init(a, gf_const_tbl);
		gf_vect_mul(TEST_SIZE, gf_const_tbl, buff1, buff2);

		for (i = 0; i < TEST_SIZE; i++) {
			if (gf_mul(a, buff1[i]) != buff2[i]) {
				printf("fail at %d, 0x%x x %d = 0x%x (0x%x)\n",
				       i, a, buff1[i], buff2[i], gf_mul(2, buff1[i]));
				return 1;
			}
		}
		putchar('.');
	}

	// Run tests at end of buffer for Electric Fence
	align = 32;
	a = 2;

	gf_vect_mul_init(a, gf_const_tbl);
	for (size = 0; size < TEST_SIZE; size += align) {
		// Line up TEST_SIZE from end
		efence_buff1 = buff1 + size;
		efence_buff2 = buff2 + size;
		efence_buff3 = buff3 + size;

		gf_vect_mul(TEST_SIZE - size, gf_const_tbl, efence_buff1, efence_buff2);

		for (i = 0; i < TEST_SIZE - size; i++)
			if (gf_mul(a, efence_buff1[i]) != efence_buff2[i]) {
				printf("fail at %d, 0x%x x 2 = 0x%x (0x%x)\n",
				       i, efence_buff1[i], efence_buff2[i],
				       gf_mul(2, efence_buff1[i]));
				return 1;
			}

		gf_vect_mul_base(TEST_SIZE - size, gf_const_tbl, efence_buff1, efence_buff3);

		// Check reference function
		for (i = 0; i < TEST_SIZE - size; i++)
			if (efence_buff2[i] != efence_buff3[i]) {
				printf("fail at %d, 0x%x x 0x%d = 0x%x (0x%x)\n",
				       i, a, efence_buff2[i], efence_buff3[i],
				       gf_mul(2, efence_buff1[i]));
				return 1;
			}

		putchar('.');
	}

	printf(" done: Pass\n");
	return 0;
}
