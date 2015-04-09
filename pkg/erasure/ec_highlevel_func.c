/**********************************************************************
  Copyright(c) 2011-2015 Intel Corporation All rights reserved.

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
#include <limits.h>
#include "ec_code.h"
#include "ec_types.h"

void ec_init_tables(int k, int rows, unsigned char *a, unsigned char *g_tbls)
{
	int i, j;

	for (i = 0; i < rows; i++) {
		for (j = 0; j < k; j++) {
			gf_vect_mul_init(*a++, g_tbls);
			g_tbls += 32;
		}
	}
}

void ec_encode_data_sse(int len, int k, int rows, unsigned char *g_tbls, unsigned char **data,
			unsigned char **coding)
{

	if (len < 16) {
		ec_encode_data_base(len, k, rows, g_tbls, data, coding);
		return;
	}

	while (rows >= 4) {
		gf_4vect_dot_prod_sse(len, k, g_tbls, data, coding);
		g_tbls += 4 * k * 32;
		coding += 4;
		rows -= 4;
	}
	switch (rows) {
	case 3:
		gf_3vect_dot_prod_sse(len, k, g_tbls, data, coding);
		break;
	case 2:
		gf_2vect_dot_prod_sse(len, k, g_tbls, data, coding);
		break;
	case 1:
		gf_vect_dot_prod_sse(len, k, g_tbls, data, *coding);
		break;
	case 0:
		break;
	}

}

void ec_encode_data_avx(int len, int k, int rows, unsigned char *g_tbls, unsigned char **data,
			unsigned char **coding)
{
	if (len < 16) {
		ec_encode_data_base(len, k, rows, g_tbls, data, coding);
		return;
	}

	while (rows >= 4) {
		gf_4vect_dot_prod_avx(len, k, g_tbls, data, coding);
		g_tbls += 4 * k * 32;
		coding += 4;
		rows -= 4;
	}
	switch (rows) {
	case 3:
		gf_3vect_dot_prod_avx(len, k, g_tbls, data, coding);
		break;
	case 2:
		gf_2vect_dot_prod_avx(len, k, g_tbls, data, coding);
		break;
	case 1:
		gf_vect_dot_prod_avx(len, k, g_tbls, data, *coding);
		break;
	case 0:
		break;
	}

}

void ec_encode_data_avx2(int len, int k, int rows, unsigned char *g_tbls, unsigned char **data,
			 unsigned char **coding)
{

	if (len < 32) {
		ec_encode_data_base(len, k, rows, g_tbls, data, coding);
		return;
	}

	while (rows >= 4) {
		gf_4vect_dot_prod_avx2(len, k, g_tbls, data, coding);
		g_tbls += 4 * k * 32;
		coding += 4;
		rows -= 4;
	}
	switch (rows) {
	case 3:
		gf_3vect_dot_prod_avx2(len, k, g_tbls, data, coding);
		break;
	case 2:
		gf_2vect_dot_prod_avx2(len, k, g_tbls, data, coding);
		break;
	case 1:
		gf_vect_dot_prod_avx2(len, k, g_tbls, data, *coding);
		break;
	case 0:
		break;
	}

}

#if __WORDSIZE == 64 || _WIN64 || __x86_64__

void ec_encode_data_update_sse(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
			       unsigned char *data, unsigned char **coding)
{
	if (len < 16) {
		ec_encode_data_update_base(len, k, rows, vec_i, g_tbls, data, coding);
		return;
	}

	while (rows > 6) {
		gf_6vect_mad_sse(len, k, vec_i, g_tbls, data, coding);
		g_tbls += 6 * k * 32;
		coding += 6;
		rows -= 6;
	}
	switch (rows) {
	case 6:
		gf_6vect_mad_sse(len, k, vec_i, g_tbls, data, coding);
		break;
	case 5:
		gf_5vect_mad_sse(len, k, vec_i, g_tbls, data, coding);
		break;
	case 4:
		gf_4vect_mad_sse(len, k, vec_i, g_tbls, data, coding);
		break;
	case 3:
		gf_3vect_mad_sse(len, k, vec_i, g_tbls, data, coding);
		break;
	case 2:
		gf_2vect_mad_sse(len, k, vec_i, g_tbls, data, coding);
		break;
	case 1:
		gf_vect_mad_sse(len, k, vec_i, g_tbls, data, *coding);
		break;
	case 0:
		break;
	}

}

void ec_encode_data_update_avx(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
			       unsigned char *data, unsigned char **coding)
{
	if (len < 16) {
		ec_encode_data_update_base(len, k, rows, vec_i, g_tbls, data, coding);
		return;
	}
	while (rows > 6) {
		gf_6vect_mad_avx(len, k, vec_i, g_tbls, data, coding);
		g_tbls += 6 * k * 32;
		coding += 6;
		rows -= 6;
	}
	switch (rows) {
	case 6:
		gf_6vect_mad_avx(len, k, vec_i, g_tbls, data, coding);
		break;
	case 5:
		gf_5vect_mad_avx(len, k, vec_i, g_tbls, data, coding);
		break;
	case 4:
		gf_4vect_mad_avx(len, k, vec_i, g_tbls, data, coding);
		break;
	case 3:
		gf_3vect_mad_avx(len, k, vec_i, g_tbls, data, coding);
		break;
	case 2:
		gf_2vect_mad_avx(len, k, vec_i, g_tbls, data, coding);
		break;
	case 1:
		gf_vect_mad_avx(len, k, vec_i, g_tbls, data, *coding);
		break;
	case 0:
		break;
	}

}

void ec_encode_data_update_avx2(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
				unsigned char *data, unsigned char **coding)
{
	if (len < 32) {
		ec_encode_data_update_base(len, k, rows, vec_i, g_tbls, data, coding);
		return;
	}
	while (rows > 6) {
		gf_6vect_mad_avx2(len, k, vec_i, g_tbls, data, coding);
		g_tbls += 6 * k * 32;
		coding += 6;
		rows -= 6;
	}
	switch (rows) {
	case 6:
		gf_6vect_mad_avx2(len, k, vec_i, g_tbls, data, coding);
		break;
	case 5:
		gf_5vect_mad_avx2(len, k, vec_i, g_tbls, data, coding);
		break;
	case 4:
		gf_4vect_mad_avx2(len, k, vec_i, g_tbls, data, coding);
		break;
	case 3:
		gf_3vect_mad_avx2(len, k, vec_i, g_tbls, data, coding);
		break;
	case 2:
		gf_2vect_mad_avx2(len, k, vec_i, g_tbls, data, coding);
		break;
	case 1:
		gf_vect_mad_avx2(len, k, vec_i, g_tbls, data, *coding);
		break;
	case 0:
		break;
	}

}

#endif //__WORDSIZE == 64 || _WIN64 || __x86_64__

struct slver {
	UINT16 snum;
	UINT8 ver;
	UINT8 core;
};

// Version info
struct slver ec_init_tables_slver_00010068;
struct slver ec_init_tables_slver = { 0x0068, 0x01, 0x00 };

struct slver ec_encode_data_sse_slver_00020069;
struct slver ec_encode_data_sse_slver = { 0x0069, 0x02, 0x00 };
