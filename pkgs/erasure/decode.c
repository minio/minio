/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <erasure-code.h>
#include "decode.h"

static int src_in_err (int r, int *src_err_list)
{
        int i;
        for (i = 0; src_err_list[i] != -1; i++) {
                if (src_err_list[i] == r) {
                        return 1;
                }
        }
        // false
        return 0;
}

/*
  Generate decode matrix during the decoding phase
*/

int minio_init_decoder (int *src_err_list,
                        unsigned char *encode_matrix,
                        unsigned char **decode_matrix,
                        unsigned char **decode_tbls,
                        int k, int n, int errs)
{
        int i, j, r, s, l, z;
        unsigned char input_matrix[k * n];
        unsigned char inverse_matrix[k * n];
        unsigned char *tmp_decode_matrix;
        unsigned char *tmp_decode_tbls;

        tmp_decode_matrix = (unsigned char *) malloc (k * n);
        if (!tmp_decode_matrix)
                return -1;

        tmp_decode_tbls = (unsigned char *) malloc (k * n * 32);
        if (!tmp_decode_tbls)
                return -1;

        for (i = 0, r = 0; i < k; i++, r++) {
                while (src_in_err(r, src_err_list))
                        r++;
                for (j = 0; j < k; j++) {
                        input_matrix[k * i + j] = encode_matrix[k * r + j];
                }
        }

        // Not all Vandermonde matrix can be inverted
        if (gf_invert_matrix(input_matrix, inverse_matrix, k) < 0) {
                return -1;
        }

        for (l = 0; l < errs; l++) {
                if (src_err_list[l] < k) {
                        // decoding matrix elements for data chunks
                        for (j = 0; j < k; j++) {
                                tmp_decode_matrix[k * l + j] =
                                        inverse_matrix[k *
                                                       src_err_list[l] + j];
                        }
                } else {
                        int s = 0;
                        // decoding matrix element for coding chunks
                        for (i = 0; i < k; i++) {
                                s = 0;
                                for (j = 0; j < k; j++) {
                                        s ^= gf_mul(inverse_matrix[j * k + i],
                                                    encode_matrix[k *
                                                                  src_err_list[l] + j]);
                                }
                                tmp_decode_matrix[k * l + i] = s;
                        }
                }
        }

        ec_init_tables(k, errs, tmp_decode_matrix, tmp_decode_tbls);

        *decode_matrix = tmp_decode_matrix;
        *decode_tbls = tmp_decode_tbls;
        return 0;
}
