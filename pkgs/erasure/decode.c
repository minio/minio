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
#include "common.h"

int32_t minio_src_in_err (int r, int *src_err_list)
{
        int i;
        for (i = 0; src_err_list[i] != -1; i++) {
                if (src_err_list[i] == r) {
                        // true
                        return 1;
                }
        }
        // false
        return 0;
}

int32_t minio_get_source_target(int *src_err_list,
                                int errs, int k, int m,
                                unsigned char **data,
                                unsigned char **coding,
                                unsigned char ***source,
                                unsigned char ***target)
{
        int i, j, l;
        unsigned char *tmp_source[k];
        unsigned char *tmp_target[m];

        // Fill zeroes
        memset (tmp_source, 0, sizeof(tmp_source));
        memset (tmp_target, 0, sizeof(tmp_target));

        // Separate out source and target buffers from input data/coding chunks
        // This separation needs to happen at error chunks from input chunks
        for (i = 0, j = 0, l = 0;
             ((l < k) || (j < errs)) && (i < (k + m)); i++) {
                if (!minio_src_in_err(i, src_err_list)) {
                        if (l < k) {
                                if (i < k)
                                        tmp_source[l] =
                                                (unsigned char *) data[i];
                                else
                                        tmp_source[l] =
                                                (unsigned char *) coding[i - k];
                                l++;
                        }
                } else {
                        if (j < m) {
                                if (i < k)
                                        tmp_target[j] =
                                                (unsigned char *) data[i];
                                else
                                        tmp_target[j] =
                                                (unsigned char *) coding[i - k];
                                j++;
                        }
                }
        }
        *source = tmp_source;
        *target = tmp_target;
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
                while (minio_src_in_err(r, src_err_list))
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
