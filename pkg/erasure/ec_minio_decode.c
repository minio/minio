/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

#include "ec.h"
#include "ec_minio_common.h"

static
int32_t _minio_src_index_in_error (int r, int32_t *error_index, int errs)
{
        int i;
        for (i = 0; i < errs; i++) {
                if (error_index[i] == r) {
                        // true
                        return 1;
                }
        }
        // false
        return 0;
}

// Separate out source data and target buffers
int32_t minio_get_source_target (int errs, int k, int m,
                                 int32_t *error_index,
                                 uint32_t *decode_index,
                                 unsigned char **buffs,
                                 unsigned char ***source,
                                 unsigned char ***target)
{
        int i;
        unsigned char *tmp_source[k];
        unsigned char *tmp_target[m];

        if (k < 0 || m < 0) {
                return -1;
        }

        memset (tmp_source, 0, k);
        memset (tmp_target, 0, m);

        for (i = 0; i < k; i++) {
                tmp_source[i] = (unsigned char *) buffs[decode_index[i]];
        }

        for (i = 0; i < m; i++) {
                if (i < errs)
                        tmp_target[i] = (unsigned char *) buffs[error_index[i]];
        }

        *source = tmp_source;
        *target = tmp_target;

	return 0;
}

/*
  Generate decode matrix during the decoding phase
*/

int minio_init_decoder (int32_t *error_index,
                        int k, int n, int errs,
                        unsigned char *encode_matrix,
                        unsigned char **decode_matrix,
                        unsigned char **decode_tbls,
                        uint32_t **decode_index)
{
        int i, j, r, l;

        uint32_t      *tmp_decode_index = (uint32_t *) malloc(sizeof(uint32_t) * k);
        unsigned char *input_matrix;
        unsigned char *inverse_matrix;
        unsigned char *tmp_decode_matrix;
        unsigned char *tmp_decode_tbls;

        input_matrix = (unsigned char *) malloc(sizeof(unsigned char) * k * n);
        inverse_matrix = (unsigned char *) malloc(sizeof(unsigned char) * k * n);
        tmp_decode_matrix = (unsigned char *) malloc(sizeof(unsigned char) * k * n);;
        tmp_decode_tbls = (unsigned char *) malloc(sizeof(unsigned char) * k * n * 32);

        for (i = 0, r = 0; i < k; i++, r++) {
                while (_minio_src_index_in_error(r, error_index, errs))
                        r++;
                for (j = 0; j < k; j++) {
                        input_matrix[k * i + j] = encode_matrix[k * r + j];
                }
                tmp_decode_index[i] = r;
        }

        // Not all vandermonde matrix can be inverted
        if (gf_invert_matrix(input_matrix, inverse_matrix, k) < 0) {
                free(tmp_decode_matrix);
                free(tmp_decode_tbls);
                free(tmp_decode_index);
                return -1;
        }

        for (l = 0; l < errs; l++) {
                if (error_index[l] < k) {
                        // decoding matrix elements for data chunks
                        for (j = 0; j < k; j++) {
                                tmp_decode_matrix[k * l + j] =
                                        inverse_matrix[k *
                                                       error_index[l] + j];
                        }
                } else {
                        // decoding matrix element for coding chunks
                        for (i = 0; i < k; i++) {
                                unsigned char s = 0;
                                for (j = 0; j < k; j++) {
                                        s ^= gf_mul(inverse_matrix[j * k + i],
                                                    encode_matrix[k *
                                                                  error_index[l] + j]);
                                }
                                tmp_decode_matrix[k * l + i] = s;
                        }
                }
        }

        ec_init_tables (k, errs, tmp_decode_matrix, tmp_decode_tbls);

        *decode_matrix = tmp_decode_matrix;
        *decode_tbls = tmp_decode_tbls;
        *decode_index = tmp_decode_index;

        return 0;
}
