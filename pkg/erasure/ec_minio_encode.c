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

#include <stdlib.h>
#include <stdio.h>

#include "ec.h"
#include "ec_minio_common.h"

/*
  Generate encode matrix during the encoding phase
*/

int32_t minio_init_encoder (int k, int m, unsigned char **encode_matrix, unsigned char **encode_tbls)
{
        unsigned char *tmp_matrix;
        unsigned char *tmp_tbls;

        tmp_matrix = (unsigned char *) malloc (k * (k + m));
        tmp_tbls = (unsigned char *) malloc (k * (k + m) * 32);

        if (k < 5) {
                /*
                  Commonly used method for choosing coefficients in erasure
                  encoding but does not guarantee invertable for every sub
                  matrix.  For large k it is possible to find cases where the
                  decode matrix chosen from sources and parity not in erasure
                  are not invertable. Users may want to adjust for k > 5.
                  -- Intel
                */
		gf_gen_rs_matrix (tmp_matrix, k + m, k);
        } else {
		gf_gen_cauchy1_matrix (tmp_matrix, k + m, k);
        }

	ec_init_tables(k, m, &tmp_matrix[k * k], tmp_tbls);

        *encode_matrix = tmp_matrix;
        *encode_tbls = tmp_tbls;

        return 0;
}
