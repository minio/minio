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

#ifndef __MATRIX_DECODE_H__
#define __MATRIX_DECODE_H__

int gf_gen_decode_matrix (int *src_err_list,
                          unsigned char *encoding_matrix,
                          unsigned char *decode_matrix, int k, int n,
                          int errs, size_t matrix_size);
#endif /* __MATRIX_DECODE_H__  */
