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

#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>

#define SIMD_ALIGN 32

int32_t minio_init_encoder (int technique, int k, int m,
                            unsigned char **encode_matrix,
                            unsigned char **encode_tbls);

uint32_t minio_calc_chunk_size (int k,
                                uint32_t split_len);

int32_t minio_init_decoder (int *src_err_list,
                            unsigned char *encoding_matrix,
                            unsigned char **decode_matrix,
                            unsigned char **decode_tbls,
                            int k, int n, int errs);

int32_t minio_src_in_err (int r, int *src_err_list);

int32_t minio_get_source_target(int *src_err_list,
                                int errs, int k, int m,
                                unsigned char **data,
                                unsigned char **coding,
                                unsigned char ***source,
                                unsigned char ***target);
#endif /* __COMMON_H__ */
