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
                            uint8_t **encode_matrix,
                            uint8_t **encode_tbls);

uint32_t minio_calc_chunk_size (int k, uint32_t split_len);

int32_t minio_init_decoder (int32_t *error_index,
                            int k, int n, int errs,
                            uint8_t *encoding_matrix,
                            uint8_t **decode_matrix,
                            uint8_t **decode_tbls,
                            uint32_t **decode_index);

int32_t minio_get_source_target (int errs, int k, int m,
                                 int32_t *error_index,
                                 uint32_t *decode_index,
                                 uint8_t **buffs,
                                 uint8_t ***source,
                                 uint8_t ***target);
#endif /* __COMMON_H__ */
