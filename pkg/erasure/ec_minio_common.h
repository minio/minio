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

#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>

int32_t minio_init_encoder (int k, int m,
                            unsigned char **encode_matrix,
                            unsigned char **encode_tbls);

int32_t minio_init_decoder (int32_t *error_index,
                            int k, int n, int errs,
                            unsigned char *encoding_matrix,
                            unsigned char **decode_matrix,
                            unsigned char **decode_tbls,
                            uint32_t **decode_index);

int32_t minio_get_source_target (int errs, int k, int m,
                                 int32_t *error_index,
                                 uint32_t *decode_index,
                                 unsigned char **buffs,
                                 unsigned char ***source,
                                 unsigned char ***target);
#endif /* __COMMON_H__ */
