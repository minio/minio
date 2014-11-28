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

#ifndef __ENCODE_H__
#define __ENCODE_H__

#define SIMD_ALIGN 32
void minio_init_encoder (int technique, int k, int m,
                         unsigned char **encode_matrix,
                         unsigned char **encode_tbls);
unsigned int calc_chunk_size (int k, unsigned int split_len);
#endif /* __ENCODE_H__ */
