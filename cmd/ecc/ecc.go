/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

// Package ecc implements the erasure coding and error correction
// of the MinIO server.
//
//
// Architecture
//
// Conceptually, MinIO implements S3, data collection, error detection and error correction
// in different layers:
//                       HTTP handler --------------------- S3, encryption, compression, ...
//                            |
//                            |
//                        Object API ---------------------- Backend implementation (FS, XL, Gateway, ...)
//                            |
//                            |
//                      Reconstruction -------------------- Reedsolomon Erasure-coding
//                            |
//                            |
//                      spawn | join  --------------------- Concurrent read scheduling
//                 +------+---+---+------+
//                 |      |       |      |
//                 |      |       |      |
//                Detection      Detection ----------------- Content verification (HighwayHash, BLAKE2, ...)
//                 |      |       |      |
//                 |      |       |      |
//               disk1  disk2   disk3  disk4 --------------- File / POSIX layer
//                 |      |       |      |
//                 |      |       |       \
//         part.1 -+      |       +-part.1 +--- part.1
//                 |      |       |         \
//         part.2 -+      |       +-part.2   +--- part.2
//                 |      |       |           \
//         part.3 -+   offline    +-part.3     +--- part.3
//
// The ecc package implements primitives to build the reconstruction,
// read scheduling and content verification layers.
package ecc

type errorType string

func (e errorType) Error() string { return string(e) }
