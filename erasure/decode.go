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

// +build linux
// amd64

package erasure

// #cgo CPPFLAGS: -Iisal/include
// #cgo LDFLAGS: isal/isa-l.so
// #include <stdlib.h>
// #include <erasure-code.h>
// #include <stdlib.h>
//
// #include "matrix_decode.h"
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

func (e *Encoder) Decode(chunks [][]byte, length int) ([]byte, error) {
	var decode_matrix *C.uchar
	var decode_tbls *C.uchar

	var matrix_size C.size_t
	var decode_tbls_size C.size_t

	k := int(e.p.k)
	n := int(e.p.n)

	if len(chunks) != n {
		return nil, errors.New(fmt.Sprintf("chunks length must be %d", n))
	}

	var chunk_size int = e.CalcChunkSize(length)

	src_err_list := make([]int, n+1)
	var err_count int = 0

	for i := range chunks {
		// Check of chunks are really null
		if chunks[i] == nil || len(chunks[i]) == 0 {
			src_err_list[err_count] = i
			err_count++
		}
	}
	src_err_list[err_count] = -1
	err_count++

	// Too many missing chunks, cannot be more than parity `m`
	if err_count-1 > e.p.m {
		return nil, errors.New("too many erasures requested, can't decode")
	}

	src_err_list_ptr := Int2cInt(src_err_list[:err_count])

	for i := range chunks {
		if chunks[i] == nil || len(chunks[i]) == 0 {
			chunks[i] = make([]byte, chunk_size)
		}
	}

	matrix_size = C.size_t(k * n)
	decode_matrix = (*C.uchar)(unsafe.Pointer(C.malloc(matrix_size)))
	defer C.free(unsafe.Pointer(decode_matrix))

	decode_tbls_size = C.size_t(k * n * 32)
	decode_tbls = (*C.uchar)(unsafe.Pointer(C.malloc(decode_tbls_size)))
	defer C.free(unsafe.Pointer(decode_tbls))

	C.gf_gen_decode_matrix(src_err_list_ptr, e.encode_matrix,
		decode_matrix, e.k, e.k+e.m, C.int(err_count-1), matrix_size)

	C.ec_init_tables(e.k, C.int(err_count-1), decode_matrix, decode_tbls)
	e.decode_matrix = decode_matrix
	e.decode_tbls = decode_tbls

	pointers := make([]*byte, n)
	for i := range chunks {
		pointers[i] = &chunks[i][0]
	}

	data := (**C.uchar)(unsafe.Pointer(&pointers[:k][0]))
	coding := (**C.uchar)(unsafe.Pointer(&pointers[k:][0]))

	C.ec_encode_data(C.int(matrix_size), e.k, C.int(err_count-1), e.decode_tbls,
		data, coding)

	recovered_output := make([]byte, 0, chunk_size*k)
	for i := 0; i < k; i++ {
		recovered_output = append(recovered_output, chunks[i]...)
	}
	return recovered_output[:length], nil
}

func Decode(chunks [][]byte, ep EncoderParams, length int) (block []byte, err error) {
	return GetEncoder(ep).Decode(chunks, length)
}
