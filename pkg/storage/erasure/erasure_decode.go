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

// +build amd64

package erasure

// #cgo CFLAGS: -O0
// #include <stdlib.h>
// #include "ec-code.h"
// #include "ec-common.h"
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

func (e *Encoder) Decode(chunks [][]byte, length int) ([]byte, error) {
	var decode_matrix *C.uint8_t
	var decode_tbls *C.uint8_t
	var decode_index *C.uint32_t
	var source, target **C.uint8_t

	k := int(e.k)
	n := int(e.k + e.m)

	if len(chunks) != n {
		return nil, errors.New(fmt.Sprintf("chunks length must be %d", n))
	}

	chunk_size := int(C.minio_calc_chunk_size(e.k, C.uint32_t(length)))

	error_index := make([]int, n+1)
	var err_count int = 0

	for i := range chunks {
		// Check of chunks are really null
		if chunks[i] == nil || len(chunks[i]) == 0 {
			error_index[err_count] = i
			err_count++
		}
	}
	error_index[err_count] = -1
	err_count++

	// Too many missing chunks, cannot be more than parity `m`
	if err_count-1 > (n - k) {
		return nil, errors.New("too many erasures requested, can't decode")
	}

	error_index_ptr := int2cInt(error_index[:err_count])

	for i := range chunks {
		if chunks[i] == nil || len(chunks[i]) == 0 {
			chunks[i] = make([]byte, chunk_size)
		}
	}

	C.minio_init_decoder(error_index_ptr, e.k, e.k+e.m, C.int(err_count-1),
		e.encode_matrix, &decode_matrix, &decode_tbls, &decode_index)

	pointers := make([]*byte, n)
	for i := range chunks {
		pointers[i] = &chunks[i][0]
	}

	data := (**C.uint8_t)(unsafe.Pointer(&pointers[0]))

	ret := C.minio_get_source_target(C.int(err_count-1), e.k, e.m, error_index_ptr,
		decode_index, data, &source, &target)

	if int(ret) == -1 {
		return nil, errors.New("Decoding source target failed")
	}

	C.ec_encode_data(C.int(chunk_size), e.k, C.int(err_count-1), decode_tbls,
		source, target)

	recovered_output := make([]byte, 0, chunk_size*k)
	for i := 0; i < k; i++ {
		recovered_output = append(recovered_output, chunks[i]...)
	}

	e.decode_matrix = decode_matrix
	e.decode_tbls = decode_tbls

	return recovered_output[:length], nil
}
