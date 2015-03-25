/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

// Decode decodes 2 tuple data containing (k + m) chunks back into its original form.
// Additionally original block length should also be provided as input.
//
// Decoded data is exactly similar in length and content as the original data.
func (e *Encoder) Decode(chunks [][]byte, length int) ([]byte, error) {
	var decodeMatrix *C.uint8_t
	var decodeTbls *C.uint8_t
	var decodeIndex *C.uint32_t
	var source, target **C.uint8_t

	k := int(e.params.K)
	m := int(e.params.M)
	n := k + m
	if len(chunks) != n {
		msg := fmt.Sprintf("chunks length must be %d", n)
		return nil, errors.New(msg)
	}
	chunkLen := GetEncodedBlockLen(length, uint8(k))

	errorIndex := make([]int, n+1)
	var errCount int

	for i := range chunks {
		// Check of chunks are really null
		if chunks[i] == nil || len(chunks[i]) == 0 {
			errorIndex[errCount] = i
			errCount++
		}
	}
	errorIndex[errCount] = -1
	errCount++

	// Too many missing chunks, cannot be more than parity `m`
	if errCount-1 > int(n-k) {
		return nil, errors.New("too many erasures requested, can't decode")
	}

	errorIndexPtr := int2CInt(errorIndex[:errCount])

	for i := range chunks {
		if chunks[i] == nil || len(chunks[i]) == 0 {
			chunks[i] = make([]byte, chunkLen)
		}
	}

	C.minio_init_decoder(errorIndexPtr, C.int(k), C.int(n), C.int(errCount-1),
		e.encodeMatrix, &decodeMatrix, &decodeTbls, &decodeIndex)

	pointers := make([]*byte, n)
	for i := range chunks {
		pointers[i] = &chunks[i][0]
	}

	data := (**C.uint8_t)(unsafe.Pointer(&pointers[0]))

	ret := C.minio_get_source_target(C.int(errCount-1), C.int(k), C.int(m), errorIndexPtr,
		decodeIndex, data, &source, &target)

	if int(ret) == -1 {
		return nil, errors.New("Decoding source target failed")
	}

	C.ec_encode_data(C.int(chunkLen), C.int(k), C.int(errCount-1), decodeTbls,
		source, target)

	recoveredOutput := make([]byte, 0, chunkLen*int(k))
	for i := 0; i < int(k); i++ {
		recoveredOutput = append(recoveredOutput, chunks[i]...)
	}

	// TODO cache this if necessary
	e.decodeMatrix = decodeMatrix
	e.decodeTbls = decodeTbls

	return recoveredOutput[:length], nil
}
