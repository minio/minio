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

package erasure

// #cgo CFLAGS: -O0
// #include <stdlib.h>
// #include "ec.h"
// #include "ec_minio_common.h"
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

// Decode decodes erasure coded blocks of data into its original
// form. Erasure coded data contains K data blocks and M parity
// blocks. Decode can withstand data loss up to any M number of blocks.
//
// "encodedDataBlocks" is an array of K data blocks and M parity
// blocks. Data blocks are position and order dependent. Missing blocks
// are set to "nil". There must be at least "K" number of data|parity
// blocks.
//
// "dataLen" is the length of original source data
func (e *Erasure) Decode(encodedDataBlocks [][]byte, dataLen int) (decodedData []byte, err error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	var source, target **C.uchar

	k := int(e.params.K)
	m := int(e.params.M)
	n := k + m
	// We need the data and parity blocks preserved in the same order. Missing blocks are set to nil.
	if len(encodedDataBlocks) != n {
		msg := fmt.Sprintf("Encoded data blocks slice must of length [%d]", n)
		return nil, errors.New(msg)
	}

	// Length of a single encoded block
	encodedBlockLen := GetEncodedBlockLen(dataLen, uint8(k))

	// Keep track of errors per block.
	missingEncodedBlocks := make([]int, n+1)
	var missingEncodedBlocksCount int

	// Check for the missing encoded blocks
	for i := range encodedDataBlocks {
		if encodedDataBlocks[i] == nil || len(encodedDataBlocks[i]) == 0 {
			missingEncodedBlocks[missingEncodedBlocksCount] = i
			missingEncodedBlocksCount++
		}
	}

	// Cannot reconstruct original data. Need at least M number of data or parity blocks.
	if missingEncodedBlocksCount > m {
		return nil, fmt.Errorf("Cannot reconstruct original data. Need at least [%d]  data or parity blocks", m)
	}

	// Convert from Go int slice to C int array
	missingEncodedBlocksC := intSlice2CIntArray(missingEncodedBlocks[:missingEncodedBlocksCount])

	// Allocate buffer for the missing blocks
	for i := range encodedDataBlocks {
		if encodedDataBlocks[i] == nil || len(encodedDataBlocks[i]) == 0 {
			encodedDataBlocks[i] = make([]byte, encodedBlockLen)
		}
	}

	// If not already initialized, recompute and cache
	if e.decodeMatrix == nil || e.decodeTbls == nil || e.decodeIndex == nil {
		var decodeMatrix, decodeTbls *C.uchar
		var decodeIndex *C.uint32_t

		C.minio_init_decoder(missingEncodedBlocksC, C.int(k), C.int(n), C.int(missingEncodedBlocksCount),
			e.encodeMatrix, &decodeMatrix, &decodeTbls, &decodeIndex)

		// cache this for future needs
		e.decodeMatrix = decodeMatrix
		e.decodeTbls = decodeTbls
		e.decodeIndex = decodeIndex
	}

	// Make a slice of pointers to encoded blocks. Necessary to bridge to the C world.
	pointers := make([]*byte, n)
	for i := range encodedDataBlocks {
		pointers[i] = &encodedDataBlocks[i][0]
	}

	// Get pointers to source "data" and target "parity" blocks from the output byte array.
	ret := C.minio_get_source_target(C.int(missingEncodedBlocksCount), C.int(k), C.int(m), missingEncodedBlocksC,
		e.decodeIndex, (**C.uchar)(unsafe.Pointer(&pointers[0])), &source, &target)
	if int(ret) == -1 {
		return nil, errors.New("Unable to decode data")
	}

	// Decode data
	C.ec_encode_data(C.int(encodedBlockLen), C.int(k), C.int(missingEncodedBlocksCount), e.decodeTbls,
		source, target)

	// Allocate buffer to output buffer
	decodedData = make([]byte, 0, encodedBlockLen*int(k))
	for i := 0; i < int(k); i++ {
		decodedData = append(decodedData, encodedDataBlocks[i]...)
	}

	return decodedData[:dataLen], nil
}
