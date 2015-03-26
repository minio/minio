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
	"unsafe"
)

// Technique - type of matrix type used in encoding
type Technique uint8

// Different types of supported matrix types
const (
	Vandermonde Technique = iota
	Cauchy
	None
)

// Default Data and Parity blocks
const (
	K = 10
	M = 3
)

// Block alignment
const (
	SIMDAlign = 32
)

// EncoderParams is a configuration set for building an encoder. It is created using ValidateParams.
type EncoderParams struct {
	K         uint8
	M         uint8
	Technique Technique // cauchy or vandermonde matrix (RS)
}

// Encoder is an object used to encode and decode data.
type Encoder struct {
	params                   *EncoderParams
	encodeMatrix, encodeTbls *C.uint8_t
	decodeMatrix, decodeTbls *C.uint8_t
	decodeIndex              *C.uint32_t
}

// ParseEncoderParams creates an EncoderParams object.
//
// k and m represent the matrix size, which corresponds to the protection level
// technique is the matrix type. Valid inputs are Cauchy (recommended) or Vandermonde.
//
func ParseEncoderParams(k, m uint8, technique Technique) (*EncoderParams, error) {
	if k < 1 {
		return nil, errors.New("k cannot be zero")
	}

	if m < 1 {
		return nil, errors.New("m cannot be zero")
	}

	if k+m > 255 {
		return nil, errors.New("(k + m) cannot be bigger than Galois field GF(2^8) - 1")
	}

	switch technique {
	case Vandermonde:
		break
	case Cauchy:
		break
	default:
		return nil, errors.New("Technique can be either vandermonde or cauchy")
	}

	return &EncoderParams{
		K:         k,
		M:         m,
		Technique: technique,
	}, nil
}

// NewEncoder creates an encoder object with a given set of parameters.
func NewEncoder(ep *EncoderParams) *Encoder {
	var k = C.int(ep.K)
	var m = C.int(ep.M)

	var encodeMatrix *C.uint8_t
	var encodeTbls *C.uint8_t

	C.minio_init_encoder(C.int(ep.Technique), k, m, &encodeMatrix,
		&encodeTbls)

	return &Encoder{
		params:       ep,
		encodeMatrix: encodeMatrix,
		encodeTbls:   encodeTbls,
		decodeMatrix: nil,
		decodeTbls:   nil,
		decodeIndex:  nil,
	}
}

// GetEncodedBlocksLen - total length of all encoded blocks
func GetEncodedBlocksLen(inputLen int, k, m uint8) (outputLen int) {
	outputLen = GetEncodedBlockLen(inputLen, k) * int(k+m)
	return outputLen
}

// GetEncodedBlockLen - length per block of encoded blocks
func GetEncodedBlockLen(inputLen int, k uint8) (encodedOutputLen int) {
	alignment := int(k) * SIMDAlign
	remainder := inputLen % alignment

	paddedInputLen := inputLen
	if remainder != 0 {
		paddedInputLen = inputLen + (alignment - remainder)
	}
	encodedOutputLen = paddedInputLen / int(k)
	return encodedOutputLen
}

// Encode erasure codes a block of data in "k" data blocks and "m" parity blocks.
// Output is [k+m][]blocks of data and parity slices.
func (e *Encoder) Encode(inputData []byte) (encodedBlocks [][]byte, err error) {
	k := int(e.params.K) // "k" data blocks
	m := int(e.params.M) // "m" parity blocks
	n := k + m           // "n" total encoded blocks

	// Length of a single encoded chunk.
	// Total number of encoded chunks = "k" data  + "m" parity blocks
	encodedBlockLen := GetEncodedBlockLen(len(inputData), uint8(k))

	// Length of total number of "k" data chunks
	encodedDataBlocksLen := encodedBlockLen * k

	// Length of extra padding required for the data blocks.
	encodedDataBlocksPadLen := encodedDataBlocksLen - len(inputData)

	// Extend inputData buffer to accommodate coded data blocks if necesssary
	if encodedDataBlocksPadLen > 0 {
		padding := make([]byte, encodedDataBlocksPadLen)
		// Expand with new padded blocks to the byte array
		inputData = append(inputData, padding...)
	}

	// Extend inputData buffer to accommodate coded parity blocks
	{ // Local Scope
		encodedParityBlocksLen := encodedBlockLen * m
		parityBlocks := make([]byte, encodedParityBlocksLen)
		inputData = append(inputData, parityBlocks...)
	}

	// Allocate memory to the "encoded blocks" return buffer
	encodedBlocks = make([][]byte, n) // Return buffer

	// Nessary to bridge Go to the C world. C requires 2D arry of pointers to
	// byte array. "encodedBlocks" is a 2D slice.
	pointersToEncodedBlock := make([]*byte, n) // Pointers to encoded blocks.

	// Copy data block slices to encoded block buffer
	for i := 0; i < k; i++ {
		encodedBlocks[i] = inputData[i*encodedBlockLen : (i+1)*encodedBlockLen]
		pointersToEncodedBlock[i] = &encodedBlocks[i][0]
	}

	// Copy erasure block slices to encoded block buffer
	for i := k; i < n; i++ {
		encodedBlocks[i] = make([]byte, encodedBlockLen)
		pointersToEncodedBlock[i] = &encodedBlocks[i][0]
	}

	// Erasure code the data into K data blocks and M parity
	// blocks. Only the parity blocks are filled. Data blocks remain
	// intact.
	C.ec_encode_data(C.int(encodedBlockLen), C.int(k), C.int(m), e.encodeTbls,
		(**C.uint8_t)(unsafe.Pointer(&pointersToEncodedBlock[:k][0])), // Pointers to data blocks
		(**C.uint8_t)(unsafe.Pointer(&pointersToEncodedBlock[k:][0]))) // Pointers to parity blocks

	return encodedBlocks, nil
}
