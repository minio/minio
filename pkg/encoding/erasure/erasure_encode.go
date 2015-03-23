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

type Technique int

const (
	Vandermonde Technique = iota
	Cauchy
)

const (
	K = 10
	M = 3
)

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
	params *EncoderParams
	encode_matrix,
	encode_tbls,
	decode_matrix,
	decode_tbls *C.uint8_t
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
	var encode_matrix *C.uint8_t
	var encode_tbls *C.uint8_t

	k := C.int(ep.K)
	m := C.int(ep.M)

	C.minio_init_encoder(C.int(ep.Technique), k, m, &encode_matrix,
		&encode_tbls)

	return &Encoder{
		params:        ep,
		encode_matrix: encode_matrix,
		encode_tbls:   encode_tbls,
		decode_matrix: nil,
		decode_tbls:   nil,
	}
}

func GetEncodedLen(inputLen int, k, m uint8) (outputLen int) {
	outputLen = GetEncodedChunkLen(inputLen, k) * int(k+m)
	return outputLen
}

func GetEncodedChunkLen(inputLen int, k uint8) (outputChunkLen int) {
	alignment := int(k) * SIMDAlign
	remainder := inputLen % alignment

	paddedInputLen := inputLen
	if remainder != 0 {
		paddedInputLen = inputLen + (alignment - remainder)
	}
	outputChunkLen = paddedInputLen / int(k)
	return outputChunkLen
}

// Encode encodes a block of data. The input is the original data. The output
// is a 2 tuple containing (k + m) chunks of erasure encoded data and the
// length of the original object.
func (e *Encoder) Encode(input []byte) ([][]byte, error) {
	inputLen := len(input)
	k := C.int(e.params.K)
	m := C.int(e.params.M)
	n := k + m

	chunkLen := GetEncodedChunkLen(inputLen, e.params.K)
	encodedDataLen := chunkLen * int(k)
	paddedDataLen := int(encodedDataLen) - inputLen

	if paddedDataLen > 0 {
		s := make([]byte, paddedDataLen)
		// Expand with new padded blocks to the byte array
		input = append(input, s...)
	}

	encodedParityLen := chunkLen * int(e.params.M)
	c := make([]byte, encodedParityLen)
	input = append(input, c...)

	// encodedOutLen := encodedDataLen + encodedParityLen

	// Allocate chunks
	chunks := make([][]byte, k+m)
	pointers := make([]*byte, k+m)

	var i int
	// Add data blocks to chunks
	for i = 0; i < int(k); i++ {
		chunks[i] = input[i*chunkLen : (i+1)*chunkLen]
		pointers[i] = &chunks[i][0]
	}

	for i = int(k); i < int(n); i++ {
		chunks[i] = make([]byte, chunkLen)
		pointers[i] = &chunks[i][0]
	}

	data := (**C.uint8_t)(unsafe.Pointer(&pointers[:k][0]))
	coding := (**C.uint8_t)(unsafe.Pointer(&pointers[k:][0]))

	C.ec_encode_data(C.int(chunkLen), k, m, e.encode_tbls, data,
		coding)
	return chunks, nil
}
