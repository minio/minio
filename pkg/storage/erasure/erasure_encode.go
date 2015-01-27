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
	"unsafe"
)

const (
	VANDERMONDE = iota
	CAUCHY
)

const (
	K = 10
	M = 3
)

// EncoderParams is a configuration set for building an encoder. It is created using ValidateParams.
type EncoderParams struct {
	K,
	M,
	Technique int // cauchy or vandermonde matrix (RS)
}

// Encoder is an object used to encode and decode data.
type Encoder struct {
	p *EncoderParams
	k,
	m C.int
	encode_matrix,
	encode_tbls,
	decode_matrix,
	decode_tbls *C.uint8_t
}

// ParseEncoderParams creates an EncoderParams object.
//
// k and n represent the matrix size, which corresponds to the protection level.
//
// technique is the matrix type. Valid inputs are CAUCHY (recommended) or VANDERMONDE.
func ParseEncoderParams(k, m, technique int) (*EncoderParams, error) {
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
	case VANDERMONDE:
		break
	case CAUCHY:
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

// NewEncoder creates an encoder with a given set of parameters.
func NewEncoder(ep *EncoderParams) *Encoder {
	var k = C.int(ep.K)
	var m = C.int(ep.M)

	var encode_matrix *C.uint8_t
	var encode_tbls *C.uint8_t

	C.minio_init_encoder(C.int(ep.Technique), k, m, &encode_matrix,
		&encode_tbls)

	return &Encoder{
		p:             ep,
		k:             k,
		m:             m,
		encode_matrix: encode_matrix,
		encode_tbls:   encode_tbls,
		decode_matrix: nil,
		decode_tbls:   nil,
	}
}

// Encode encodes a block of data. The input is the original data. The output
// is a 2 tuple containing (k + m) chunks of erasure encoded data and the
// length of the original object.
func (e *Encoder) Encode(block []byte) ([][]byte, int) {
	var block_len = len(block)

	chunk_size := int(C.minio_calc_chunk_size(e.k, C.uint32_t(block_len)))
	chunk_len := chunk_size * e.p.K
	pad_len := chunk_len - block_len

	if pad_len > 0 {
		s := make([]byte, pad_len)
		// Expand with new padded blocks to the byte array
		block = append(block, s...)
	}

	coded_len := chunk_size * e.p.M
	c := make([]byte, coded_len)
	block = append(block, c...)

	// Allocate chunks
	chunks := make([][]byte, e.p.K+e.p.M)
	pointers := make([]*byte, e.p.K+e.p.M)

	var i int
	// Add data blocks to chunks
	for i = 0; i < e.p.K; i++ {
		chunks[i] = block[i*chunk_size : (i+1)*chunk_size]
		pointers[i] = &chunks[i][0]
	}

	for i = e.p.K; i < (e.p.K + e.p.M); i++ {
		chunks[i] = make([]byte, chunk_size)
		pointers[i] = &chunks[i][0]
	}

	data := (**C.uint8_t)(unsafe.Pointer(&pointers[:e.p.K][0]))
	coding := (**C.uint8_t)(unsafe.Pointer(&pointers[e.p.K:][0]))

	C.ec_encode_data(C.int(chunk_size), e.k, e.m, e.encode_tbls, data,
		coding)
	return chunks, block_len
}
