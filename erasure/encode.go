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
// #include "cpufeatures.h"
import "C"
import (
	"errors"
	"unsafe"
)

const (
	VANDERMONDE = iota
	CAUCHY      = iota
)

const (
	DEFAULT_K = 10
	DEFAULT_M = 3
)

type EncoderParams struct {
	k,
	m,
	w,
	n,
	technique int // cauchy or vandermonde matrix (RS)
}

type Encoder struct {
	p *EncoderParams
	k,
	m,
	w C.int
	encode_matrix,
	encode_tbls,
	decode_matrix,
	decode_tbls *C.uchar
}

// Parameter validation
func ValidateParams(k, m, w, technique int) (*EncoderParams, error) {
	if k < 1 {
		return nil, errors.New("k cannot be zero")
	}

	if m < 1 {
		return nil, errors.New("m cannot be zero")
	}

	if k+m > 255 {
		return nil, errors.New("(k + m) cannot be bigger than Galois field GF(2^8) - 1")
	}

	if 1<<uint(w) < k+m {
		return nil, errors.New("Wordsize should be bigger than Galois field GF(2^8) - 1")
	}

	if w < 0 {
		return nil, errors.New("Wordsize cannot be negative")
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
		k:         k,
		m:         m,
		w:         w,
		n:         k + m,
		technique: technique,
	}, nil
}

func NewEncoder(ep *EncoderParams) *Encoder {
	var k = C.int(ep.k)
	var m = C.int(ep.m)
	var w = C.int(ep.w)
	var n = C.int(ep.n)

	var encode_matrix *C.uchar
	var encode_tbls *C.uchar

	var matrix_size C.size_t
	var encode_tbls_size C.size_t

	matrix_size = C.size_t(k * n)
	encode_matrix = (*C.uchar)(unsafe.Pointer(C.malloc(matrix_size)))
	defer C.free(unsafe.Pointer(encode_matrix))

	encode_tbls_size = C.size_t(k * n * 32)
	encode_tbls = (*C.uchar)(unsafe.Pointer(C.malloc(encode_tbls_size)))
	defer C.free(unsafe.Pointer(encode_tbls))

	if ep.technique == VANDERMONDE {
		// Commonly used method for choosing coefficients in erasure encoding
		// but does not guarantee invertable for every sub matrix.  For large
		// k it is possible to find cases where the decode matrix chosen from
		// sources and parity not in erasure are not invertable. Users may
		// want to adjust for k > 5.
		//   -- Intel
		C.gf_gen_rs_matrix(encode_matrix, n, k)
	} else if ep.technique == CAUCHY {
		C.gf_gen_cauchy1_matrix(encode_matrix, n, k)
	}

	C.ec_init_tables(k, m, encode_matrix, encode_tbls)

	return &Encoder{
		p:             ep,
		k:             k,
		m:             m,
		w:             w,
		encode_matrix: encode_matrix,
		encode_tbls:   encode_tbls,
		decode_matrix: nil,
		decode_tbls:   nil,
	}
}

func (e *Encoder) CalcChunkSize(block_len int) int {
	var alignment int = e.p.k * e.p.w
	var padding = block_len % alignment
	var padded_len int

	if padding > 0 {
		padded_len = block_len + (alignment - padding)
	} else {
		padded_len = block_len
	}
	return padded_len / e.p.k
}

func (e *Encoder) Encode(block []byte) ([][]byte, int) {
	var block_len = len(block)

	chunk_size := e.CalcChunkSize(block_len)
	padded_len := chunk_size * e.p.k

	if (padded_len - block_len) > 0 {
		s := make([]byte, (padded_len - block_len))
		// Expand with new padded blocks to the byte array
		block = append(block, s...)
	}

	coded_len := chunk_size * e.p.m
	c := make([]byte, coded_len)
	block = append(block, c...)

	// Allocate chunks
	chunks := make([][]byte, e.p.n)
	pointers := make([]*byte, e.p.n)

	var i int
	// Add data and code blocks to chunks
	for i = 0; i < e.p.n; i++ {
		chunks[i] = block[i*chunk_size : (i+1)*chunk_size]
		pointers[i] = &chunks[i][0]
	}

	data := (**C.uchar)(unsafe.Pointer(&pointers[:e.p.k][0]))
	coding := (**C.uchar)(unsafe.Pointer(&pointers[e.p.k:][0]))

	C.ec_encode_data(C.int(chunk_size), e.k, e.m, e.encode_tbls, data,
		coding)

	return chunks, block_len
}

func GetEncoder(ep EncoderParams) *Encoder {
	return DefaultCache.GetC(ep)
}

func Encode(data []byte, ep EncoderParams) (chunks [][]byte, length int) {
	return GetEncoder(ep).Encode(data)
}
