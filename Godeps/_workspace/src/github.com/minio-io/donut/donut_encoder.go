/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"errors"
	"strconv"

	encoding "github.com/minio-io/erasure"
	"github.com/minio-io/iodine"
)

// encoder internal struct
type encoder struct {
	encoder   *encoding.Erasure
	k, m      uint8
	technique encoding.Technique
}

// getErasureTechnique - convert technique string into Technique type
func getErasureTechnique(technique string) (encoding.Technique, error) {
	switch true {
	case technique == "Cauchy":
		return encoding.Cauchy, nil
	case technique == "Vandermonde":
		return encoding.Cauchy, nil
	default:
		return encoding.None, iodine.New(errors.New("Invalid erasure technique"), nil)
	}
}

// NewEncoder - instantiate a new encoder
func NewEncoder(k, m uint8, technique string) (Encoder, error) {
	errParams := map[string]string{
		"k":         strconv.FormatUint(uint64(k), 10),
		"m":         strconv.FormatUint(uint64(m), 10),
		"technique": technique,
	}
	e := encoder{}
	t, err := getErasureTechnique(technique)
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	params, err := encoding.ValidateParams(k, m, t)
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	e.encoder = encoding.NewErasure(params)
	e.k = k
	e.m = m
	e.technique = t
	return e, nil
}

// TODO - think again if this is needed
// GetEncodedBlockLen - wrapper around erasure function with the same name
func (e encoder) GetEncodedBlockLen(dataLength int) (int, error) {
	if dataLength <= 0 {
		return 0, iodine.New(errors.New("invalid argument"), nil)
	}
	return encoding.GetEncodedBlockLen(dataLength, e.k), nil
}

// Encode - erasure code input bytes
func (e encoder) Encode(data []byte) (encodedData [][]byte, err error) {
	if data == nil {
		return nil, iodine.New(errors.New("invalid argument"), nil)
	}
	encodedData, err = e.encoder.Encode(data)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return encodedData, nil
}

// Decode - erasure decode input encoded bytes
func (e encoder) Decode(encodedData [][]byte, dataLength int) (data []byte, err error) {
	decodedData, err := e.encoder.Decode(encodedData, dataLength)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return decodedData, nil
}
