/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	encoding "github.com/minio/minio/pkg/erasure"
	"github.com/minio/minio/pkg/probe"
)

// encoder internal struct
type encoder struct {
	encoder *encoding.Erasure
	k, m    uint8
}

// newEncoder - instantiate a new encoder
func newEncoder(k, m uint8) (encoder, *probe.Error) {
	e := encoder{}
	params, err := encoding.ValidateParams(k, m)
	if err != nil {
		return encoder{}, probe.NewError(err)
	}
	e.encoder = encoding.NewErasure(params)
	e.k = k
	e.m = m
	return e, nil
}

// TODO - think again if this is needed
// GetEncodedBlockLen - wrapper around erasure function with the same name
func (e encoder) GetEncodedBlockLen(dataLength int) (int, *probe.Error) {
	if dataLength <= 0 {
		return 0, probe.NewError(InvalidArgument{})
	}
	return encoding.GetEncodedBlockLen(dataLength, e.k), nil
}

// Encode - erasure code input bytes
func (e encoder) Encode(data []byte) ([][]byte, *probe.Error) {
	if data == nil {
		return nil, probe.NewError(InvalidArgument{})
	}
	encodedData, err := e.encoder.Encode(data)
	if err != nil {
		return nil, probe.NewError(err)
	}
	return encodedData, nil
}

// Decode - erasure decode input encoded bytes
func (e encoder) Decode(encodedData [][]byte, dataLength int) ([]byte, *probe.Error) {
	decodedData, err := e.encoder.Decode(encodedData, dataLength)
	if err != nil {
		return nil, probe.NewError(err)
	}
	return decodedData, nil
}
