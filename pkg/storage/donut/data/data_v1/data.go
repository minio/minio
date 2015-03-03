/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package data_v1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

type Metadata map[string]string

type DataHeader struct {
	Key           string
	Part          uint8
	Metadata      Metadata
	EncoderParams EncoderParams
}

type EncoderTechnique int

const (
	Vandermonde EncoderTechnique = iota
	Cauchy
)

type EncoderParams struct {
	Length    uint32
	K         uint8
	M         uint8
	Technique EncoderTechnique
}

func NewHeader(key string, part uint8, metadata Metadata, encoderParams EncoderParams) DataHeader {
	header := DataHeader{}
	header.Key = key
	header.Part = part
	header.Metadata = metadata
	header.EncoderParams = EncoderParams{
		Length:    encoderParams.Length,
		K:         encoderParams.K,
		M:         encoderParams.M,
		Technique: encoderParams.Technique,
	}
	return header
}

func ValidateHeader(header DataHeader) bool {
	if header.Key == "" || header.Part < 0 || len(header.Metadata) < 2 {
		return false
	}

	if header.EncoderParams.Length < 0 || header.EncoderParams.Technique > 1 {
		return false
	}

	return true
}

func WriteData(target io.Writer, header DataHeader, data io.Reader) error {
	if !ValidateHeader(header) {
		return fmt.Errorf("Invalid header")
	}

	var headerBuffer bytes.Buffer
	// encode header
	encoder := gob.NewEncoder(&headerBuffer)
	encoder.Encode(header)

	// write version
	binary.Write(target, binary.LittleEndian, uint32(1))

	// write encoded header
	if _, err := io.Copy(target, &headerBuffer); err != nil {
		return err
	}
	// write data
	if _, err := io.Copy(target, data); err != nil {
		return err
	}
	return nil
}
