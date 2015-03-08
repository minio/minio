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

package erasure1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"strconv"
)

const (
	Version = uint32(1)
)

// DataHeader represents the structure serialized to gob.
type DataHeader struct {
	// object + block stored
	Key string
	// chunk index of encoded block
	ChunkIndex uint8
	// Original Length of the block output
	OriginalLength uint32
	// Data Blocks
	EncoderK uint8
	// Parity Blocks
	EncoderM uint8
	// Matrix Technique
	EncoderTechnique EncoderTechnique
}

// EncoderTechnique specified the Matrix type used in encoding
type EncoderTechnique int

const (
	// Vandermonde matrix type
	Vandermonde EncoderTechnique = iota
	// Cauchy matrix type
	Cauchy
)

// validate populated header
func validateHeader(header DataHeader) error {
	if header.Key == "" {
		return errors.New("Empty Key")
	}

	if header.EncoderTechnique > 1 {
		return errors.New("Invalid encoder technique")
	}

	return nil
}

// Write returns error upon any failure
func Write(target io.Writer, key string, part uint8, length uint32, k, m uint8, technique EncoderTechnique, data io.Reader) error {
	header := DataHeader{
		Key:              key,
		ChunkIndex:       part,
		OriginalLength:   length,
		EncoderK:         k,
		EncoderM:         m,
		EncoderTechnique: technique,
	}

	if err := validateHeader(header); err != nil {
		return err
	}

	var headerBuffer bytes.Buffer
	// encode header
	encoder := gob.NewEncoder(&headerBuffer)
	encoder.Encode(header)

	// write version
	binary.Write(target, binary.LittleEndian, Version)

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

// Read an erasure block
func ReadHeader(reader io.Reader) (dataHeader DataHeader, err error) {
	versionArray := make([]byte, 4)
	if err := binary.Read(reader, binary.LittleEndian, versionArray); err != nil {
		return dataHeader, err
	}
	version := binary.LittleEndian.Uint32(versionArray)
	if version > Version || version == 0 {
		return dataHeader, errors.New("Unknown version: " + strconv.FormatUint(uint64(version), 10))
	}
	decoder := gob.NewDecoder(reader)
	err = decoder.Decode(&dataHeader)
	return dataHeader, err
}
