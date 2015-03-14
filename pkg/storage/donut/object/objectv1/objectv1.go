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

package objectv1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"strconv"
)

// Package Version
const Version = uint32(1)

// ObjectMetadata contains information necessary to reconstruct the object and basic object metadata.
type ObjectMetadata struct {
	Bucket      string
	Key         string
	ErasurePart uint16
	EncodedPart uint8
}

// Write an encoded part to a writer
func Write(target io.Writer, metadata ObjectMetadata, reader io.Reader) error {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, uint32(Version))
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(metadata); err != nil {
		return err
	}
	reader = io.MultiReader(buffer, reader)
	_, err := io.Copy(target, reader)
	return err
}

// ReadMetadata reads the first elements from the stream and returns the object metadata
func ReadMetadata(reader io.Reader) (metadata ObjectMetadata, err error) {
	versionBytes := make([]byte, 4)
	if err := binary.Read(reader, binary.LittleEndian, versionBytes); err != nil {
		return metadata, err
	}
	var version uint32
	version = binary.LittleEndian.Uint32(versionBytes)
	if version != 1 {
		return metadata, errors.New("Unknown Version: " + strconv.FormatUint(uint64(version), 10))
	}
	decoder := gob.NewDecoder(reader)
	err = decoder.Decode(&metadata)
	return metadata, err
}
