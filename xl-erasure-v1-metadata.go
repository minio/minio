/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"encoding/json"
	"io"
	"time"
)

// A xlMetaV1 represents a metadata header mapping keys to sets of values.
type xlMetaV1 struct {
	Version string `json:"version"`
	Stat    struct {
		Size    int64     `json:"size"`
		ModTime time.Time `json:"modTime"`
		Version int64     `json:"version"`
	} `json:"stat"`
	Erasure struct {
		DataBlocks   int   `json:"data"`
		ParityBlocks int   `json:"parity"`
		BlockSize    int64 `json:"blockSize"`
	} `json:"erasure"`
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
}

// Write writes a metadata in wire format.
func (m xlMetaV1) Write(writer io.Writer) error {
	metadataBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = writer.Write(metadataBytes)
	return err
}

// xlMetaV1Decode - file metadata decode.
func xlMetaV1Decode(reader io.Reader) (metadata xlMetaV1, err error) {
	decoder := json.NewDecoder(reader)
	// Unmarshalling failed, file possibly corrupted.
	if err = decoder.Decode(&metadata); err != nil {
		return xlMetaV1{}, err
	}
	return metadata, nil
}
