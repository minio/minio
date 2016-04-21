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
	"errors"
	"io"
	"strconv"
	"time"
)

// error type when key is not found.
var errMetadataKeyNotExist = errors.New("Key not found in fileMetadata.")

// This code is built on similar ideas of http.Header.
// Ref - https://golang.org/pkg/net/http/#Header

// A fileMetadata represents a metadata header mapping
// keys to sets of values.
type fileMetadata map[string][]string

// Add adds the key, value pair to the header.
// It appends to any existing values associated with key.
func (f fileMetadata) Add(key, value string) {
	f[key] = append(f[key], value)
}

// Set sets the header entries associated with key to
// the single element value. It replaces any existing
// values associated with key.
func (f fileMetadata) Set(key, value string) {
	f[key] = []string{value}
}

// Get gets the first value associated with the given key.
// If there are no values associated with the key, Get returns "".
// Get is a convenience method.  For more complex queries,
// access the map directly.
func (f fileMetadata) Get(key string) []string {
	if f == nil {
		return nil
	}
	v, ok := f[key]
	if !ok {
		return nil
	}
	return v
}

// Write writes a metadata in wire format.
func (f fileMetadata) Write(writer io.Writer) error {
	metadataBytes, err := json.Marshal(f)
	if err != nil {
		return err
	}
	_, err = writer.Write(metadataBytes)
	return err
}

// Get file size.
func (f fileMetadata) GetSize() (int64, error) {
	sizes := f.Get("file.size")
	if sizes == nil {
		return 0, errMetadataKeyNotExist
	}
	sizeStr := sizes[0]
	return strconv.ParseInt(sizeStr, 10, 64)
}

// Set file size.
func (f fileMetadata) SetSize(size int64) {
	f.Set("file.size", strconv.FormatInt(size, 10))
}

// Get file Modification time.
func (f fileMetadata) GetModTime() (time.Time, error) {
	timeStrs := f.Get("file.modTime")
	if timeStrs == nil {
		return time.Time{}, errMetadataKeyNotExist
	}
	return time.Parse(timeFormatAMZ, timeStrs[0])
}

// fileMetadataDecode - file metadata decode.
func fileMetadataDecode(reader io.Reader) (fileMetadata, error) {
	metadata := make(fileMetadata)
	decoder := json.NewDecoder(reader)
	// Unmarshalling failed, file possibly corrupted.
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}
