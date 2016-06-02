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
	"bytes"
	"io"
	"math/rand"
	"path"
	"time"
)

// randInts - uses Knuth Fisher-Yates shuffle algorithm for generating uniform shuffling.
func randInts(count int) []int {
	rand.Seed(time.Now().UTC().UnixNano()) // Seed with current time.
	ints := make([]int, count)
	for i := 0; i < count; i++ {
		ints[i] = i + 1
	}
	for i := 0; i < count; i++ {
		// Choose index uniformly in [i, count-1]
		r := i + rand.Intn(count-i)
		ints[r], ints[i] = ints[i], ints[r]
	}
	return ints
}

// readAll - returns contents from volume/path as byte array.
func readAll(disk StorageAPI, volume string, path string) ([]byte, error) {
	var writer = new(bytes.Buffer)
	startOffset := int64(0)

	// Allocate 10MiB buffer.
	buf := make([]byte, blockSizeV1)

	// Read until io.EOF.
	for {
		n, err := disk.ReadFile(volume, path, startOffset, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		writer.Write(buf[:n])
		startOffset += n
	}
	return writer.Bytes(), nil
}

// readXLMeta reads `xl.json` returns contents as byte array.
func readXLMeta(disk StorageAPI, bucket string, object string) ([]byte, error) {
	var writer = new(bytes.Buffer)
	startOffset := int64(0)

	// Allocate 2MiB buffer, this is sufficient for the most of `xl.json`.
	buf := make([]byte, 2*1024*1024)

	// Read until io.EOF.
	for {
		n, err := disk.ReadFile(bucket, path.Join(object, xlMetaJSONFile), startOffset, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		writer.Write(buf[:n])
		startOffset += n
	}
	return writer.Bytes(), nil
}
