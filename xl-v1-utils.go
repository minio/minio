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

// readAll reads from bucket, object until an error or returns the data it read until io.EOF.
func readAll(disk StorageAPI, bucket, object string) ([]byte, error) {
	var writer = new(bytes.Buffer)
	startOffset := int64(0)
	// Read until io.EOF.
	for {
		buf := make([]byte, blockSizeV1)
		n, err := disk.ReadFile(bucket, object, startOffset, buf)
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
