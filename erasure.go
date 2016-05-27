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

import "github.com/klauspost/reedsolomon"

// erasure storage layer.
type erasure struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataBlocks   int
	ParityBlocks int
	storageDisks []StorageAPI
	distribution []int
}

// newErasure instantiate a new erasure.
func newErasure(disks []StorageAPI, distribution []int) *erasure {
	// Initialize E.
	e := &erasure{}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(disks)/2, len(disks)/2

	// Initialize reed solomon encoding.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	fatalIf(err, "Unable to initialize reedsolomon package.")

	// Save the reedsolomon.
	e.DataBlocks = dataBlocks
	e.ParityBlocks = parityBlocks
	e.ReedSolomon = rs

	// Save all the initialized storage disks.
	e.storageDisks = disks

	// Save the distribution.
	e.distribution = distribution

	// Return successfully initialized.
	return e
}
