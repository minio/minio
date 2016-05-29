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
	"encoding/hex"
	"hash"

	"github.com/klauspost/reedsolomon"
)

// erasure storage layer.
type erasureConfig struct {
	reedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	dataBlocks   int                 // Calculated data disks.
	storageDisks []StorageAPI        // Initialized storage disks.
	distribution []int               // Erasure block distribution.
	hashWriters  []hash.Hash         // Allocate hash writers.

	// Carries hex checksums needed for validating Reads.
	hashChecksums []string
	checkSumAlgo  string
}

// newErasure instantiate a new erasure.
func newErasure(disks []StorageAPI, distribution []int) *erasureConfig {
	// Initialize E.
	e := &erasureConfig{}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(disks)/2, len(disks)/2

	// Initialize reed solomon encoding.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	fatalIf(err, "Unable to initialize reedsolomon package.")

	// Save the reedsolomon.
	e.dataBlocks = dataBlocks
	e.reedSolomon = rs

	// Save all the initialized storage disks.
	e.storageDisks = disks

	// Save the distribution.
	e.distribution = distribution

	// Return successfully initialized.
	return e
}

// SaveAlgo - FIXME.
func (e *erasureConfig) SaveAlgo(algo string) {
	e.checkSumAlgo = algo
}

// Save hex encoded hashes - saves hashes that need to be validated
// during reads for each blocks.
func (e *erasureConfig) SaveHashes(hashes []string) {
	e.hashChecksums = hashes
}

// InitHash - initializes new hash for all blocks.
func (e *erasureConfig) InitHash(algo string) {
	e.hashWriters = make([]hash.Hash, len(e.storageDisks))
	for index := range e.storageDisks {
		e.hashWriters[index] = newHash(algo)
	}
}

// GetHashes - returns a slice of hex encoded hash.
func (e erasureConfig) GetHashes() []string {
	var hexHashes = make([]string, len(e.storageDisks))
	for index, hashWriter := range e.hashWriters {
		hexHashes[index] = hex.EncodeToString(hashWriter.Sum(nil))
	}
	return hexHashes
}
