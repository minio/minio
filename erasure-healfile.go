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

import "encoding/hex"

// erasureHealFile heals part file of an object. It never writes the healed file to the actual
// location, but to a temporary location ".minio/tmpPath" as specified by the caller. It will
// be the responsibility of the caller to rename to the actual location.
func erasureHealFile(latestDisks []StorageAPI, outDatedDisks []StorageAPI,
	volume, // Volume of the object.
	path, // Path to the part file.
	partName, // Name of the part.
	metaBucket, // .minio
	tmpPath string, // Temporary file path where healed part will be written to.
	size int64, // Size of the part file.
	erasure erasureInfo) ([]checkSumInfo, error) {
	var offset int64
	remainingSize := size

	// Sha512 hash for bitrot protection.
	hashWriters := newHashWriters(len(outDatedDisks), bitRotAlgo)

	for remainingSize > 0 {
		curBlockSize := erasure.BlockSize
		if remainingSize < curBlockSize {
			curBlockSize = remainingSize
		}

		// Calculate the block size that needs to be read from each disk.
		curEncBlockSize := getChunkSize(curBlockSize, erasure.DataBlocks)

		// Memory for reading data from disks and reconstructing missing data using erasure coding.
		enBlocks := make([][]byte, len(latestDisks))

		// Read data from the latest disks.
		// FIXME: can be optimized to read from DataBlocks only if possible.
		for index, disk := range latestDisks {
			if disk == nil {
				continue
			}
			enBlocks[index] = make([]byte, curEncBlockSize)
			_, err := disk.ReadFile(volume, path, offset,
				enBlocks[index])
			if err != nil {
				enBlocks[index] = nil
			}
		}

		// Reconstruct missing data.
		err := decodeData(enBlocks, erasure.DataBlocks, erasure.ParityBlocks)
		if err != nil {
			return nil, err
		}

		// Write to the temporary heal file.
		for index, disk := range outDatedDisks {
			if disk == nil {
				continue
			}
			err := disk.AppendFile(metaBucket, tmpPath, enBlocks[index])
			if err != nil {
				return nil, err
			}
			hashWriters[index].Write(enBlocks[index])
		}
		remainingSize -= curBlockSize
		offset += curEncBlockSize
	}

	// Sha512 checksums for the healed parts.
	checkSums := make([]checkSumInfo, len(outDatedDisks))
	for index, disk := range outDatedDisks {
		if disk == nil {
			continue
		}
		checkSums[index] = checkSumInfo{
			Name:      partName,
			Algorithm: "sha512",
			Hash:      hex.EncodeToString(hashWriters[index].Sum(nil)),
		}
	}
	return checkSums, nil
}
