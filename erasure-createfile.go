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
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// erasureCreateFile - writes an entire stream by erasure coding to
// all the disks, writes also calculate individual block's checksum
// for future bit-rot protection.
func erasureCreateFile(disks []StorageAPI, volume string, path string, partName string, data io.Reader, eInfos []erasureInfo) (newEInfos []erasureInfo, size int64, err error) {
	// Allocated blockSized buffer for reading.
	buf := make([]byte, blockSizeV1)
	hashWriters := newHashWriters(len(disks))

	// Just pick one eInfo.
	eInfo := pickValidErasureInfo(eInfos)

	// Read until io.EOF, erasure codes data and writes to all disks.
	for {
		var n int
		n, err = io.ReadFull(data, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, 0, err
		}
		size += int64(n)
		var blocks [][]byte
		// Returns encoded blocks.
		blocks, err = encodeData(buf[:n], eInfo.DataBlocks, eInfo.ParityBlocks)
		if err != nil {
			return nil, 0, err
		}
		err = appendFile(disks, volume, path, blocks, eInfo.Distribution, hashWriters)
		if err != nil {
			return nil, 0, err
		}
	}

	// Save the checksums.
	checkSums := make([]checkSumInfo, len(disks))
	for index := range disks {
		blockIndex := eInfo.Distribution[index] - 1
		checkSums[blockIndex] = checkSumInfo{
			Name:      partName,
			Algorithm: "sha512",
			Hash:      hex.EncodeToString(hashWriters[blockIndex].Sum(nil)),
		}
	}

	// Erasure info update for checksum for each disks.
	newEInfos = make([]erasureInfo, len(disks))
	for index, eInfo := range eInfos {
		if eInfo.IsValid() {
			blockIndex := eInfo.Distribution[index] - 1
			newEInfos[index] = eInfo
			newEInfos[index].Checksum = append(newEInfos[index].Checksum, checkSums[blockIndex])
		}
	}

	// Return newEInfos.
	return newEInfos, size, nil
}

// encodeData - encodes incoming data buffer into
// dataBlocks+parityBlocks returns a 2 dimensional byte array.
func encodeData(dataBuffer []byte, dataBlocks, parityBlocks int) ([][]byte, error) {
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return nil, err
	}
	// Split the input buffer into data and parity blocks.
	var blocks [][]byte
	blocks, err = rs.Split(dataBuffer)
	if err != nil {
		return nil, err
	}

	// Encode parity blocks using data blocks.
	err = rs.Encode(blocks)
	if err != nil {
		return nil, err
	}

	// Return encoded blocks.
	return blocks, nil
}

// appendFile - append data buffer at path.
func appendFile(disks []StorageAPI, volume, path string, enBlocks [][]byte, distribution []int, hashWriters []hash.Hash) (err error) {
	var wg = &sync.WaitGroup{}
	var wErrs = make([]error, len(disks))
	// Write encoded data to quorum disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		// Write encoded data in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Pick the block from the distribution.
			blockIndex := distribution[index] - 1
			n, wErr := disk.AppendFile(volume, path, enBlocks[blockIndex])
			if wErr != nil {
				wErrs[index] = wErr
				return
			}
			if n != int64(len(enBlocks[blockIndex])) {
				wErrs[index] = errUnexpected
				return
			}

			// Calculate hash for each blocks.
			hashWriters[blockIndex].Write(enBlocks[blockIndex])

			// Successfully wrote.
			wErrs[index] = nil
		}(index, disk)
	}

	// Wait for all the appends to finish.
	wg.Wait()

	// Return success.
	return nil
}
