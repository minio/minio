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

// getDataBlocks - fetches the data block only part of the input encoded blocks.
func getDataBlocks(enBlocks [][]byte, dataBlocks int, curBlockSize int) (data []byte, err error) {
	if len(enBlocks) < dataBlocks {
		return nil, reedsolomon.ErrTooFewShards
	}
	size := 0
	blocks := enBlocks[:dataBlocks]
	for _, block := range blocks {
		size += len(block)
	}
	if size < curBlockSize {
		return nil, reedsolomon.ErrShortData
	}
	write := curBlockSize
	for _, block := range blocks {
		if write < len(block) {
			data = append(data, block[:write]...)
			return data, nil
		}
		data = append(data, block...)
		write -= len(block)
	}
	return data, nil
}

// checkBlockSize return the size of a single block.
// The first non-zero size is returned,
// or 0 if all blocks are size 0.
func checkBlockSize(blocks [][]byte) int {
	for _, block := range blocks {
		if len(block) != 0 {
			return len(block)
		}
	}
	return 0
}

// calculate the blockSize based on input length and total number of
// data blocks.
func getEncodedBlockLen(inputLen int64, dataBlocks int) (curEncBlockSize int64) {
	curEncBlockSize = (inputLen + int64(dataBlocks) - 1) / int64(dataBlocks)
	return curEncBlockSize
}
