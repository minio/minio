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

package cmd

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
func erasureCreateFile(disks []StorageAPI, volume, path string, reader io.Reader, blockSize int64, dataBlocks int, parityBlocks int, algo string, writeQuorum int) (bytesWritten int64, checkSums []string, err error) {
	// Allocated blockSized buffer for reading.
	buf := make([]byte, blockSize)

	hashWriters := newHashWriters(len(disks), algo)

	// Read until io.EOF, erasure codes data and writes to all disks.
	for {
		var blocks [][]byte
		n, rErr := io.ReadFull(reader, buf)
		// FIXME: this is a bug in Golang, n == 0 and err ==
		// io.ErrUnexpectedEOF for io.ReadFull function.
		if n == 0 && rErr == io.ErrUnexpectedEOF {
			return 0, nil, traceError(rErr)
		}
		if rErr == io.EOF {
			// We have reached EOF on the first byte read, io.Reader
			// must be 0bytes, we don't need to erasure code
			// data. Will create a 0byte file instead.
			if bytesWritten == 0 {
				blocks = make([][]byte, len(disks))
				rErr = appendFile(disks, volume, path, blocks, hashWriters, writeQuorum)
				if rErr != nil {
					return 0, nil, rErr
				}
			} // else we have reached EOF after few reads, no need to
			// add an additional 0bytes at the end.
			break
		}
		if rErr != nil && rErr != io.ErrUnexpectedEOF {
			return 0, nil, traceError(rErr)
		}
		if n > 0 {
			// Returns encoded blocks.
			var enErr error
			blocks, enErr = encodeData(buf[0:n], dataBlocks, parityBlocks)
			if enErr != nil {
				return 0, nil, enErr
			}

			// Write to all disks.
			if err = appendFile(disks, volume, path, blocks, hashWriters, writeQuorum); err != nil {
				return 0, nil, err
			}
			bytesWritten += int64(n)
		}
	}

	checkSums = make([]string, len(disks))
	for i := range checkSums {
		checkSums[i] = hex.EncodeToString(hashWriters[i].Sum(nil))
	}
	return bytesWritten, checkSums, nil
}

// encodeData - encodes incoming data buffer into
// dataBlocks+parityBlocks returns a 2 dimensional byte array.
func encodeData(dataBuffer []byte, dataBlocks, parityBlocks int) ([][]byte, error) {
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return nil, traceError(err)
	}
	// Split the input buffer into data and parity blocks.
	var blocks [][]byte
	blocks, err = rs.Split(dataBuffer)
	if err != nil {
		return nil, traceError(err)
	}

	// Encode parity blocks using data blocks.
	err = rs.Encode(blocks)
	if err != nil {
		return nil, traceError(err)
	}

	// Return encoded blocks.
	return blocks, nil
}

// appendFile - append data buffer at path.
func appendFile(disks []StorageAPI, volume, path string, enBlocks [][]byte, hashWriters []hash.Hash, writeQuorum int) (err error) {
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
			wErr := disk.AppendFile(volume, path, enBlocks[index])
			if wErr != nil {
				wErrs[index] = traceError(wErr)
				return
			}

			// Calculate hash for each blocks.
			hashWriters[index].Write(enBlocks[index])

			// Successfully wrote.
			wErrs[index] = nil
		}(index, disk)
	}

	// Wait for all the appends to finish.
	wg.Wait()

	// Do we have write quorum?.
	if !isDiskQuorum(wErrs, writeQuorum) {
		return traceError(errXLWriteQuorum)
	}
	return reduceWriteQuorumErrs(wErrs, objectOpIgnoredErrs, writeQuorum)
}
