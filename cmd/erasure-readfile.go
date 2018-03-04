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
	"io"

	"github.com/minio/minio/pkg/errors"
)

type errIdx struct {
	idx int
	err error
}

func (s ErasureStorage) readConcurrent(volume, path string, offset, length int64,
	verifiers []*BitrotVerifier) (buffers [][]byte, needsReconstruction bool,
	err error) {

	errChan := make(chan errIdx)
	stageBuffers := make([][]byte, len(s.disks))
	buffers = make([][]byte, len(s.disks))

	readDisk := func(i int) {
		stageBuffers[i] = make([]byte, length)
		disk := s.disks[i]
		if disk == OfflineDisk {
			errChan <- errIdx{i, errors.Trace(errDiskNotFound)}
			return
		}
		_, rerr := disk.ReadFile(volume, path, offset, stageBuffers[i], verifiers[i])
		errChan <- errIdx{i, rerr}
	}

	var finishedCount, successCount, launchIndex int

	for ; launchIndex < s.dataBlocks; launchIndex++ {
		go readDisk(launchIndex)
	}
	for finishedCount < launchIndex {
		select {
		case errVal := <-errChan:
			finishedCount++
			if errVal.err != nil {
				// TODO: meaningfully log the disk read error

				// A disk failed to return data, so we
				// request an additional disk if possible
				if launchIndex < s.dataBlocks+s.parityBlocks {
					needsReconstruction = true
					// requiredBlocks++
					go readDisk(launchIndex)
					launchIndex++
				}
			} else {
				successCount++
				buffers[errVal.idx] = stageBuffers[errVal.idx]
				stageBuffers[errVal.idx] = nil
			}
		}
	}
	if successCount != s.dataBlocks {
		// Not enough disks returns data.
		err = errors.Trace(errXLReadQuorum)
	}
	return
}

// ReadFile reads as much data as requested from the file under the
// given volume and path and writes the data to the provided writer.
// The algorithm and the keys/checksums are used to verify the
// integrity of the given file. ReadFile will read data from the given
// offset up to the given length. If parts of the file are corrupted
// ReadFile tries to reconstruct the data.
func (s ErasureStorage) ReadFile(writer io.Writer, volume, path string, offset,
	length, totalLength int64, checksums [][]byte, algorithm BitrotAlgorithm,
	blocksize int64) (f ErasureFileInfo, err error) {

	if offset < 0 || length < 0 {
		return f, errors.Trace(errUnexpected)
	}
	if offset+length > totalLength {
		return f, errors.Trace(errUnexpected)
	}
	if !algorithm.Available() {
		return f, errors.Trace(errBitrotHashAlgoInvalid)
	}

	f.Checksums = make([][]byte, len(s.disks))
	verifiers := make([]*BitrotVerifier, len(s.disks))
	for i, disk := range s.disks {
		if disk == OfflineDisk {
			continue
		}
		verifiers[i] = NewBitrotVerifier(algorithm, checksums[i])
	}

	chunksize := ceilFrac(blocksize, int64(s.dataBlocks))

	// We read all whole-blocks of erasure coded data containing
	// the requested data range.
	//
	// The start index of the erasure coded block containing the
	// `offset` byte of data is:
	partDataStartIndex := (offset / blocksize) * chunksize
	// The start index of the erasure coded block containing the
	// (last) byte of data at the index `offset + length - 1` is:
	blockStartIndex := ((offset + length - 1) / blocksize) * chunksize
	// However, we need the end index of the e.c. block containing
	// the last byte - we need to check if that block is the last
	// block in the part (in that case, it may be have a different
	// chunk size)
	isLastBlock := (totalLength-1)/blocksize == (offset+length-1)/blocksize
	var partDataEndIndex int64
	if isLastBlock {
		lastBlockChunkSize := chunksize
		if totalLength%blocksize != 0 {
			lastBlockChunkSize = ceilFrac(totalLength%blocksize, int64(s.dataBlocks))
		}
		partDataEndIndex = blockStartIndex + lastBlockChunkSize - 1
	} else {
		partDataEndIndex = blockStartIndex + chunksize - 1
	}

	// Thus, the length of data to be read from the part file(s) is:
	partDataLength := partDataEndIndex - partDataStartIndex + 1
	// The calculation above does not apply when length == 0:
	if length == 0 {
		partDataLength = 0
	}

	var buffers [][]byte
	var needsReconstruction bool
	buffers, needsReconstruction, err = s.readConcurrent(volume, path,
		partDataStartIndex, partDataLength, verifiers)
	if err != nil {
		// Could not read enough disks.
		return
	}

	numChunks := ceilFrac(partDataLength, chunksize)
	blocks := make([][]byte, len(s.disks))

	if needsReconstruction && numChunks > 1 {
		// Allocate once for all the equal length blocks. The
		// last block may have a different length - allocation
		// for this happens inside the for loop below.
		for i := range blocks {
			if len(buffers[i]) == 0 {
				blocks[i] = make([]byte, chunksize)
			}
		}
	}

	var buffOffset int64
	for chunkNumber := int64(0); chunkNumber < numChunks; chunkNumber++ {
		if chunkNumber == numChunks-1 && partDataLength%chunksize != 0 {
			chunksize = partDataLength % chunksize
			// We allocate again as the last chunk has a
			// different size.
			for i := range blocks {
				if len(buffers[i]) == 0 {
					blocks[i] = make([]byte, chunksize)
				}
			}
		}

		for i := range blocks {
			if len(buffers[i]) == 0 {
				blocks[i] = blocks[i][0:0]
			}
		}

		for i := range blocks {
			if len(buffers[i]) != 0 {
				blocks[i] = buffers[i][buffOffset : buffOffset+chunksize]
			}
		}
		buffOffset += chunksize

		if needsReconstruction {
			if err = s.ErasureDecodeDataBlocks(blocks); err != nil {
				return f, errors.Trace(err)
			}
		}

		var writeStart int64
		if chunkNumber == 0 {
			writeStart = offset % blocksize
		}

		writeLength := blocksize - writeStart
		if chunkNumber == numChunks-1 {
			lastBlockLength := (offset + length) % blocksize
			if lastBlockLength != 0 {
				writeLength = lastBlockLength - writeStart
			}
		}
		n, err := writeDataBlocks(writer, blocks, s.dataBlocks, writeStart, writeLength)
		if err != nil {
			return f, err
		}

		f.Size += n
	}

	f.Algorithm = algorithm
	for i, disk := range s.disks {
		if disk == OfflineDisk || buffers[i] == nil {
			continue
		}
		f.Checksums[i] = verifiers[i].Sum(nil)
	}
	return f, nil
}
