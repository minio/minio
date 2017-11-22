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

// ReadFile reads as much data as requested from the file under the given volume and path and writes the data to the provided writer.
// The algorithm and the keys/checksums are used to verify the integrity of the given file. ReadFile will read data from the given offset
// up to the given length. If parts of the file are corrupted ReadFile tries to reconstruct the data.
func (s ErasureStorage) ReadFile(writer io.Writer, volume, path string, offset, length int64, totalLength int64, checksums [][]byte, algorithm BitrotAlgorithm, blocksize int64) (f ErasureFileInfo, err error) {
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
	errChans := make([]chan error, len(s.disks))
	for i := range errChans {
		errChans[i] = make(chan error, 1)
	}
	lastBlock := totalLength / blocksize
	startOffset := offset % blocksize
	chunksize := getChunkSize(blocksize, s.dataBlocks)

	blocks := make([][]byte, len(s.disks))
	for i := range blocks {
		blocks[i] = make([]byte, chunksize)
	}
	for off := offset / blocksize; length > 0; off++ {
		blockOffset := off * chunksize

		if currentBlock := (offset + f.Size) / blocksize; currentBlock == lastBlock {
			blocksize = totalLength % blocksize
			chunksize = getChunkSize(blocksize, s.dataBlocks)
			for i := range blocks {
				blocks[i] = blocks[i][:chunksize]
			}
		}
		err = s.readConcurrent(volume, path, blockOffset, blocks, verifiers, errChans)
		if err != nil {
			return f, errors.Trace(errXLReadQuorum)
		}

		writeLength := blocksize - startOffset
		if length < writeLength {
			writeLength = length
		}
		n, err := writeDataBlocks(writer, blocks, s.dataBlocks, startOffset, writeLength)
		if err != nil {
			return f, err
		}
		startOffset = 0
		f.Size += n
		length -= n
	}

	f.Algorithm = algorithm
	for i, disk := range s.disks {
		if disk == OfflineDisk {
			continue
		}
		f.Checksums[i] = verifiers[i].Sum(nil)
	}
	return f, nil
}

func erasureCountMissingBlocks(blocks [][]byte, limit int) int {
	missing := 0
	for i := range blocks[:limit] {
		if len(blocks[i]) == 0 {
			missing++
		}
	}
	return missing
}

// readConcurrent reads all requested data concurrently from the disks into blocks. It returns an error if
// too many disks failed while reading.
func (s *ErasureStorage) readConcurrent(volume, path string, offset int64, blocks [][]byte, verifiers []*BitrotVerifier, errChans []chan error) (err error) {
	errs := make([]error, len(s.disks))

	erasureReadBlocksConcurrent(s.disks[:s.dataBlocks], volume, path, offset, blocks[:s.dataBlocks], verifiers[:s.dataBlocks], errs[:s.dataBlocks], errChans[:s.dataBlocks])
	missingDataBlocks := erasureCountMissingBlocks(blocks, s.dataBlocks)
	mustReconstruct := missingDataBlocks > 0
	if mustReconstruct {
		requiredReads := s.dataBlocks + missingDataBlocks
		if requiredReads > s.dataBlocks+s.parityBlocks {
			return errXLReadQuorum
		}
		erasureReadBlocksConcurrent(s.disks[s.dataBlocks:requiredReads], volume, path, offset, blocks[s.dataBlocks:requiredReads], verifiers[s.dataBlocks:requiredReads], errs[s.dataBlocks:requiredReads], errChans[s.dataBlocks:requiredReads])
		if erasureCountMissingBlocks(blocks, requiredReads) > 0 {
			erasureReadBlocksConcurrent(s.disks[requiredReads:], volume, path, offset, blocks[requiredReads:], verifiers[requiredReads:], errs[requiredReads:], errChans[requiredReads:])
		}
	}
	if err = reduceReadQuorumErrs(errs, []error{}, s.dataBlocks); err != nil {
		return err
	}
	if mustReconstruct {
		if err = s.ErasureDecodeDataBlocks(blocks); err != nil {
			return err
		}
	}
	return nil
}

// erasureReadBlocksConcurrent reads all data from each disk to each data block in parallel.
// Therefore disks, blocks, verifiers errors and locks must have the same length.
func erasureReadBlocksConcurrent(disks []StorageAPI, volume, path string, offset int64, blocks [][]byte, verifiers []*BitrotVerifier, errors []error, errChans []chan error) {
	for i := range errChans {
		go erasureReadFromFile(disks[i], volume, path, offset, blocks[i], verifiers[i], errChans[i])
	}
	for i := range errChans {
		errors[i] = <-errChans[i] // blocks until the go routine 'i' is done - no data race
		if errors[i] != nil {
			disks[i] = OfflineDisk
			blocks[i] = blocks[i][:0] // mark shard as missing
		}
	}
}

// erasureReadFromFile reads data from the disk to buffer in parallel.
// It sends the returned error through the error channel.
func erasureReadFromFile(disk StorageAPI, volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier, errChan chan<- error) {
	if disk == OfflineDisk {
		errChan <- errors.Trace(errDiskNotFound)
		return
	}
	_, err := disk.ReadFile(volume, path, offset, buffer, verifier)
	errChan <- err
}
