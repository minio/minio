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

	"github.com/minio/minio/pkg/bitrot"
	"github.com/minio/minio/pkg/bpool"
)

// ReadFile reads as much data as requested from the file under the given volume and path and writes the data to the provided writer.
// The algorithm and the keys/checksums are used to verify the integrity of the given file. ReadFile will read data from the given offset
// up to the given length. If parts of the file are corrupted ReadFile tries to reconstruct the data.
func (s XLStorage) ReadFile(writer io.Writer, volume, path string, offset, length int64, totalLength int64, key []byte, checksums [][]byte, algorithm bitrot.Algorithm, blocksize int64, pool *bpool.BytePool) (f ErasureFileInfo, err error) {
	if offset < 0 || length < 0 {
		return f, traceError(errUnexpected)
	}
	if offset+length > totalLength {
		return f, traceError(errUnexpected)
	}

	f.Checksums = make([][]byte, len(s.disks))
	verifiers := make([]*BitrotVerifier, len(s.disks))
	for i, disk := range s.disks {
		if disk != OfflineDisk {
			verifiers[i], err = NewBitrotVerifier(algorithm, key, checksums[i])
			if err != nil {
				return f, err
			}
		}
	}
	locks := make([]chan error, len(s.disks))
	for i := range locks {
		locks[i] = make(chan error, 1)
	}
	lastBlock := totalLength / blocksize
	startOffset := offset % blocksize
	chunksize := getChunkSize(blocksize, s.dataBlocks)

	blocks := make([][]byte, len(s.disks))
	for off := offset / blocksize; length > 0; off++ {
		blockOffset := off * chunksize
		pool.Reset()

		if currentBlock := (offset + f.Size) / blocksize; currentBlock == lastBlock {
			blocksize = totalLength % blocksize
			chunksize = getChunkSize(blocksize, s.dataBlocks)
		}
		err = s.readConcurrent(volume, path, blockOffset, chunksize, blocks, verifiers, locks, pool)
		if err != nil {
			return f, traceError(errXLReadQuorum)
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
		f.Size += int64(n)
		length -= int64(n)
	}

	f.Algorithm = algorithm
	f.Key = key
	for i, disk := range s.disks {
		if disk != OfflineDisk {
			f.Checksums[i] = verifiers[i].Sum(nil)
		}
	}
	return f, nil
}

func erasureCountMissingBlocks(blocks [][]byte, limit int) int {
	missing := 0
	for i := range blocks[:limit] {
		if blocks[i] == nil {
			missing++
		}
	}
	return missing
}

// readConcurrent reads all requested data concurrently from the disks into blocks. It returns an error if
// too many disks failed while reading.
func (s *XLStorage) readConcurrent(volume, path string, offset int64, length int64, blocks [][]byte, verifiers []*BitrotVerifier, locks []chan error, pool *bpool.BytePool) (err error) {
	errs := make([]error, len(s.disks))
	for i := range blocks {
		blocks[i], err = pool.Get()
		if err != nil {
			return Errorf("failed to get new buffer from pool: %v", err)
		}
		blocks[i] = blocks[i][:length]
	}

	erasureReadBlocksConcurrent(s.disks[:s.dataBlocks], volume, path, offset, blocks[:s.dataBlocks], verifiers[:s.dataBlocks], errs[:s.dataBlocks], locks[:s.dataBlocks])
	missingDataBlocks := erasureCountMissingBlocks(blocks, s.dataBlocks)
	mustReconstruct := missingDataBlocks > 0
	if mustReconstruct {
		requiredReads := s.dataBlocks + missingDataBlocks
		if requiredReads > s.dataBlocks+s.parityBlocks {
			return errXLReadQuorum
		}
		erasureReadBlocksConcurrent(s.disks[s.dataBlocks:requiredReads], volume, path, offset, blocks[s.dataBlocks:requiredReads], verifiers[s.dataBlocks:requiredReads], errs[s.dataBlocks:requiredReads], locks[s.dataBlocks:requiredReads])
		if erasureCountMissingBlocks(blocks, requiredReads) > 0 {
			erasureReadBlocksConcurrent(s.disks[requiredReads:], volume, path, offset, blocks[requiredReads:], verifiers[requiredReads:], errs[requiredReads:], locks[requiredReads:])
		}
	}
	if err = reduceReadQuorumErrs(errs, []error{}, s.readQuorum); err != nil {
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
func erasureReadBlocksConcurrent(disks []StorageAPI, volume, path string, offset int64, blocks [][]byte, verifiers []*BitrotVerifier, errors []error, locks []chan error) {
	for i := range locks {
		if disks[i] == OfflineDisk {
			locks[i] <- traceError(errDiskNotFound)
			continue
		}
		go erasureReadFromFile(disks[i], volume, path, offset, blocks[i], verifiers[i], locks[i])
	}
	for i := range locks {
		errors[i] = <-locks[i] // blocks until the go routine 'i' is done - no data race
		if errors[i] != nil {
			disks[i] = OfflineDisk
			blocks[i] = nil
		}
	}
}

// erasureReadReadFromFile reads data from the disk to buffer in parallel.
// It sends the returned error through the lock channel.
func erasureReadFromFile(disk StorageAPI, volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier, lock chan<- error) {
	var err error
	if !verifier.IsVerified() {
		_, err = disk.ReadFileWithVerify(volume, path, offset, buffer, verifier)
	} else {
		_, err = disk.ReadFile(volume, path, offset, buffer)
	}
	lock <- err
}
