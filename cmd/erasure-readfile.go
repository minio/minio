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
func (s XLStorage) ReadFile(writer io.Writer, volume, path string, offset, length int64, totalLength int64, keys, checksums [][]byte, algorithm bitrot.Algorithm, blocksize int64, pool *bpool.BytePool) (f ErasureFileInfo, err error) {
	if offset < 0 || length < 0 {
		return f, traceError(errUnexpected)
	}
	if offset+length > totalLength {
		return f, traceError(errUnexpected)
	}

	f.Keys, f.Checksums = make([][]byte, len(s)), make([][]byte, len(s))
	verifiers := make([]*BitrotVerifier, len(s))
	for i, disk := range s {
		if disk != OfflineDisk {
			verifiers[i], err = NewBitrotVerifier(algorithm, keys[i], checksums[i])
			if err != nil {
				return f, err
			}
		}
	}
	locks := make([]chan error, len(s))
	for i := range locks {
		locks[i] = make(chan error, 1)
	}
	lastBlock := totalLength / blocksize
	startOffset := offset % blocksize
	chunksize := getChunkSize(blocksize, len(s)/2)

	blocks := make([][]byte, len(s))
	for off := offset / blocksize; length > 0; off++ {
		blockOffset := off * chunksize
		pool.Reset()

		if currentBlock := (offset + f.Size) / blocksize; currentBlock == lastBlock {
			blocksize = totalLength % blocksize
			chunksize = getChunkSize(blocksize, len(s)/2)
		}
		err = s.readConcurrent(volume, path, blockOffset, chunksize, blocks, verifiers, locks, pool)
		if err != nil {
			return f, traceError(errXLReadQuorum)
		}

		writeLength := blocksize - startOffset
		if length < writeLength {
			writeLength = length
		}
		n, err := writeDataBlocks(writer, blocks, len(s)/2, startOffset, writeLength)
		if err != nil {
			return f, err
		}
		startOffset = 0
		f.Size += int64(n)
		length -= int64(n)
	}

	f.Algorithm = algorithm
	for i, disk := range s {
		if disk != OfflineDisk {
			f.Keys[i] = verifiers[i].key
			f.Checksums[i] = verifiers[i].Sum(nil)
		}
	}
	return f, nil
}

func erasureMustReconstruct(blocks [][]byte, datablocks int) bool {
	for _, block := range blocks[:datablocks] {
		if block == nil { // at least one shard is missing
			return true
		}
	}
	return false
}

func (s XLStorage) readConcurrent(volume, path string, offset int64, length int64, blocks [][]byte, verifiers []*BitrotVerifier, locks []chan error, pool *bpool.BytePool) (err error) {
	errs := make([]error, len(s))
	for i := range blocks {
		blocks[i], err = pool.Get()
		if err != nil {
			return Errorf("failed to get new buffer from pool: %v", err)
		}
		blocks[i] = blocks[i][:length]
	}
	for i := range locks {
		go erasureReadFromFile(s[i], volume, path, offset, blocks[i][:length], verifiers[i], locks[i])
	}
	for i := range locks {
		errs[i] = <-locks[i]
		if errs[i] != nil {
			s[i] = OfflineDisk
			blocks[i] = MissingShard
		}
	}
	if err = reduceReadQuorumErrs(errs, []error{}, len(s)/2); err != nil {
		return err
	}
	if erasureMustReconstruct(blocks, len(s)/2) {
		if err = s.ErasureDecode(blocks); err != nil {
			return err
		}
	}
	return nil
}

func erasureReadFromFile(disk StorageAPI, volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier, lock chan<- error) {
	if disk == OfflineDisk {
		lock <- traceError(errDiskNotFound)
		return
	}
	var err error
	if !verifier.IsVerified() {
		_, err = disk.ReadFileWithVerify(volume, path, offset, buffer, verifier)
	} else {
		_, err = disk.ReadFile(volume, path, offset, buffer)
	}
	lock <- err
}
