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
	"hash"
)

// HealFile tries to reconstruct an erasure-coded file spread over all
// available disks. HealFile will read the valid parts of the file,
// reconstruct the missing data and write the reconstructed parts back
// to `staleDisks`.
//
// `staleDisks` is a slice of disks where each non-nil entry has stale
// or no data, and so will be healed.
//
// It is required that `s.disks` have a (read-quorum) majority of
// disks with valid data for healing to work.
//
// In addition, `staleDisks` and `s.disks` must have the same ordering
// of disks w.r.t. erasure coding of the object.
//
// The function will try to read the valid parts from the file under
// the given volume and path and tries to reconstruct the file under
// the given healVolume and healPath (on staleDisks). The given
// algorithm will be used to verify the valid parts and to protect the
// reconstructed file.
//
// It returns bitrot checksums for the non-nil staleDisks.
func (s ErasureStorage) HealFile(staleDisks []StorageAPI, volume, path string,
	blocksize int64, healVolume, healPath string, size int64,
	algorithm BitrotAlgorithm, checksums [][]byte) (f ErasureFileInfo,
	err error) {

	if !algorithm.Available() {
		return f, traceError(errBitrotHashAlgoInvalid)
	}

	// Initialization
	f.Checksums = make([][]byte, len(s.disks))
	hashers := make([]hash.Hash, len(s.disks))
	verifiers := make([]*BitrotVerifier, len(s.disks))
	for i, disk := range s.disks {
		switch {
		case staleDisks[i] != nil:
			hashers[i] = algorithm.New()
		case disk == nil:
			// disregard unavailable disk
			continue
		default:
			verifiers[i] = NewBitrotVerifier(algorithm, checksums[i])
			f.Checksums[i] = checksums[i]
		}
	}

	// Scan part files on disk, block-by-block reconstruct it and
	// write to stale disks.
	chunksize := getChunkSize(blocksize, s.dataBlocks)
	var chunkOffset, blockOffset int64
	for ; blockOffset < size; blockOffset += blocksize {
		// last iteration may have less than blocksize data
		// left, so chunksize needs to be recomputed.
		if size < blockOffset+blocksize {
			blocksize = size - blockOffset
			chunksize = getChunkSize(blocksize, s.dataBlocks)
		}

		// read a chunk from each disk, until we have
		// `s.dataBlocks` number of chunks set to non-nil in
		// `blocks`
		blocks := make([][]byte, len(s.disks))
		var buffer []byte
		numReads := 0
		for i, disk := range s.disks {
			// skip reading from unavailable or stale disks
			if disk == nil || staleDisks[i] != nil {
				continue
			}
			// allocate buffer only when needed - when
			// reads fail, the buffer can be reused
			if int64(len(buffer)) != chunksize {
				buffer = make([]byte, chunksize)
			}
			if !verifiers[i].IsVerified() {
				_, err = disk.ReadFileWithVerify(volume, path,
					chunkOffset, buffer, verifiers[i])
			} else {
				_, err = disk.ReadFile(volume, path,
					chunkOffset, buffer)
			}
			if err != nil {
				// LOG FIXME: add a conditional log
				// for read failures, once per-disk
				// per-function-invocation.
				continue
			}

			// read was successful, so set the buffer as
			// blocks[i], and reset buffer to nil to force
			// allocation on next iteration
			blocks[i], buffer = buffer, nil

			numReads++
			if numReads == s.dataBlocks {
				// we have enough data to reconstruct
				break
			}
		}

		// advance the chunk offset to prepare for next loop
		// iteration
		chunkOffset += chunksize

		// reconstruct data - this computes all data and parity shards
		if err = s.ErasureDecodeDataAndParityBlocks(blocks); err != nil {
			return f, err
		}

		// write computed shards as chunks on file in each
		// stale disk
		for i, disk := range staleDisks {
			if disk == nil {
				continue
			}

			err = disk.AppendFile(healVolume, healPath, blocks[i])
			if err != nil {
				return f, traceError(err)
			}
			hashers[i].Write(blocks[i])
		}
	}

	// copy computed file hashes into output variable
	f.Size = size
	f.Algorithm = algorithm
	for i, disk := range staleDisks {
		if disk == nil {
			continue
		}
		f.Checksums[i] = hashers[i].Sum(nil)
	}
	return f, nil
}
