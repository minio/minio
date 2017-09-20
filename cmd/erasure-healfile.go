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
	"fmt"
	"hash"
	"strings"
)

// HealFile tries to reconstruct an erasure-coded file spread over all
// available disks. HealFile will read the valid parts of the file,
// reconstruct the missing data and write the reconstructed parts back
// to `staleDisks` at the destination `dstVol/dstPath/`. Parts are
// verified against the given BitrotAlgorithm and checksums.
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
// Errors when writing to `staleDisks` are not propagated as long as
// writes succeed for at least one disk. This allows partial healing
// despite stale disks being faulty.
//
// It returns bitrot checksums for the non-nil staleDisks on which
// healing succeeded.
func (s ErasureStorage) HealFile(staleDisks []StorageAPI, volume, path string, blocksize int64,
	dstVol, dstPath string, size int64, alg BitrotAlgorithm, checksums [][]byte) (
	f ErasureFileInfo, err error) {

	if !alg.Available() {
		return f, traceError(errBitrotHashAlgoInvalid)
	}

	// Initialization
	f.Checksums = make([][]byte, len(s.disks))
	hashers := make([]hash.Hash, len(s.disks))
	verifiers := make([]*BitrotVerifier, len(s.disks))
	for i, disk := range s.disks {
		switch {
		case staleDisks[i] != nil:
			hashers[i] = alg.New()
		case disk == nil:
			// disregard unavailable disk
			continue
		default:
			verifiers[i] = NewBitrotVerifier(alg, checksums[i])
		}
	}
	writeErrors := make([]error, len(s.disks))

	// Scan part files on disk, block-by-block reconstruct it and
	// write to stale disks.
	chunksize := getChunkSize(blocksize, s.dataBlocks)
	blocks := make([][]byte, len(s.disks))
	for i := range blocks {
		blocks[i] = make([]byte, chunksize)
	}
	var chunkOffset, blockOffset int64
	for ; blockOffset < size; blockOffset += blocksize {
		// last iteration may have less than blocksize data
		// left, so chunksize needs to be recomputed.
		if size < blockOffset+blocksize {
			blocksize = size - blockOffset
			chunksize = getChunkSize(blocksize, s.dataBlocks)
			for i := range blocks {
				blocks[i] = blocks[i][:chunksize]
			}
		}
		// read a chunk from each disk, until we have
		// `s.dataBlocks` number of chunks set to non-nil in
		// `blocks`
		numReads := 0
		for i, disk := range s.disks {
			// skip reading from unavailable or stale disks
			if disk == nil || staleDisks[i] != nil {
				blocks[i] = blocks[i][:0] // mark shard as missing
				continue
			}
			_, err = disk.ReadFile(volume, path, chunkOffset, blocks[i], verifiers[i])
			if err != nil {
				// LOG FIXME: add a conditional log
				// for read failures, once per-disk
				// per-function-invocation.
				blocks[i] = blocks[i][:0] // mark shard as missing
				continue
			}
			numReads++
			if numReads == s.dataBlocks {
				// we have enough data to reconstruct
				// mark all other blocks as missing
				for j := i + 1; j < len(blocks); j++ {
					blocks[j] = blocks[j][:0] // mark shard as missing
				}
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
		writeSucceeded := false
		for i, disk := range staleDisks {
			// skip nil disk or disk that had error on
			// previous write
			if disk == nil || writeErrors[i] != nil {
				continue
			}

			writeErrors[i] = disk.AppendFile(dstVol, dstPath, blocks[i])
			if writeErrors[i] == nil {
				hashers[i].Write(blocks[i])
				writeSucceeded = true
			}
		}

		// If all disks had write errors we quit.
		if !writeSucceeded {
			// build error from all write errors
			return f, traceError(joinWriteErrors(writeErrors))
		}
	}

	// copy computed file hashes into output variable
	f.Size = size
	f.Algorithm = alg
	for i, disk := range staleDisks {
		if disk == nil || writeErrors[i] != nil {
			continue
		}
		f.Checksums[i] = hashers[i].Sum(nil)
	}
	return f, nil
}

func joinWriteErrors(errs []error) error {
	msgs := []string{}
	for i, err := range errs {
		if err == nil {
			continue
		}
		msgs = append(msgs, fmt.Sprintf("disk %d: %v", i+1, err))
	}
	return fmt.Errorf("all stale disks had write errors during healing: %s",
		strings.Join(msgs, ", "))
}
