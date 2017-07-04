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

import "hash"

// HealFile tries to reconstruct a bitrot encoded file spread over all available disks. HealFile will read the valid parts of the file,
// reconstruct the missing data and write the reconstructed parts back to the disks.
// It will try to read the valid parts from the file under the given volume and path and tries to reconstruct the file under the given
// healVolume and healPath. The given algorithm will be used to verify the valid parts and to protect the reconstructed file.
func (s ErasureStorage) HealFile(offlineDisks []StorageAPI, volume, path string, blocksize int64, healVolume, healPath string, size int64, algorithm BitrotAlgorithm, checksums [][]byte) (f ErasureFileInfo, err error) {
	if !algorithm.Available() {
		return f, traceError(errBitrotHashAlgoInvalid)
	}
	f.Checksums = make([][]byte, len(s.disks))
	hashers, verifiers := make([]hash.Hash, len(s.disks)), make([]*BitrotVerifier, len(s.disks))
	for i, disk := range s.disks {
		if disk == OfflineDisk {
			hashers[i] = algorithm.New()
		} else {
			verifiers[i] = NewBitrotVerifier(algorithm, checksums[i])
			f.Checksums[i] = checksums[i]
		}
	}
	blocks := make([][]byte, len(s.disks))
	chunksize := getChunkSize(blocksize, s.dataBlocks)
	for offset := int64(0); offset < size; offset += blocksize {
		if size < blocksize {
			blocksize = size
			chunksize = getChunkSize(blocksize, s.dataBlocks)
		}
		numReads := 0
		for i, disk := range s.disks {
			if disk != OfflineDisk {
				if blocks[i] == nil {
					blocks[i] = make([]byte, chunksize)
				}
				blocks[i] = blocks[i][:chunksize]
				if !verifiers[i].IsVerified() {
					_, err = disk.ReadFileWithVerify(volume, path, offset, blocks[i], verifiers[i])
				} else {
					_, err = disk.ReadFile(volume, path, offset, blocks[i])
				}
				if err != nil {
					blocks[i] = nil
				} else {
					numReads++
				}
				if numReads == s.dataBlocks { // we have enough data to reconstruct
					break
				}
			}
		}
		if err = s.ErasureDecodeDataAndParityBlocks(blocks); err != nil {
			return f, err
		}
		for i, disk := range s.disks {
			if disk != OfflineDisk {
				continue
			}
			if err = offlineDisks[i].AppendFile(healVolume, healPath, blocks[i]); err != nil {
				return f, traceError(err)
			}
			hashers[i].Write(blocks[i])
		}
	}
	f.Size = size
	f.Algorithm = algorithm
	for i, disk := range s.disks {
		if disk != OfflineDisk {
			continue
		}
		f.Checksums[i] = hashers[i].Sum(nil)
	}
	return f, nil
}
