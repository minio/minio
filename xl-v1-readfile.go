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
	"errors"
	"fmt"
	"io"
	slashpath "path"

	"github.com/Sirupsen/logrus"
)

// ReadFile - read file
func (xl XL) ReadFile(volume, path string, offset int64) (io.ReadCloser, error) {
	// Input validation.
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	// Acquire a read lock.
	readLock := true
	xl.lockNS(volume, path, readLock)
	quorumDisks, metadata, doSelfHeal, err := xl.getReadableDisks(volume, path)
	xl.unlockNS(volume, path, readLock)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("Get readable disks failed with %s", err)
		return nil, err
	}

	if doSelfHeal {
		if err = xl.doHealFile(volume, path); err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Debugf("doHealFile failed with %s", err)
			return nil, err
		}
	}

	// Acquire read lock again.
	xl.lockNS(volume, path, readLock)
	defer xl.unlockNS(volume, path, readLock)

	fileSize, err := metadata.GetSize()
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("Failed to get file size, %s", err)
		return nil, err
	}

	readers := make([]io.ReadCloser, len(xl.storageDisks))
	for index, disk := range quorumDisks {
		if disk == nil {
			continue
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		// If disk.ReadFile returns error and we don't have read quorum it will be taken care as
		// ReedSolomon.Reconstruct() will fail later.
		var reader io.ReadCloser
		if reader, err = disk.ReadFile(volume, erasurePart, offset); err == nil {
			readers[index] = reader
		}
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = fileSize
		// Read until the totalLeft.
		for totalLeft > 0 {
			// Figure out the right blockSize as it was encoded before.
			var curBlockSize int
			if erasureBlockSize < totalLeft {
				curBlockSize = erasureBlockSize
			} else {
				curBlockSize = int(totalLeft)
			}
			// Calculate the current encoded block size.
			curEncBlockSize := getEncodedBlockLen(curBlockSize, xl.DataBlocks)
			enBlocks := make([][]byte, len(xl.storageDisks))
			// Loop through all readers and read.
			for index, reader := range readers {
				// Initialize shard slice and fill the data from each parts.
				enBlocks[index] = make([]byte, curEncBlockSize)
				if reader == nil {
					continue
				}
				_, err = io.ReadFull(reader, enBlocks[index])
				if err != nil && err != io.ErrUnexpectedEOF {
					readers[index] = nil
				}
			}

			// TODO need to verify block512Sum.

			// Check blocks if they are all zero in length.
			if checkBlockSize(enBlocks) == 0 {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("%s", errDataCorrupt)
				pipeWriter.CloseWithError(errDataCorrupt)
				return
			}

			// Verify the blocks.
			var ok bool
			ok, err = xl.ReedSolomon.Verify(enBlocks)
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("ReedSolomon verify failed with %s", err)
				pipeWriter.CloseWithError(err)
				return
			}

			// Verification failed, blocks require reconstruction.
			if !ok {
				for index, reader := range readers {
					if reader == nil {
						// Reconstruct expects missing blocks to be nil.
						enBlocks[index] = nil
					}
				}
				err = xl.ReedSolomon.Reconstruct(enBlocks)
				if err != nil {
					log.WithFields(logrus.Fields{
						"volume": volume,
						"path":   path,
					}).Debugf("ReedSolomon reconstruct failed with %s", err)
					pipeWriter.CloseWithError(err)
					return
				}
				// Verify reconstructed blocks again.
				ok, err = xl.ReedSolomon.Verify(enBlocks)
				if err != nil {
					log.WithFields(logrus.Fields{
						"volume": volume,
						"path":   path,
					}).Debugf("ReedSolomon verify failed with %s", err)
					pipeWriter.CloseWithError(err)
					return
				}
				if !ok {
					// Blocks cannot be reconstructed, corrupted data.
					err = errors.New("Verification failed after reconstruction, data likely corrupted.")
					log.WithFields(logrus.Fields{
						"volume": volume,
						"path":   path,
					}).Debugf("%s", err)
					pipeWriter.CloseWithError(err)
					return
				}
			}

			// Join the decoded blocks.
			err = xl.ReedSolomon.Join(pipeWriter, enBlocks, curBlockSize)
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("ReedSolomon joining decoded blocks failed with %s", err)
				pipeWriter.CloseWithError(err)
				return
			}

			// Save what's left after reading erasureBlockSize.
			totalLeft = totalLeft - erasureBlockSize
		}

		// Cleanly end the pipe after a successful decoding.
		pipeWriter.Close()

		// Cleanly close all the underlying data readers.
		for _, reader := range readers {
			if reader == nil {
				continue
			}
			reader.Close()
		}
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}
