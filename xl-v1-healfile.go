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

// doSelfHeal - heals the file at path.
func (xl XL) doHealFile(volume string, path string) error {
	totalBlocks := xl.DataBlocks + xl.ParityBlocks
	needsHeal := make([]bool, totalBlocks)
	var readers = make([]io.Reader, totalBlocks)
	var writers = make([]io.WriteCloser, totalBlocks)

	// Acquire a read lock.
	readLock := true
	xl.lockNS(volume, path, readLock)
	defer xl.unlockNS(volume, path, readLock)

	quorumDisks, metadata, doHeal, err := xl.getReadableDisks(volume, path)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("Get readable disks failed with %s", err)
		return err
	}
	if !doHeal {
		return nil
	}

	size, err := metadata.GetSize()
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("Failed to get file size, %s", err)
		return err
	}

	for index, disk := range quorumDisks {
		if disk == nil {
			needsHeal[index] = true
			continue
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		// If disk.ReadFile returns error and we don't have read quorum it will be taken care as
		// ReedSolomon.Reconstruct() will fail later.
		var reader io.ReadCloser
		offset := int64(0)
		if reader, err = xl.storageDisks[index].ReadFile(volume, erasurePart, offset); err == nil {
			readers[index] = reader
			defer reader.Close()
		}
	}

	// Check if there is atleast one part that needs to be healed.
	atleastOneHeal := false
	for _, healNeeded := range needsHeal {
		if healNeeded {
			atleastOneHeal = true
			break
		}
	}
	if !atleastOneHeal {
		// Return if healing not needed anywhere.
		return nil
	}

	// create writers for parts where healing is needed.
	for index, healNeeded := range needsHeal {
		if !healNeeded {
			continue
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		writers[index], err = xl.storageDisks[index].CreateFile(volume, erasurePart)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Debugf("CreateFile failed with error %s", err)
			// Unexpected error
			closeAndRemoveWriters(writers...)
			return err
		}
	}
	var totalLeft = size
	for totalLeft > 0 {
		// Figure out the right blockSize.
		var curBlockSize int
		if erasureBlockSize < totalLeft {
			curBlockSize = erasureBlockSize
		} else {
			curBlockSize = int(totalLeft)
		}
		// Calculate the current block size.
		curBlockSize = getEncodedBlockLen(curBlockSize, xl.DataBlocks)
		enBlocks := make([][]byte, totalBlocks)
		// Loop through all readers and read.
		for index, reader := range readers {
			// Initialize block slice and fill the data from each parts.
			// ReedSolomon.Verify() expects that slice is not nil even if the particular
			// part needs healing.
			enBlocks[index] = make([]byte, curBlockSize)
			if needsHeal[index] {
				// Skip reading if the part needs healing.
				continue
			}
			_, err = io.ReadFull(reader, enBlocks[index])
			if err != nil && err != io.ErrUnexpectedEOF {
				enBlocks[index] = nil
			}
		}

		// Check blocks if they are all zero in length.
		if checkBlockSize(enBlocks) == 0 {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Debugf("%s", errDataCorrupt)
			return errDataCorrupt
		}

		// Verify the blocks.
		ok, err := xl.ReedSolomon.Verify(enBlocks)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Debugf("ReedSolomon verify failed with %s", err)
			closeAndRemoveWriters(writers...)
			return err
		}

		// Verification failed, blocks require reconstruction.
		if !ok {
			for index, healNeeded := range needsHeal {
				if healNeeded {
					// Reconstructs() reconstructs the parts if the array is nil.
					enBlocks[index] = nil
				}
			}
			err = xl.ReedSolomon.Reconstruct(enBlocks)
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("ReedSolomon reconstruct failed with %s", err)
				closeAndRemoveWriters(writers...)
				return err
			}
			// Verify reconstructed blocks again.
			ok, err = xl.ReedSolomon.Verify(enBlocks)
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("ReedSolomon verify failed with %s", err)
				closeAndRemoveWriters(writers...)
				return err
			}
			if !ok {
				// Blocks cannot be reconstructed, corrupted data.
				err = errors.New("Verification failed after reconstruction, data likely corrupted.")
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("%s", err)
				closeAndRemoveWriters(writers...)
				return err
			}
		}
		for index, healNeeded := range needsHeal {
			if !healNeeded {
				continue
			}
			_, err := writers[index].Write(enBlocks[index])
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Debugf("Write failed with %s", err)
				closeAndRemoveWriters(writers...)
				return err
			}
		}
		totalLeft = totalLeft - erasureBlockSize
	}

	// After successful healing Close() the writer so that the temp
	// files are committed to their location.
	for _, writer := range writers {
		if writer == nil {
			continue
		}
		writer.Close()
	}

	// Update the quorum metadata after selfheal.
	errs := xl.setPartsMetadata(volume, path, metadata, needsHeal)
	for index, healNeeded := range needsHeal {
		if healNeeded && errs[index] != nil {
			return errs[index]
		}
	}
	return nil
}
