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
)

func (xl XL) selfHeal(volume string, path string) error {
	totalBlocks := xl.DataBlocks + xl.ParityBlocks
	needsSelfHeal := make([]bool, totalBlocks)
	var readers = make([]io.Reader, totalBlocks)
	var writers = make([]io.WriteCloser, totalBlocks)

	// Acquire a read lock.
	readLock := true
	xl.lockNS(volume, path, readLock)
	defer xl.unlockNS(volume, path, readLock)

	quorumDisks, metadata, doSelfHeal, err := xl.getReadableDisks(volume, path)
	if err != nil {
		return err
	}
	if !doSelfHeal {
		return nil
	}

	size, err := metadata.GetSize()
	if err != nil {
		return err
	}

	for index, disk := range quorumDisks {
		if disk == nil {
			needsSelfHeal[index] = true
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
	atleastOneSelfHeal := false
	for _, shNeeded := range needsSelfHeal {
		if shNeeded {
			atleastOneSelfHeal = true
			break
		}
	}
	if !atleastOneSelfHeal {
		// Return if healing not needed anywhere.
		return nil
	}

	// create writers for parts where healing is needed.
	for index, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		writers[index], err = xl.storageDisks[index].CreateFile(volume, erasurePart)
		if err != nil {
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
			if needsSelfHeal[index] {
				// Skip reading if the part needs healing.
				continue
			}
			_, e := io.ReadFull(reader, enBlocks[index])
			if e != nil && e != io.ErrUnexpectedEOF {
				enBlocks[index] = nil
			}
		}

		// Check blocks if they are all zero in length.
		if checkBlockSize(enBlocks) == 0 {
			err = errors.New("Data likely corrupted, all blocks are zero in length.")
			return err
		}

		// Verify the blocks.
		ok, e := xl.ReedSolomon.Verify(enBlocks)
		if e != nil {
			closeAndRemoveWriters(writers...)
			return e
		}

		// Verification failed, blocks require reconstruction.
		if !ok {
			for index, shNeeded := range needsSelfHeal {
				if shNeeded {
					// Reconstructs() reconstructs the parts if the array is nil.
					enBlocks[index] = nil
				}
			}
			e = xl.ReedSolomon.Reconstruct(enBlocks)
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
			// Verify reconstructed blocks again.
			ok, e = xl.ReedSolomon.Verify(enBlocks)
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
			if !ok {
				// Blocks cannot be reconstructed, corrupted data.
				e = errors.New("Verification failed after reconstruction, data likely corrupted.")
				closeAndRemoveWriters(writers...)
				return e
			}
		}
		for index, shNeeded := range needsSelfHeal {
			if !shNeeded {
				continue
			}
			_, e := writers[index].Write(enBlocks[index])
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
		}
		totalLeft = totalLeft - erasureBlockSize
	}

	// After successful healing Close() the writer so that the temp
	// files are committed to their location.
	for index, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		writers[index].Close()
	}

	// Update the quorum metadata after selfheal.
	errs := xl.setPartsMetadata(volume, path, metadata, needsSelfHeal)
	for index, shNeeded := range needsSelfHeal {
		if shNeeded && errs[index] != nil {
			return errs[index]
		}
	}
	return nil
}
