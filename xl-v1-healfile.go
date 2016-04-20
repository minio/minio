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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	slashpath "path"
	"strconv"
)

func (xl XL) selfHeal(volume string, path string) error {
	totalShards := xl.DataBlocks + xl.ParityBlocks
	needsSelfHeal := make([]bool, totalShards)
	var metadata = make(map[string]string)
	var readers = make([]io.Reader, totalShards)
	var writers = make([]io.WriteCloser, totalShards)
	for index, disk := range xl.storageDisks {
		metadataFile := slashpath.Join(path, metadataFile)

		// Start from the beginning, we are not reading partial metadata files.
		offset := int64(0)

		metadataReader, err := disk.ReadFile(volume, metadataFile, offset)
		if err != nil {
			if err != errFileNotFound {
				continue
			}
			// Needs healing if part.json is not found
			needsSelfHeal[index] = true
			continue
		}
		defer metadataReader.Close()

		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
			// needs healing if parts.json is not parsable
			needsSelfHeal[index] = true
		}

		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		erasuredPartReader, err := disk.ReadFile(volume, erasurePart, offset)
		if err != nil {
			if err == errFileNotFound {
				// Needs healing if part file not found
				needsSelfHeal[index] = true
			}
			return err
		}
		readers[index] = erasuredPartReader
		defer erasuredPartReader.Close()
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
		var err error
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		writers[index], err = xl.storageDisks[index].CreateFile(volume, erasurePart)
		if err != nil {
			// Unexpected error
			closeAndRemoveWriters(writers...)
			return err
		}
	}
	size, err := strconv.ParseInt(metadata["file.size"], 10, 64)
	if err != nil {
		closeAndRemoveWriters(writers...)
		return err
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
		// Calculate the current shard size.
		curShardSize := getEncodedBlockLen(curBlockSize, xl.DataBlocks)
		enShards := make([][]byte, totalShards)
		// Loop through all readers and read.
		for index, reader := range readers {
			// Initialize shard slice and fill the data from each parts.
			// ReedSolomon.Verify() expects that slice is not nil even if the particular
			// part needs healing.
			enShards[index] = make([]byte, curShardSize)
			if needsSelfHeal[index] {
				// Skip reading if the part needs healing.
				continue
			}
			_, e := io.ReadFull(reader, enShards[index])
			if e != nil && e != io.ErrUnexpectedEOF {
				enShards[index] = nil
			}
		}

		// Check blocks if they are all zero in length.
		if checkBlockSize(enShards) == 0 {
			err = errors.New("Data likely corrupted, all blocks are zero in length.")
			return err
		}

		// Verify the shards.
		ok, e := xl.ReedSolomon.Verify(enShards)
		if e != nil {
			closeAndRemoveWriters(writers...)
			return e
		}

		// Verification failed, shards require reconstruction.
		if !ok {
			for index, shNeeded := range needsSelfHeal {
				if shNeeded {
					// Reconstructs() reconstructs the parts if the array is nil.
					enShards[index] = nil
				}
			}
			e = xl.ReedSolomon.Reconstruct(enShards)
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
			// Verify reconstructed shards again.
			ok, e = xl.ReedSolomon.Verify(enShards)
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
			if !ok {
				// Shards cannot be reconstructed, corrupted data.
				e = errors.New("Verification failed after reconstruction, data likely corrupted.")
				closeAndRemoveWriters(writers...)
				return e
			}
		}
		for index, shNeeded := range needsSelfHeal {
			if !shNeeded {
				continue
			}
			_, e := writers[index].Write(enShards[index])
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

	// Write part.json where ever healing was done.
	var metadataWriters = make([]io.WriteCloser, len(xl.storageDisks))
	for index, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		metadataFile := slashpath.Join(path, metadataFile)
		metadataWriters[index], err = xl.storageDisks[index].CreateFile(volume, metadataFile)
		if err != nil {
			closeAndRemoveWriters(writers...)
			return err
		}
	}
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		closeAndRemoveWriters(metadataWriters...)
		return err
	}
	for index, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		_, err = metadataWriters[index].Write(metadataBytes)
		if err != nil {
			closeAndRemoveWriters(metadataWriters...)
			return err
		}
	}

	// Metadata written for all the healed parts hence Close() so that
	// temp files can be committed.
	for index := range xl.storageDisks {
		if !needsSelfHeal[index] {
			continue
		}
		metadataWriters[index].Close()
	}
	return nil
}

// self heal.
type selfHeal struct {
	volume string
	path   string
	errCh  chan<- error
}

// selfHealRoutine - starts a go routine and listens on a channel for healing requests.
func (xl *XL) selfHealRoutine() {
	xl.selfHealCh = make(chan selfHeal)

	// Healing request can be made like this:
	// errCh := make(chan error)
	// xl.selfHealCh <- selfHeal{"testbucket", "testobject", errCh}
	// fmt.Println(<-errCh)
	go func() {
		for sh := range xl.selfHealCh {
			if sh.volume == "" || sh.path == "" {
				sh.errCh <- errors.New("volume or path can not be empty")
				continue
			}
			xl.selfHeal(sh.volume, sh.path)
			sh.errCh <- nil
		}
	}()
}
