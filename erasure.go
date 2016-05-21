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

	"github.com/klauspost/reedsolomon"
)

// erasure storage layer.
type erasure struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataBlocks   int
	ParityBlocks int
	storageDisks []StorageAPI
}

// errUnexpected - returned for any unexpected error.
var errUnexpected = errors.New("Unexpected error - please report at https://github.com/minio/minio/issues")

// newErasure instantiate a new erasure.
func newErasure(disks []StorageAPI) (StorageAPI, error) {
	// Initialize XL.
	xl := &erasure{}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(disks)/2, len(disks)/2

	// Initialize reed solomon encoding.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return nil, err
	}

	// Save the reedsolomon.
	xl.DataBlocks = dataBlocks
	xl.ParityBlocks = parityBlocks
	xl.ReedSolomon = rs

	// Save all the initialized storage disks.
	xl.storageDisks = disks

	// Return successfully initialized.
	return xl, nil
}

// MakeVol - make a volume.
func (xl erasure) MakeVol(volume string) error {
	return nil
}

// DeleteVol - delete a volume.
func (xl erasure) DeleteVol(volume string) error {
	return nil
}

// ListVols - list volumes.
func (xl erasure) ListVols() (volsInfo []VolInfo, err error) {
	return nil, nil
}

// StatVol - get volume stat info.
func (xl erasure) StatVol(volume string) (volInfo VolInfo, err error) {
	return VolInfo{}, nil
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing "/".
func (xl erasure) ListDir(volume, dirPath string) (entries []string, err error) {
	return nil, nil
}

// Object API.

// StatFile - stat a file
func (xl erasure) StatFile(volume, path string) (FileInfo, error) {
	return FileInfo{}, nil
}

// DeleteFile - delete a file.
func (xl erasure) DeleteFile(volume, path string) error {
	return nil
}

// RenameFile - rename file.
func (xl erasure) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	return nil
}
