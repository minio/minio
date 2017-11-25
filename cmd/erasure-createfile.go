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
	"io"

	"github.com/minio/minio/pkg/errors"
)

// CreateFile creates a new bitrot encoded file spread over all available disks. CreateFile will create
// the file at the given volume and path. It will read from src until an io.EOF occurs. The given algorithm will
// be used to protect the erasure encoded file.
func (s *ErasureStorage) CreateFile(src io.Reader, volume, path string, buffer []byte, algorithm BitrotAlgorithm, writeQuorum int) (f ErasureFileInfo, err error) {
	if !algorithm.Available() {
		return f, errors.Trace(errBitrotHashAlgoInvalid)
	}
	f.Checksums = make([][]byte, len(s.disks))
	hashers := make([]hash.Hash, len(s.disks))
	for i := range hashers {
		hashers[i] = algorithm.New()
	}
	errChans, errs := make([]chan error, len(s.disks)), make([]error, len(s.disks))
	for i := range errChans {
		errChans[i] = make(chan error, 1) // create buffered channel to let finished go-routines die early
	}

	var blocks [][]byte
	var n = len(buffer)
	for n == len(buffer) {
		n, err = io.ReadFull(src, buffer)
		if n == 0 && err == io.EOF {
			if f.Size != 0 { // don't write empty block if we have written to the disks
				break
			}
			blocks = make([][]byte, len(s.disks)) // write empty block
		} else if err == nil || (n > 0 && err == io.ErrUnexpectedEOF) {
			blocks, err = s.ErasureEncode(buffer[:n])
			if err != nil {
				return f, err
			}
		} else {
			return f, errors.Trace(err)
		}

		for i := range errChans { // span workers
			go erasureAppendFile(s.disks[i], volume, path, hashers[i], blocks[i], errChans[i])
		}
		for i := range errChans { // what until all workers are finished
			errs[i] = <-errChans[i]
		}
		if err = reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, writeQuorum); err != nil {
			return f, err
		}
		s.disks = evalDisks(s.disks, errs)
		f.Size += int64(n)
	}

	f.Algorithm = algorithm
	for i, disk := range s.disks {
		if disk == OfflineDisk {
			continue
		}
		f.Checksums[i] = hashers[i].Sum(nil)
	}
	return f, nil
}

// erasureAppendFile appends the content of buf to the file on the given disk and updates computes
// the hash of the written data. It sends the write error (or nil) over the error channel.
func erasureAppendFile(disk StorageAPI, volume, path string, hash hash.Hash, buf []byte, errChan chan<- error) {
	if disk == OfflineDisk {
		errChan <- errors.Trace(errDiskNotFound)
		return
	}
	err := disk.AppendFile(volume, path, buf)
	if err != nil {
		errChan <- err
		return
	}
	hash.Write(buf)
	errChan <- err
}
