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
)

// CreateFile creates a new bitrot encoded file spread over all available disks. CreateFile will create
// the file at the given volume and path. It will read from src until an io.EOF occurs. The given algorithm will
// be used to protect the erasure encoded file. The random reader should return random data (if it's nil the system PRNG will be used).
func (s *XLStorage) CreateFile(src io.Reader, volume, path string, buffer []byte, random io.Reader, algorithm bitrot.Algorithm) (f ErasureFileInfo, err error) {
	f.Keys, f.Checksums = make([][]byte, len(s.disks)), make([][]byte, len(s.disks))
	hashers := make([]bitrot.Hash, len(s.disks))
	for i := range hashers {
		f.Keys[i], hashers[i], err = NewBitrotProtector(algorithm, random)
		if err != nil {
			return f, err
		}
	}
	locks, errors := make([]chan error, len(s.disks)), make([]error, len(s.disks))
	for i := range locks {
		locks[i] = make(chan error, 1)
	}
	appendWriters := s.AppendWriters(volume, path)

	blocks, n := [][]byte{}, len(buffer)
	for n == len(buffer) {
		n, err = io.ReadFull(src, buffer)
		if n == 0 && err == io.EOF {
			if f.Size != 0 {
				break
			}
			blocks = make([][]byte, len(s.disks))
		} else if err == nil || (n > 0 && err == io.ErrUnexpectedEOF) {
			blocks, err = s.ErasureEncode(buffer[:n])
			if err != nil {
				return f, err
			}
		} else {
			return f, traceError(err)
		}

		for i := range locks {
			if s.disks[i] == OfflineDisk {
				locks[i] <- traceError(errDiskNotFound)
				continue
			}
			go erasureAppendFile(io.MultiWriter(appendWriters[i], hashers[i]), blocks[i], locks[i])
		}
		for i := range locks {
			errors[i] = <-locks[i]
		}
		if err = reduceWriteQuorumErrs(errors, objectOpIgnoredErrs, s.writeQuorum); err != nil {
			return f, err
		}
		s.disks = evalDisks(s.disks, errors)
		f.Size += int64(n)
	}

	f.Algorithm = algorithm
	for i, disk := range s.disks {
		if disk == OfflineDisk {
			f.Keys[i] = nil
			f.Checksums[i] = nil
		} else {
			f.Checksums[i] = hashers[i].Sum(nil)
		}
	}
	return f, nil
}

func erasureAppendFile(w io.Writer, buf []byte, lock chan<- error) {
	_, err := w.Write(buf)
	lock <- err
}
