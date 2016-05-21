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
	"io"
	"sync"
)

// cleanupCreateFileOps - cleans up all the temporary files and other
// temporary data upon any failure.
func (e erasure) cleanupCreateFileOps(volume, path string, writers []io.WriteCloser) {
	// Close and remove temporary writers.
	for _, writer := range writers {
		if err := safeCloseAndRemove(writer); err != nil {
			errorIf(err, "Failed to close writer.")
		}
	}
	// Remove any temporary written data.
	for _, disk := range e.storageDisks {
		if err := disk.DeleteFile(volume, path); err != nil {
			errorIf(err, "Unable to delete file.")
		}
	}
}

// WriteErasure reads predefined blocks, encodes them and writes to
// configured storage disks.
func (e erasure) writeErasure(volume, path string, reader *io.PipeReader, wcloser *waitCloser) {
	// Release the block writer upon function return.
	defer wcloser.release()

	writers := make([]io.WriteCloser, len(e.storageDisks))

	// Initialize all writers.
	for index, disk := range e.storageDisks {
		writer, err := disk.CreateFile(volume, path)
		if err != nil {
			e.cleanupCreateFileOps(volume, path, writers)
			reader.CloseWithError(err)
			return
		}
		writers[index] = writer
	}

	// Allocate 4MiB block size buffer for reading.
	dataBuffer := make([]byte, erasureBlockSize)
	for {
		// Read up to allocated block size.
		n, err := io.ReadFull(reader, dataBuffer)
		if err != nil {
			// Any unexpected errors, close the pipe reader with error.
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				// Remove all temp writers.
				e.cleanupCreateFileOps(volume, path, writers)
				reader.CloseWithError(err)
				return
			}
		}
		// At EOF break out.
		if err == io.EOF {
			break
		}
		if n > 0 {
			// Split the input buffer into data and parity blocks.
			var dataBlocks [][]byte
			dataBlocks, err = e.ReedSolomon.Split(dataBuffer[0:n])
			if err != nil {
				// Remove all temp writers.
				e.cleanupCreateFileOps(volume, path, writers)
				reader.CloseWithError(err)
				return
			}

			// Encode parity blocks using data blocks.
			err = e.ReedSolomon.Encode(dataBlocks)
			if err != nil {
				// Remove all temp writers upon error.
				e.cleanupCreateFileOps(volume, path, writers)
				reader.CloseWithError(err)
				return
			}

			var wg = &sync.WaitGroup{}
			var wErrs = make([]error, len(writers))
			// Write encoded data to quorum disks in parallel.
			for index, writer := range writers {
				if writer == nil {
					continue
				}
				wg.Add(1)
				// Write encoded data in routine.
				go func(index int, writer io.Writer) {
					defer wg.Done()
					encodedData := dataBlocks[index]
					_, wErr := writers[index].Write(encodedData)
					if wErr != nil {
						wErrs[index] = wErr
						return
					}
					wErrs[index] = nil
				}(index, writer)
			}
			wg.Wait()

			// Cleanup and return on first non-nil error.
			for _, wErr := range wErrs {
				if wErr == nil {
					continue
				}
				// Remove all temp writers upon error.
				e.cleanupCreateFileOps(volume, path, writers)
				reader.CloseWithError(wErr)
				return
			}
		}
	}

	// Close all writers and metadata writers in routines.
	for _, writer := range writers {
		if writer == nil {
			continue
		}
		// Safely wrote, now rename to its actual location.
		if err := writer.Close(); err != nil {
			// Remove all temp writers upon error.
			e.cleanupCreateFileOps(volume, path, writers)
			reader.CloseWithError(err)
			return
		}
	}

	// Close the pipe reader and return.
	reader.Close()
	return
}

// CreateFile - create a file.
func (e erasure) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	// Initialize pipe for data pipe line.
	pipeReader, pipeWriter := io.Pipe()

	// Initialize a new wait closer, implements both Write and Close.
	wcloser := newWaitCloser(pipeWriter)

	// Start erasure encoding in routine, reading data block by block from pipeReader.
	go e.writeErasure(volume, path, pipeReader, wcloser)

	// Return the writer, caller should start writing to this.
	return wcloser, nil
}
