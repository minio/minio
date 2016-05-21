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
func (xl erasure) cleanupCreateFileOps(volume, path string, writers ...io.WriteCloser) {
	closeAndRemoveWriters(writers...)
	for _, disk := range xl.storageDisks {
		if err := disk.DeleteFile(volume, path); err != nil {
			errorIf(err, "Unable to delete file.")
		}
	}
}

// Close and remove writers if they are safeFile.
func closeAndRemoveWriters(writers ...io.WriteCloser) {
	for _, writer := range writers {
		if err := safeCloseAndRemove(writer); err != nil {
			errorIf(err, "Failed to close writer.")
		}
	}
}

// WriteErasure reads predefined blocks, encodes them and writes to
// configured storage disks.
func (xl erasure) writeErasure(volume, path string, reader *io.PipeReader, wcloser *waitCloser) {
	// Release the block writer upon function return.
	defer wcloser.release()

	writers := make([]io.WriteCloser, len(xl.storageDisks))

	for index, disk := range xl.storageDisks {
		writer, err := disk.CreateFile(volume, path)
		if err != nil {
			xl.cleanupCreateFileOps(volume, path, writers...)
			reader.CloseWithError(err)
			return
		}
		writers[index] = writer
	}

	// Allocate 4MiB block size buffer for reading.
	dataBuffer := make([]byte, erasureBlockSize)
	var totalSize int64 // Saves total incoming stream size.
	for {
		// Read up to allocated block size.
		n, err := io.ReadFull(reader, dataBuffer)
		if err != nil {
			// Any unexpected errors, close the pipe reader with error.
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				// Remove all temp writers.
				xl.cleanupCreateFileOps(volume, path, writers...)
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
			dataBlocks, err = xl.ReedSolomon.Split(dataBuffer[0:n])
			if err != nil {
				// Remove all temp writers.
				xl.cleanupCreateFileOps(volume, path, writers...)
				reader.CloseWithError(err)
				return
			}

			// Encode parity blocks using data blocks.
			err = xl.ReedSolomon.Encode(dataBlocks)
			if err != nil {
				// Remove all temp writers upon error.
				xl.cleanupCreateFileOps(volume, path, writers...)
				reader.CloseWithError(err)
				return
			}

			var wg = &sync.WaitGroup{}
			var wErrs = make([]error, len(writers))
			// Loop through and write encoded data to quorum disks.
			for index, writer := range writers {
				if writer == nil {
					continue
				}
				wg.Add(1)
				go func(index int, writer io.Writer) {
					defer wg.Done()
					encodedData := dataBlocks[index]
					_, wErr := writers[index].Write(encodedData)
					wErrs[index] = wErr
				}(index, writer)
			}
			wg.Wait()

			for _, wErr := range wErrs {
				if wErr == nil {
					continue
				}
				// Remove all temp writers upon error.
				xl.cleanupCreateFileOps(volume, path, writers...)
				reader.CloseWithError(wErr)
				return
			}

			// Update total written.
			totalSize += int64(n)
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
			xl.cleanupCreateFileOps(volume, path, writers...)
			reader.CloseWithError(err)
			return
		}
	}

	// Close the pipe reader and return.
	reader.Close()
	return
}

// CreateFile - create a file.
func (xl erasure) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
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
	go xl.writeErasure(volume, path, pipeReader, wcloser)

	// Return the writer, caller should start writing to this.
	return wcloser, nil
}
