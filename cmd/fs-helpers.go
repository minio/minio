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

import "io"

// Reads from the requested local location uses a staging buffer. Restricts
// reads upto requested range of length and offset. If successful staging
// buffer is written to the incoming stream. Returns errors if any.
func fsReadFile(disk StorageAPI, bucket, object string, writer io.Writer, totalLeft, startOffset int64, buf []byte) (err error) {
	bufSize := int64(len(buf))
	// Start the read loop until requested range.
	for {
		// Figure out the right size for the buffer.
		curLeft := bufSize
		if totalLeft < bufSize {
			curLeft = totalLeft
		}
		// Reads the file at offset.
		nr, er := disk.ReadFile(bucket, object, startOffset, buf[:curLeft])
		if nr > 0 {
			// Write to response writer.
			nw, ew := writer.Write(buf[0:nr])
			if nw > 0 {
				// Decrement whats left to write.
				totalLeft -= int64(nw)

				// Progress the offset
				startOffset += int64(nw)
			}
			if ew != nil {
				err = traceError(ew)
				break
			}
			if nr != int64(nw) {
				err = traceError(io.ErrShortWrite)
				break
			}
		}
		if er == io.EOF || er == io.ErrUnexpectedEOF {
			break
		}
		if er != nil {
			err = traceError(er)
			break
		}
		if totalLeft == 0 {
			break
		}
	}
	return err
}

// Reads from input stream until end of file, takes an input buffer for staging reads.
// The staging buffer is then written to the disk. Returns for any error that occurs
// while reading the stream or writing to disk. Caller should cleanup partial files.
// Upon errors total data written will be 0 and returns error, on success returns
// total data written to disk.
func fsCreateFile(disk StorageAPI, reader io.Reader, buf []byte, bucket, object string) (int64, error) {
	bytesWritten := int64(0)
	// Read the buffer till io.EOF and appends data to path at bucket/object.
	for {
		n, rErr := reader.Read(buf)
		if rErr != nil && rErr != io.EOF {
			return 0, traceError(rErr)
		}
		bytesWritten += int64(n)
		wErr := disk.AppendFile(bucket, object, buf[0:n])
		if wErr != nil {
			return 0, traceError(wErr)
		}
		if rErr == io.EOF {
			break
		}
	}
	return bytesWritten, nil
}
