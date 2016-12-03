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

func fsCreateFile(disk StorageAPI, reader io.Reader, buf []byte, tmpBucket, tempObj string) (int64, error) {
	bytesWritten := int64(0)
	// Read the buffer till io.EOF and append the read data to the temporary file.
	for {
		n, rErr := reader.Read(buf)
		if rErr != nil && rErr != io.EOF {
			return 0, traceError(rErr)
		}
		bytesWritten += int64(n)
		wErr := disk.AppendFile(tmpBucket, tempObj, buf[0:n])
		if wErr != nil {
			return 0, traceError(wErr)
		}
		if rErr == io.EOF {
			break
		}
	}
	return bytesWritten, nil
}
