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

import "io"

// writeN - writes the all contents from path to aforementioned
// writer until the size. Additionally you can also provide a starting
// to read at.
func writeN(disk StorageAPI, volume string, path string, startOffset int64, size int64, writer io.Writer) error {
	var totalLeft = size
	for totalLeft > 0 {
		// Figure out the right blockSize to read.
		var curBlockSize int64
		if blockSizeV1 < totalLeft {
			curBlockSize = blockSizeV1
		} else {
			curBlockSize = totalLeft
		}
		buf, err := disk.ReadFile(volume, path, startOffset, curBlockSize)
		if err != nil {
			return err
		}
		n, err := writer.Write(buf)
		if err != nil {
			return err
		}
		totalLeft -= int64(n)
		startOffset += int64(n)
	}
	return nil
}

// readAll - returns contents from volume/path as byte array.
// Be careful on using this function to read large files, this is
// only meant to be used on case to case basis. Predominantly used
// in case of reading metadata json files.
func readAll(disk StorageAPI, volume string, path string) ([]byte, error) {
	fi, err := disk.StatFile(volume, path)
	if err != nil {
		return nil, err
	}
	var startOffset = int64(0)
	var totalLeft = fi.Size
	// Read the entire file.
	return disk.ReadFile(volume, path, startOffset, totalLeft)
}
