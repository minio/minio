/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package disk

import (
	"io"
	"math"
	"time"

	humanize "github.com/dustin/go-humanize"
)

var speedTestBlockSize = 4 * humanize.MiByte

// speedTestWrite computes the write speed by writing
// `speedTestFileSize` bytes of data to `w` in 4MiB direct-aligned
// blocks present in `buf`
func speedTestWrite(w io.Writer, buf []byte, size int64) (float64, error) {
	// Write speedTestFileSize of data and record write speed
	startTime := time.Now()
	remaining := size
	for remaining > 0 {
		var toWrite int
		// there's more remaining to write than the buffer can hold
		if int64(len(buf)) < remaining {
			toWrite = len(buf)
		} else { // buffer can hold all there is to write
			toWrite = int(remaining)
		}

		written, err := w.Write(buf[:toWrite])
		if err != nil {
			return 0, err
		}

		remaining = remaining - int64(written)
	}

	elapsedTime := time.Since(startTime).Seconds()
	totalWriteMBs := float64(size) / humanize.MiByte
	writeSpeed := totalWriteMBs / elapsedTime

	return roundToTwoDecimals(writeSpeed), nil
}

// speedTestRead computes the read speed by reading
// `speedTestFileSize` bytes from the reader `r` using 4MiB size `buf`
func speedTestRead(r io.Reader, buf []byte, size int64) (float64, error) {
	// Read speedTestFileSize and record read speed
	startTime := time.Now()
	remaining := size
	for remaining > 0 {
		// reads `speedTestBlockSize` on every read
		n, err := io.ReadFull(r, buf)
		if err == io.ErrUnexpectedEOF || err == nil {
			remaining = remaining - int64(n)
			continue
		}

		// Nothing more left to read from the Reader
		if err == io.EOF {
			break
		}
		// Error while reading from the underlying Reader
		if err != nil {
			return 0, err
		}
	}

	if remaining > 0 {
		return 0, io.ErrUnexpectedEOF
	}

	elapsedTime := time.Since(startTime).Seconds()
	totalReadMBs := float64(size) / humanize.MiByte
	readSpeed := totalReadMBs / elapsedTime

	return roundToTwoDecimals(readSpeed), nil
}

func roundToTwoDecimals(num float64) float64 {
	return math.Round(num*100) / 100
}
