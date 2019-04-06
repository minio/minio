/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path"
	"strconv"
	"time"

	humanize "github.com/dustin/go-humanize"
)

// file size for performance read and write checks
const (
	randBufSize = 1 * humanize.KiByte
	randParts   = 1024
	fileSize    = randParts * randBufSize

	// Total count of read / write iteration for performance measurement
	iterations = 10
)

// Info stat fs struct is container which holds following values
// Total - total size of the volume / disk
// Free - free size of the volume / disk
// Files - total inodes available
// Ffree - free inodes available
// FSType - file system type
type Info struct {
	Total  uint64
	Free   uint64
	Files  uint64
	Ffree  uint64
	FSType string

	// Usage is calculated per tenant.
	Usage uint64
}

// Performance holds informantion about read and write speed of a disk
type Performance struct {
	Path       string  `json:"path"`
	Error      string  `json:"error,omitempty"`
	WriteSpeed float64 `json:"writeSpeed"`
	ReadSpeed  float64 `json:"readSpeed"`
}

// GetPerformance returns given disk's read and write performance
func GetPerformance(path string) Performance {
	perf := Performance{}
	write, read, err := doPerfMeasure(path)
	if err != nil {
		perf.Error = err.Error()
		return perf
	}
	perf.WriteSpeed = write
	perf.ReadSpeed = read
	return perf
}

// Calculate the write and read performance - write and read 10 tmp (1 MiB)
// files and find the average time taken (Bytes / Sec)
func doPerfMeasure(fsPath string) (write, read float64, err error) {
	var count int
	var totalWriteElapsed time.Duration
	var totalReadElapsed time.Duration

	defer os.RemoveAll(fsPath)

	randBuf := make([]byte, randBufSize)
	rand.Read(randBuf)
	buf := bytes.Repeat(randBuf, randParts)

	// create the enclosing directory
	err = os.MkdirAll(fsPath, 0777)
	if err != nil {
		return 0, 0, err
	}

	for count = 1; count <= iterations; count++ {
		fsTempObjPath := path.Join(fsPath, strconv.Itoa(count))

		// Write performance calculation
		writeStart := time.Now()
		n, err := writeFile(fsTempObjPath, buf)

		if err != nil {
			return 0, 0, err
		}
		if n != fileSize {
			return 0, 0, errors.New("Could not write temporary data to disk")
		}

		writeElapsed := time.Since(writeStart)
		totalWriteElapsed += writeElapsed

		// Read performance calculation
		readStart := time.Now()
		n, err = readFile(fsTempObjPath, buf)

		if err != nil {
			return 0, 0, err
		}
		if n != fileSize {
			return 0, 0, errors.New("Could not read temporary data from disk")
		}

		readElapsed := time.Since(readStart)
		totalReadElapsed += readElapsed
	}
	// Average time spent = total time elapsed / number of writes
	avgWriteTime := totalWriteElapsed.Seconds() / float64(count)
	// Write perf = fileSize (in Bytes) / average time spent writing (in seconds)
	write = fileSize / avgWriteTime

	// Average time spent = total time elapsed / number of writes
	avgReadTime := totalReadElapsed.Seconds() / float64(count)
	// read perf = fileSize (in Bytes) / average time spent reading (in seconds)
	read = fileSize / avgReadTime

	return write, read, nil
}
