/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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
	"os"

	"github.com/ncw/directio"
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
func GetPerformance(path string, size int64) Performance {
	perf := Performance{}
	write, read, err := doPerfMeasure(path, size)
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
func doPerfMeasure(fsPath string, size int64) (writeSpeed, readSpeed float64, err error) {
	// Remove the file created for speed test purposes
	defer os.RemoveAll(fsPath)

	// Create a file with O_DIRECT flag
	w, err := OpenFileDirectIO(fsPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil {
		return 0, 0, err
	}

	// Fetch aligned buf for direct-io
	buf := directio.AlignedBlock(speedTestBlockSize)

	writeSpeed, err = speedTestWrite(w, buf, size)
	w.Close()
	if err != nil {
		return 0, 0, err
	}

	// Open file to compute read speed
	r, err := OpenFileDirectIO(fsPath, os.O_RDONLY, 0666)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()

	readSpeed, err = speedTestRead(r, buf, size)
	if err != nil {
		return 0, 0, err
	}

	return writeSpeed, readSpeed, nil
}
