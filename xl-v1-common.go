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
	"math/rand"
	"path"
	"sync"
	"time"
)

// getRandomDisk - gives a random disk at any point in time from the
// available pool of disks.
func (xl xlObjects) getRandomDisk() (disk StorageAPI) {
	rand.Seed(time.Now().UTC().UnixNano()) // Seed with current time.
	randIndex := rand.Intn(len(xl.storageDisks) - 1)
	disk = xl.storageDisks[randIndex] // Pick a random disk.
	return disk
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." {
			return false
		}
		if xl.isObject(bucket, p) {
			// If there is already a file at prefix "p" return error.
			return true
		}
		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

func (xl xlObjects) isObject(bucket, prefix string) bool {
	// Create errs and volInfo slices of storageDisks size.
	var errs = make([]error, len(xl.storageDisks))

	// Allocate a new waitgroup.
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Stat file on all the disks in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the Stat operations to finish.
	wg.Wait()

	var errFileNotFoundCount int
	for _, err := range errs {
		if err != nil {
			if err == errFileNotFound {
				errFileNotFoundCount++
				// If we have errors with file not found greater than allowed read
				// quorum we return err as errFileNotFound.
				if errFileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
					return false
				}
				continue
			}
			errorIf(err, "Unable to access file "+path.Join(bucket, prefix))
			return false
		}
	}
	return true
}

// statPart - stat a part file.
func (xl xlObjects) statPart(bucket, objectPart string) (fileInfo FileInfo, err error) {
	// Count for errors encountered.
	var xlJSONErrCount = 0

	// Return the first success entry based on the selected random disk.
	for xlJSONErrCount < len(xl.storageDisks) {
		// Choose a random disk on each attempt.
		disk := xl.getRandomDisk()
		fileInfo, err = disk.StatFile(bucket, objectPart)
		if err == nil {
			return fileInfo, nil
		}
		xlJSONErrCount++ // Update error count.
	}
	return FileInfo{}, err
}
