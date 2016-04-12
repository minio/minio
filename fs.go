/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/probe"
)

// listObjectParams - list object params used for list object map
type listObjectParams struct {
	bucket    string
	delimiter string
	marker    string
	prefix    string
}

// listMultipartObjectParams - list multipart object params used for list multipart object map
type listMultipartObjectParams struct {
	bucket         string
	delimiter      string
	keyMarker      string
	prefix         string
	uploadIDMarker string
}

// Filesystem - local variables
type Filesystem struct {
	diskPath                    string
	minFreeDisk                 int64
	rwLock                      *sync.RWMutex
	listObjectMap               map[listObjectParams][]*treeWalker
	listObjectMapMutex          *sync.Mutex
	listMultipartObjectMap      map[listMultipartObjectParams][]<-chan multipartObjectInfo
	listMultipartObjectMapMutex *sync.Mutex
}

// newFS instantiate a new filesystem.
func newFS(diskPath string) (ObjectAPI, *probe.Error) {
	fs := &Filesystem{
		rwLock: &sync.RWMutex{},
	}
	fs.diskPath = diskPath

	/// Defaults
	// Minium free disk required for i/o operations to succeed.
	fs.minFreeDisk = 5

	// Initialize list object map.
	fs.listObjectMap = make(map[listObjectParams][]*treeWalker)
	fs.listObjectMapMutex = &sync.Mutex{}

	// Initialize list multipart map.
	fs.listMultipartObjectMap = make(map[listMultipartObjectParams][]<-chan multipartObjectInfo)
	fs.listMultipartObjectMapMutex = &sync.Mutex{}

	// Return here.
	return fs, nil
}

func (fs Filesystem) checkBucketArg(bucket string) (string, error) {
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	bucket = getActualBucketname(fs.diskPath, bucket)
	if status, e := isDirExist(filepath.Join(fs.diskPath, bucket)); !status {
		if e == nil {
			return "", BucketNotFound{Bucket: bucket}
		} else if os.IsNotExist(e) {
			return "", BucketNotFound{Bucket: bucket}
		} else {
			return "", e
		}
	}
	return bucket, nil
}

func checkDiskFree(diskPath string, minFreeDisk int64) error {
	di, e := disk.GetInfo(diskPath)
	if e != nil {
		return e
	}
	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= minFreeDisk {
		return RootPathFull{Path: diskPath}
	}
	return nil
}

// GetRootPath - get root path.
func (fs Filesystem) GetRootPath() string {
	return fs.diskPath
}
