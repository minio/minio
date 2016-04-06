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
	"sync"

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
	path                        string
	minFreeDisk                 int64
	rwLock                      *sync.RWMutex
	listObjectMap               map[listObjectParams][]*treeWalker
	listObjectMapMutex          *sync.Mutex
	listMultipartObjectMap      map[listMultipartObjectParams][]multipartObjectInfoChannel
	listMultipartObjectMapMutex *sync.Mutex
}

// newFS instantiate a new filesystem.
func newFS(rootPath string) (ObjectAPI, *probe.Error) {
	fs := &Filesystem{
		rwLock: &sync.RWMutex{},
	}
	fs.path = rootPath

	/// Defaults

	// Minium free disk required for i/o operations to succeed.
	fs.minFreeDisk = 5

	fs.listObjectMap = make(map[listObjectParams][]*treeWalker)
	fs.listObjectMapMutex = &sync.Mutex{}

	fs.listMultipartObjectMap = make(map[listMultipartObjectParams][]multipartObjectInfoChannel)
	fs.listMultipartObjectMapMutex = &sync.Mutex{}

	// Return here.
	return fs, nil
}

// GetRootPath - get root path.
func (fs Filesystem) GetRootPath() string {
	return fs.path
}
