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
	"time"

	"github.com/minio/minio/pkg/probe"
)

// listObjectParams - list object params used for list object map
type listObjectParams struct {
	bucket    string
	delimiter string
	marker    string
	prefix    string
}

// Filesystem - local variables
type Filesystem struct {
	path               string
	minFreeDisk        int64
	rwLock             *sync.RWMutex
	multiparts         *multiparts
	listObjectMap      map[listObjectParams][]objectInfoChannel
	listObjectMapMutex *sync.Mutex
}

// MultipartSession holds active session information
type multipartSession struct {
	TotalParts int
	ObjectName string
	UploadID   string
	Initiated  time.Time
	Parts      []partInfo
}

// multiparts collection of many parts
type multiparts struct {
	Version       string                       `json:"version"`
	ActiveSession map[string]*multipartSession `json:"activeSessions"`
}

func (fs *Filesystem) pushListObjectCh(params listObjectParams, ch objectInfoChannel) {
	fs.listObjectMapMutex.Lock()
	defer fs.listObjectMapMutex.Unlock()

	channels := []objectInfoChannel{ch}
	if _, ok := fs.listObjectMap[params]; ok {
		channels = append(fs.listObjectMap[params], ch)
	}

	fs.listObjectMap[params] = channels
}

func (fs *Filesystem) popListObjectCh(params listObjectParams) *objectInfoChannel {
	fs.listObjectMapMutex.Lock()
	defer fs.listObjectMapMutex.Unlock()

	if channels, ok := fs.listObjectMap[params]; ok {
		for i, channel := range channels {
			if !channel.timedOut {
				// channels[:i] have all timed out, hence remove them too.
				chs := channels[i+1:]
				if len(chs) > 0 {
					fs.listObjectMap[params] = chs
				} else {
					delete(fs.listObjectMap, params)
				}

				return &channel
			}
		}

		// As all channels are timed out, delete the map entry
		delete(fs.listObjectMap, params)
	}

	return nil
}

// newFS instantiate a new filesystem.
func newFS(rootPath string) (ObjectAPI, *probe.Error) {
	setFSMultipartsMetadataPath(filepath.Join(rootPath, "$multiparts-session.json"))

	var err *probe.Error
	// load multiparts session from disk
	var mparts *multiparts
	mparts, err = loadMultipartsSession()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			mparts = &multiparts{
				Version:       "1",
				ActiveSession: make(map[string]*multipartSession),
			}
			if err = saveMultipartsSession(*mparts); err != nil {
				return nil, err.Trace()
			}
		} else {
			return nil, err.Trace()
		}
	}

	fs := &Filesystem{
		rwLock: &sync.RWMutex{},
	}
	fs.path = rootPath
	fs.multiparts = mparts

	/// Defaults

	// Minium free disk required for i/o operations to succeed.
	fs.minFreeDisk = 5

	fs.listObjectMap = make(map[listObjectParams][]objectInfoChannel)
	fs.listObjectMapMutex = &sync.Mutex{}

	// Return here.
	return fs, nil
}

// GetRootPath - get root path.
func (fs Filesystem) GetRootPath() string {
	return fs.path
}
