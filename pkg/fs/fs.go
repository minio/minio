/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"os"
	"sync"
	"time"

	"github.com/minio/minio-xl/pkg/probe"
)

// Filesystem - local variables
type Filesystem struct {
	path        string
	minFreeDisk int64
	lock        *sync.Mutex
	multiparts  *Multiparts
}

// MultipartSession holds active session information
type MultipartSession struct {
	TotalParts int
	UploadID   string
	Initiated  time.Time
	Parts      []*PartMetadata
}

// Multiparts collection of many parts
type Multiparts struct {
	Version       string                       `json:"version"`
	ActiveSession map[string]*MultipartSession `json:"activeSessions"`
}

// New instantiate a new donut
func New() (Filesystem, *probe.Error) {
	var err *probe.Error
	// load multiparts session from disk
	var multiparts *Multiparts
	multiparts, err = loadMultipartsSession()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			multiparts = &Multiparts{
				Version:       "1",
				ActiveSession: make(map[string]*MultipartSession),
			}
			if err := SaveMultipartsSession(multiparts); err != nil {
				return Filesystem{}, err.Trace()
			}
		} else {
			return Filesystem{}, err.Trace()
		}
	}
	a := Filesystem{lock: new(sync.Mutex)}
	a.multiparts = multiparts
	return a, nil
}

// SetRootPath - set root path
func (fs *Filesystem) SetRootPath(path string) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	fs.path = path
}

// SetMinFreeDisk - set min free disk
func (fs *Filesystem) SetMinFreeDisk(minFreeDisk int64) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	fs.minFreeDisk = minFreeDisk
}
