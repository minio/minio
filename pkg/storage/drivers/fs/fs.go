/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package filesystem

import (
	"os"
	"sync"

	"github.com/minio/minio/pkg/storage/drivers"
)

type fsDriver struct {
	root       string
	lock       *sync.Mutex
	multiparts *Multiparts
}

// Start filesystem channel
func Start(root string) (chan<- string, <-chan error, drivers.Driver) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	fs := new(fsDriver)
	fs.root = root
	fs.lock = new(sync.Mutex)
	// internal related to multiparts
	fs.multiparts = new(Multiparts)
	fs.multiparts.ActiveSession = make(map[string]*MultipartSession)
	go start(ctrlChannel, errorChannel, fs)
	return ctrlChannel, errorChannel, fs
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, fs *fsDriver) {
	err := os.MkdirAll(fs.root, 0700)
	errorChannel <- err
	close(errorChannel)
}
