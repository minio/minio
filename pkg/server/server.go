/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package server

import (
	"log"
	"os"
	"os/user"
	"path"
	"reflect"

	"github.com/minio-io/minio/pkg/httpserver"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/fs"
	"github.com/minio-io/minio/pkg/storage/inmemory"
	"github.com/minio-io/minio/pkg/webapi/minioapi"
)

type ServerConfig struct {
	Address     string
	Tls         bool
	CertFile    string
	KeyFile     string
	StorageType StorageType
}

type StorageType int

const (
	InMemoryStorage = iota
	FileStorage
)

func Start(config ServerConfig) {
	var ctrlChans []chan<- string
	var statusChans []<-chan error

	var ctrlChan chan<- string
	var statusChan <-chan error
	var storage mstorage.Storage
	var srv = httpserver.HttpServer{}
	srv.Address = config.Address
	srv.TLS = config.Tls

	if config.CertFile != "" {
		srv.CertFile = config.CertFile
	}
	if config.KeyFile != "" {
		srv.KeyFile = config.KeyFile
	}

	if config.StorageType == InMemoryStorage {
		ctrlChan, statusChan, storage = inmemory.Start()
		ctrlChans = append(ctrlChans, ctrlChan)
		statusChans = append(statusChans, statusChan)
	} else if config.StorageType == FileStorage {
		currentUser, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		rootPath := path.Join(currentUser.HomeDir, "minio-storage")
		_, err = os.Stat(rootPath)
		if os.IsNotExist(err) {
			err = os.Mkdir(rootPath, 0700)
		} else if err != nil {
			log.Fatal("Could not create $HOME/minio-storage", err)
		}
		ctrlChan, statusChan, storage = fs.Start(rootPath)
		ctrlChans = append(ctrlChans, ctrlChan)
		statusChans = append(statusChans, statusChan)
	} else {

	}

	ctrlChan, statusChan = httpserver.Start(minioapi.HttpHandler(storage), srv)
	ctrlChans = append(ctrlChans, ctrlChan)
	statusChans = append(statusChans, statusChan)

	cases := createSelectCases(statusChans)

	for len(cases) > 0 {
		chosen, value, recvOk := reflect.Select(cases)
		if recvOk == true {
			// Status Message Received
			log.Println(chosen, value.Interface(), recvOk)
		} else {
			// Channel closed, remove from list
			var aliveStatusChans []<-chan error
			for i, ch := range statusChans {
				if i != chosen {
					aliveStatusChans = append(aliveStatusChans, ch)
				}
			}
			// create new select cases without defunct channel
			statusChans = aliveStatusChans
			cases = createSelectCases(statusChans)
		}
	}
}

func createSelectCases(channels []<-chan error) []reflect.SelectCase {
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	return cases
}
