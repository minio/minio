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
	// maintain a list of input and output channels for communicating with services
	var ctrlChans []chan<- string
	var statusChans []<-chan error

	// a pair of control channels, we use these primarily to add to the lists above
	var ctrlChan chan<- string
	var statusChan <-chan error

	// configure web server
	var storage mstorage.Storage
	var httpConfig = httpserver.HttpServerConfig{}
	httpConfig.Address = config.Address
	httpConfig.TLS = config.Tls

	if config.CertFile != "" {
		httpConfig.CertFile = config.CertFile
	}
	if config.KeyFile != "" {
		httpConfig.KeyFile = config.KeyFile
	}

	// instantiate storage
	// preconditions:
	//    - storage type specified
	//    - any configuration for storage is populated
	// postconditions:
	//    - storage driver is initialized
	//    - ctrlChans has channel to communicate to storage
	//    - statusChans has channel for messages coming from storage
	switch {
	case config.StorageType == InMemoryStorage:
		{
			ctrlChan, statusChan, storage = inmemory.Start()
			ctrlChans = append(ctrlChans, ctrlChan)
			statusChans = append(statusChans, statusChan)
		}
	case config.StorageType == FileStorage:
		{
			// TODO Replace this with a more configurable and robust version
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
		}
	default: // should never happen
		log.Fatal("No storage driver found")
	}

	// start minio api in a web server, pass storage driver into it
	ctrlChan, statusChan, _ = httpserver.Start(minioapi.HttpHandler(storage), httpConfig)
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
