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
	"path"
	"reflect"

	"github.com/minio-io/minio/pkg/api/minioapi"
	"github.com/minio-io/minio/pkg/api/webuiapi"
	"github.com/minio-io/minio/pkg/httpserver"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/fs"
	"github.com/minio-io/minio/pkg/storage/inmemory"
	"github.com/minio-io/minio/pkg/utils/helpers"
)

type ServerConfig struct {
	Address  string
	Tls      bool
	CertFile string
	KeyFile  string
	ApiType  interface{}
}

type MinioApi struct {
	StorageType StorageType
}

type WebUIApi struct {
	Websocket bool
}

type StorageType int

const (
	InMemoryStorage = iota
	FileStorage
)

func getHttpChannels(configs []ServerConfig) (ctrlChans []chan<- string, statusChans []<-chan error) {
	// a pair of control channels, we use these primarily to add to the lists above
	var ctrlChan chan<- string
	var statusChan <-chan error

	for _, config := range configs {
		switch k := config.ApiType.(type) {
		case MinioApi:
			{
				// configure web server
				var storage mstorage.Storage
				var httpConfig = httpserver.HttpServerConfig{}
				httpConfig.Address = config.Address
				httpConfig.Websocket = false
				httpConfig.TLS = config.Tls

				if config.CertFile != "" {
					httpConfig.CertFile = config.CertFile
				}
				if config.KeyFile != "" {
					httpConfig.KeyFile = config.KeyFile
				}

				ctrlChans, statusChans, storage = getStorageChannels(k.StorageType)
				// start minio api in a web server, pass storage driver into it
				ctrlChan, statusChan, _ = httpserver.Start(minioapi.HttpHandler(storage), httpConfig)

				ctrlChans = append(ctrlChans, ctrlChan)
				statusChans = append(statusChans, statusChan)

			}
		case WebUIApi:
			{
				var httpConfig = httpserver.HttpServerConfig{}
				httpConfig.Address = config.Address
				httpConfig.TLS = config.Tls
				httpConfig.CertFile = config.CertFile
				httpConfig.KeyFile = config.KeyFile

				httpConfig.Websocket = k.Websocket
				ctrlChan, statusChan, _ = httpserver.Start(webuiapi.HttpHandler(), httpConfig)

				ctrlChans = append(ctrlChans, ctrlChan)
				statusChans = append(statusChans, statusChan)
			}
		default:
			log.Fatal("Invalid api type")
		}
	}
	return
}

func getStorageChannels(storageType StorageType) (ctrlChans []chan<- string, statusChans []<-chan error, storage mstorage.Storage) {
	// a pair of control channels, we use these primarily to add to the lists above
	var ctrlChan chan<- string
	var statusChan <-chan error

	// instantiate storage
	// preconditions:
	//    - storage type specified
	//    - any configuration for storage is populated
	// postconditions:
	//    - storage driver is initialized
	//    - ctrlChans has channel to communicate to storage
	//    - statusChans has channel for messages coming from storage
	switch {
	case storageType == InMemoryStorage:
		{
			ctrlChan, statusChan, storage = inmemory.Start()
			ctrlChans = append(ctrlChans, ctrlChan)
			statusChans = append(statusChans, statusChan)
		}
	case storageType == FileStorage:
		{
			homeDir := helpers.HomeDir()
			root := path.Join(homeDir, "minio-storage")
			ctrlChan, statusChan, storage = fs.Start(root)
			ctrlChans = append(ctrlChans, ctrlChan)
			statusChans = append(statusChans, statusChan)
		}
	default: // should never happen
		log.Fatal("No storage driver found")
	}
	return
}

func Start(configs []ServerConfig) {
	// reflected looping is necessary to remove dead channels from loop and not flood switch
	ctrlChans, statusChans := getHttpChannels(configs)
	cases := createSelectCases(statusChans)
	for len(cases) > 0 {
		chosen, value, recvOk := reflect.Select(cases)
		switch recvOk {
		case true:
			// Status Message Received
			switch true {
			case value.Interface() != nil:
				// For any error received cleanup all existing channels and fail
				for _, ch := range ctrlChans {
					close(ch)
				}
				log.Fatal(value.Interface())
			}
		case false:
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

// creates select cases for reflect to switch over dynamically
// this is necessary in order to remove dead channels and not flood
// the loop with closed channel errors
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
