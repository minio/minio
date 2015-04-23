/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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
	"fmt"
	"os/user"
	"path"
	"reflect"

	"errors"

	"github.com/minio-io/minio/pkg/api"
	"github.com/minio-io/minio/pkg/api/web"
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/server/httpserver"
	"github.com/minio-io/minio/pkg/storage/drivers"
	"github.com/minio-io/minio/pkg/storage/drivers/donut"
	"github.com/minio-io/minio/pkg/storage/drivers/memory"
	"github.com/minio-io/minio/pkg/utils/log"
)

// Config - http server parameters
type Config struct {
	Domain   string
	Address  string
	TLS      bool
	CertFile string
	KeyFile  string
	APIType  interface{}
}

// MinioAPI - driver type donut, file, memory
type MinioAPI struct {
	DriverType DriverType
}

// Web - web related
type Web struct {
	Websocket bool // TODO
}

// DriverType - different driver types supported by minio
type DriverType int

// Driver types
const (
	Memory DriverType = iota
	Donut
)

func getHTTPChannels(configs []Config) (ctrlChans []chan<- string, statusChans []<-chan error) {
	// a pair of control channels, we use these primarily to add to the lists above
	var ctrlChan chan<- string
	var statusChan <-chan error

	for _, config := range configs {
		switch k := config.APIType.(type) {
		case MinioAPI:
			{
				// configure web server
				var driver drivers.Driver
				var httpConfig = httpserver.Config{}
				httpConfig.Address = config.Address
				httpConfig.Websocket = false
				httpConfig.TLS = config.TLS

				if config.CertFile != "" {
					httpConfig.CertFile = config.CertFile
				}
				if config.KeyFile != "" {
					httpConfig.KeyFile = config.KeyFile
				}

				ctrlChans, statusChans, driver = getDriverChannels(k.DriverType)
				// start minio api in a web server, pass driver driver into it
				ctrlChan, statusChan, _ = httpserver.Start(api.HTTPHandler(config.Domain, driver), httpConfig)

				ctrlChans = append(ctrlChans, ctrlChan)
				statusChans = append(statusChans, statusChan)

			}
		case Web:
			{
				var httpConfig = httpserver.Config{}
				httpConfig.Address = config.Address
				httpConfig.TLS = config.TLS
				httpConfig.CertFile = config.CertFile
				httpConfig.KeyFile = config.KeyFile

				httpConfig.Websocket = k.Websocket
				ctrlChan, statusChan, _ = httpserver.Start(web.HTTPHandler(), httpConfig)

				ctrlChans = append(ctrlChans, ctrlChan)
				statusChans = append(statusChans, statusChan)
			}
		default:
			{
				err := iodine.New(errors.New("Invalid API type"), nil)
				log.Fatal(err)
			}
		}
	}
	return
}

func getDriverChannels(driverType DriverType) (ctrlChans []chan<- string, statusChans []<-chan error, driver drivers.Driver) {
	// a pair of control channels, we use these primarily to add to the lists above
	var ctrlChan chan<- string
	var statusChan <-chan error

	// instantiate driver
	// preconditions:
	//    - driver type specified
	//    - any configuration for driver is populated
	// postconditions:
	//    - driver driver is initialized
	//    - ctrlChans has channel to communicate to driver
	//    - statusChans has channel for messages coming from driver
	switch {
	case driverType == Memory:
		{
			ctrlChan, statusChan, driver = memory.Start(1024 * 1024 * 1024)
			ctrlChans = append(ctrlChans, ctrlChan)
			statusChans = append(statusChans, statusChan)
		}
	case driverType == Donut:
		{
			u, err := user.Current()
			if err != nil {
				log.Error.Println(iodine.New(err, nil))
				return nil, nil, nil
			}
			root := path.Join(u.HomeDir, "minio-storage", "donut")
			ctrlChan, statusChan, driver = donut.Start(root)
			ctrlChans = append(ctrlChans, ctrlChan)
			statusChans = append(statusChans, statusChan)
		}
	default: // should never happen
		{
			log.Fatal(iodine.New(errors.New("No driver found"), nil))
		}
	}
	return
}

// Start - create channels
func Start(configs []Config) {
	// reflected looping is necessary to remove dead channels from loop and not flood switch
	ctrlChans, statusChans := getHTTPChannels(configs)
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
				msg := fmt.Sprintf("%q", value.Interface())
				log.Fatal(iodine.New(errors.New(msg), nil))
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
