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
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/minio/minio/pkg/api"
	"github.com/minio/minio/pkg/api/web"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/server/httpserver"
	"github.com/minio/minio/pkg/storage/drivers/donut"
	fs "github.com/minio/minio/pkg/storage/drivers/fs"
	"github.com/minio/minio/pkg/storage/drivers/memory"
	"github.com/minio/minio/pkg/utils/log"
)

// MemoryFactory is used to build memory api server
type MemoryFactory struct {
	httpserver.Config
	MaxMemory  uint64
	Expiration time.Duration
}

// GetStartServerFunc builds memory api server
func (f MemoryFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		_, _, driver := memory.Start(f.MaxMemory, f.Expiration)
		conf := api.Config{RateLimit: f.RateLimit}
		conf.SetDriver(driver)
		ctrl, status, _ := httpserver.Start(api.HTTPHandler(conf), f.Config)
		return ctrl, status
	}
}

// FilesystemFactory is used to build filesystem api server
type FilesystemFactory struct {
	httpserver.Config
	Path string
}

// GetStartServerFunc builds memory api server
func (f FilesystemFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		_, _, driver := fs.Start(f.Path)
		conf := api.Config{RateLimit: f.RateLimit}
		conf.SetDriver(driver)
		ctrl, status, _ := httpserver.Start(api.HTTPHandler(conf), f.Config)
		return ctrl, status
	}
}

// WebFactory is used to build web cli server
type WebFactory struct {
	httpserver.Config
}

// GetStartServerFunc builds web cli server
func (f WebFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		ctrl, status, _ := httpserver.Start(web.HTTPHandler(), f.Config)
		return ctrl, status
	}
}

// DonutFactory is used to build donut api server
type DonutFactory struct {
	httpserver.Config
	Paths []string
}

// GetStartServerFunc DonutFactory builds donut api server
func (f DonutFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		_, _, driver := donut.Start(f.Paths)
		conf := api.Config{RateLimit: f.RateLimit}
		conf.SetDriver(driver)
		ctrl, status, _ := httpserver.Start(api.HTTPHandler(conf), f.Config)
		return ctrl, status
	}
}

// StartServerFunc describes a function that can be used to start a server with StartMinio
type StartServerFunc func() (chan<- string, <-chan error)

// StartMinio starts minio server
func StartMinio(servers []StartServerFunc) {
	var ctrlChannels []chan<- string
	var errChannels []<-chan error
	for _, server := range servers {
		ctrlChannel, errChannel := server()
		ctrlChannels = append(ctrlChannels, ctrlChannel)
		errChannels = append(errChannels, errChannel)
	}
	cases := createSelectCases(errChannels)
	for len(cases) > 0 {
		chosen, value, recvOk := reflect.Select(cases)
		switch recvOk {
		case true:
			// Status Message Received
			switch true {
			case value.Interface() != nil:
				// For any error received cleanup all existing channels and fail
				for _, ch := range ctrlChannels {
					close(ch)
				}
				msg := fmt.Sprintf("%q", value.Interface())
				log.Fatal(iodine.New(errors.New(msg), nil))
			}
		case false:
			// Channel closed, remove from list
			var aliveStatusChans []<-chan error
			for i, ch := range errChannels {
				if i != chosen {
					aliveStatusChans = append(aliveStatusChans, ch)
				}
			}
			// create new select cases without defunct channel
			errChannels = aliveStatusChans
			cases = createSelectCases(errChannels)
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
