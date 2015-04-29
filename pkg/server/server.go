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
	"github.com/minio-io/minio/pkg/api"
	"github.com/minio-io/minio/pkg/api/web"
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/server/httpserver"
	"github.com/minio-io/minio/pkg/storage/drivers/donut"
	"github.com/minio-io/minio/pkg/storage/drivers/memory"
	"github.com/minio-io/minio/pkg/utils/log"
	"reflect"
	"time"
)

// MemoryFactory is used to build memory api servers
type MemoryFactory struct {
	httpserver.Config
	MaxMemory uint64
}

// GetStartServerFunc builds memory api servers
func (f MemoryFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		_, _, driver := memory.Start(f.MaxMemory, 1*time.Hour)
		ctrl, status, _ := httpserver.Start(api.HTTPHandler(f.Domain, driver), f.Config)
		return ctrl, status
	}
}

// WebFactory is used to build web cli servers
type WebFactory struct {
	httpserver.Config
}

// GetStartServerFunc builds web cli servers
func (f WebFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		ctrl, status, _ := httpserver.Start(web.HTTPHandler(), f.Config)
		return ctrl, status
	}
}

// DonutFactory is used to build donut api servers
type DonutFactory struct {
	httpserver.Config
	Paths []string
}

// GetStartServerFunc DonutFactory builds donut api servers
func (f DonutFactory) GetStartServerFunc() StartServerFunc {
	return func() (chan<- string, <-chan error) {
		_, _, driver := donut.Start(f.Paths)
		ctrl, status, _ := httpserver.Start(api.HTTPHandler(f.Domain, driver), f.Config)
		return ctrl, status
	}
}

// StartServerFunc describes a function that can be used to start a server with StartMinio
type StartServerFunc func() (chan<- string, <-chan error)

// StartMinio starts minio servers
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
