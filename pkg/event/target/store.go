/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package target

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio/pkg/event"
)

const retryInterval = 3 * time.Second

// errNotConnected - indicates that the target connection is not active.
var errNotConnected = errors.New("not connected to target server/service")

// errLimitExceeded error is sent when the maximum limit is reached.
var errLimitExceeded = errors.New("the maximum store limit reached")

// errNoSuchKey error is sent in Get when the key is not found.
var errNoSuchKey = errors.New("no such key found in store")

// Store - To persist the events.
type Store interface {
	Put(event event.Event) error
	Get(key string) (event.Event, error)
	ListN(n int) []string
	Del(key string) error
	Open() error
}

// replayEvents - Reads the events from the store and replays.
func replayEvents(store Store, doneCh <-chan struct{}) <-chan string {
	var names []string
	eventKeyCh := make(chan string)

	go func() {
		retryTimer := time.NewTimer(retryInterval)
		defer retryTimer.Stop()
		defer close(eventKeyCh)
		for {
			names = store.ListN(100)
			for _, name := range names {
				select {
				case eventKeyCh <- strings.TrimSuffix(name, eventExt):
					// Get next key.
				case <-doneCh:
					return
				}
			}

			if len(names) < 2 {
				retryTimer.Reset(retryInterval)
				select {
				case <-retryTimer.C:
				case <-doneCh:
					return
				}
			}
		}
	}()

	return eventKeyCh
}

// sendEvents - Reads events from the store and re-plays.
func sendEvents(target event.Target, eventKeyCh <-chan string, doneCh <-chan struct{}) {
	retryTimer := time.NewTimer(retryInterval)
	defer retryTimer.Stop()

	send := func(eventKey string) bool {
		for {
			err := target.Send(eventKey)
			if err == nil {
				break
			}

			if err != errNotConnected {
				panic(fmt.Errorf("target.Send() failed with '%v'", err))
			}

			retryTimer.Reset(retryInterval)
			select {
			case <-retryTimer.C:
			case <-doneCh:
				return false
			}
		}
		return true
	}

	for {
		select {
		case eventKey, ok := <-eventKeyCh:
			if !ok {
				// closed channel.
				return
			}

			if !send(eventKey) {
				return
			}
		case <-doneCh:
			return
		}
	}
}
