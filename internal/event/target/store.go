// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package target

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio/internal/event"
)

const retryInterval = 3 * time.Second

// errNotConnected - indicates that the target connection is not active.
var errNotConnected = errors.New("not connected to target server/service")

// errLimitExceeded error is sent when the maximum limit is reached.
var errLimitExceeded = errors.New("the maximum store limit reached")

// Store - To persist the events.
type Store interface {
	Put(event event.Event) error
	Get(key string) (event.Event, error)
	List() ([]string, error)
	Del(key string) error
	Open() error
}

// replayEvents - Reads the events from the store and replays.
func replayEvents(store Store, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), id event.TargetID) <-chan string {
	eventKeyCh := make(chan string)

	go func() {
		retryTicker := time.NewTicker(retryInterval)
		defer retryTicker.Stop()
		defer close(eventKeyCh)
		for {
			names, err := store.List()
			if err == nil {
				for _, name := range names {
					select {
					case eventKeyCh <- strings.TrimSuffix(name, eventExt):
						// Get next key.
					case <-doneCh:
						return
					}
				}
			}

			if len(names) < 2 {
				select {
				case <-retryTicker.C:
					if err != nil {
						loggerOnce(context.Background(),
							fmt.Errorf("store.List() failed '%w'", err), id)
					}
				case <-doneCh:
					return
				}
			}
		}
	}()

	return eventKeyCh
}

// IsConnRefusedErr - To check fot "connection refused" error.
func IsConnRefusedErr(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

// IsConnResetErr - Checks for connection reset errors.
func IsConnResetErr(err error) bool {
	if strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}
	// incase if error message is wrapped.
	return errors.Is(err, syscall.ECONNRESET)
}

// sendEvents - Reads events from the store and re-plays.
func sendEvents(target event.Target, eventKeyCh <-chan string, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{})) {
	retryTicker := time.NewTicker(retryInterval)
	defer retryTicker.Stop()

	send := func(eventKey string) bool {
		for {
			err := target.Send(eventKey)
			if err == nil {
				break
			}

			if err != errNotConnected && !IsConnResetErr(err) {
				loggerOnce(context.Background(),
					fmt.Errorf("target.Send() failed with '%w'", err),
					target.ID())
			}

			// Retrying after 3secs back-off

			select {
			case <-retryTicker.C:
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
