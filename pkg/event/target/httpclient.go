/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// HTTPClientTarget - HTTP client target.
type HTTPClientTarget struct {
	id        event.TargetID
	w         http.ResponseWriter
	eventCh   chan []byte
	DoneCh    chan struct{}
	stopCh    chan struct{}
	isStopped uint32
	isRunning uint32
}

// ID - returns target ID.
func (target HTTPClientTarget) ID() event.TargetID {
	return target.id
}

// IsActive - does nothing and available for interface compatibility.
func (target *HTTPClientTarget) IsActive() (bool, error) {
	return true, nil
}

// HasQueueStore - No-Op. Added for interface compatibility
func (target *HTTPClientTarget) HasQueueStore() bool {
	return false
}

func (target *HTTPClientTarget) start() {
	go func() {
		defer func() {
			atomic.AddUint32(&target.isRunning, 1)

			// Close DoneCh to indicate we are done.
			close(target.DoneCh)
		}()

		write := func(event []byte) error {
			if _, err := target.w.Write(event); err != nil {
				return err
			}

			target.w.(http.Flusher).Flush()
			return nil
		}

		keepAliveTicker := time.NewTicker(500 * time.Millisecond)
		defer keepAliveTicker.Stop()

		for {
			select {
			case <-target.stopCh:
				// We are asked to stop.
				return
			case event, ok := <-target.eventCh:
				if !ok {
					// Got read error.  Exit the goroutine.
					return
				}
				if err := write(event); err != nil {
					// Got write error to the client.  Exit the goroutine.
					return
				}
			case <-keepAliveTicker.C:
				if err := write([]byte(" ")); err != nil {
					// Got write error to the client.  Exit the goroutine.
					return
				}
			}
		}
	}()
}

// Save - sends event to HTTP client.
func (target *HTTPClientTarget) Save(eventData event.Event) error {
	return target.send(eventData)
}

func (target *HTTPClientTarget) send(eventData event.Event) error {
	if atomic.LoadUint32(&target.isRunning) != 0 {
		return errors.New("closed http connection")
	}

	data, err := json.Marshal(struct{ Records []event.Event }{[]event.Event{eventData}})
	if err != nil {
		return err
	}
	data = append(data, byte('\n'))

	select {
	case target.eventCh <- data:
		return nil
	case <-target.DoneCh:
		return errors.New("error in sending event")
	}
}

// Send - interface compatible method does no-op.
func (target *HTTPClientTarget) Send(eventKey string) error {
	return nil
}

// Close - closes underneath goroutine.
func (target *HTTPClientTarget) Close() error {
	atomic.AddUint32(&target.isStopped, 1)
	if atomic.LoadUint32(&target.isStopped) == 1 {
		close(target.stopCh)
	}

	return nil
}

func getNewUUID() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	return u.String(), nil
}

// NewHTTPClientTarget - creates new HTTP client target.
func NewHTTPClientTarget(host xnet.Host, w http.ResponseWriter) (*HTTPClientTarget, error) {
	uuid, err := getNewUUID()
	if err != nil {
		return nil, err
	}
	c := &HTTPClientTarget{
		id:      event.TargetID{ID: "httpclient" + "+" + uuid + "+" + host.Name, Name: host.Port.String()},
		w:       w,
		eventCh: make(chan []byte),
		DoneCh:  make(chan struct{}),
		stopCh:  make(chan struct{}),
	}
	c.start()
	return c, nil
}
