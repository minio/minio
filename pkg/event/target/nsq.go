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
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/nsqio/go-nsq"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// NSQArgs - NSQ target arguments.
type NSQArgs struct {
	Enable      bool      `json:"enable"`
	NSQDAddress xnet.Host `json:"nsqdAddress"`
	Topic       string    `json:"topic"`
	TLS         struct {
		Enable     bool `json:"enable"`
		SkipVerify bool `json:"skipVerify"`
	} `json:"tls"`
	QueueDir   string `json:"queueDir"`
	QueueLimit uint64 `json:"queueLimit"`
}

// Validate NSQArgs fields
func (n NSQArgs) Validate() error {
	if !n.Enable {
		return nil
	}

	if n.NSQDAddress.IsEmpty() {
		return errors.New("empty nsqdAddress")
	}

	if n.Topic == "" {
		return errors.New("empty topic")
	}
	if n.QueueDir != "" {
		if !filepath.IsAbs(n.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}
	if n.QueueLimit > 10000 {
		return errors.New("queueLimit should not exceed 10000")
	}

	return nil
}

// NSQTarget - NSQ target.
type NSQTarget struct {
	id       event.TargetID
	args     NSQArgs
	producer *nsq.Producer
	store    Store
}

// ID - returns target ID.
func (target *NSQTarget) ID() event.TargetID {
	return target.id
}

// Save - saves the events to the store which will be replayed when the nsq connection is active.
func (target *NSQTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	if err := target.producer.Ping(); err != nil {
		// To treat "connection refused" errors as errNotConnected.
		if isConnRefusedErr(err) {
			return errNotConnected
		}
		return err
	}
	return target.send(eventData)
}

// isConnRefusedErr - To check fot "connection refused" error.
func isConnRefusedErr(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if sysErr, ok := opErr.Err.(*os.SyscallError); ok {
			if errno, ok := sysErr.Err.(syscall.Errno); ok {
				if errno == syscall.ECONNREFUSED {
					return true
				}
			}
		}
	}
	return false
}

// send - sends an event to the NSQ.
func (target *NSQTarget) send(eventData event.Event) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	return target.producer.Publish(target.args.Topic, data)
}

// Send - reads an event from store and sends it to NSQ.
func (target *NSQTarget) Send(eventKey string) error {

	if err := target.producer.Ping(); err != nil {
		// To treat "connection refused" errors as errNotConnected.
		if isConnRefusedErr(err) {
			return errNotConnected
		}
		return err
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	if err := target.send(eventData); err != nil {
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - closes underneath connections to NSQD server.
func (target *NSQTarget) Close() (err error) {
	// this blocks until complete:
	target.producer.Stop()
	return nil
}

// NewNSQTarget - creates new NSQ target.
func NewNSQTarget(id string, args NSQArgs, doneCh <-chan struct{}) (*NSQTarget, error) {
	config := nsq.NewConfig()
	if args.TLS.Enable {
		config.TlsV1 = true
		config.TlsConfig = &tls.Config{
			InsecureSkipVerify: args.TLS.SkipVerify,
		}
	}

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-nsq-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			return nil, oErr
		}
	}

	producer, err := nsq.NewProducer(args.NSQDAddress.String(), config)
	if err != nil {
		return nil, err
	}

	target := &NSQTarget{
		id:       event.TargetID{ID: id, Name: "nsq"},
		args:     args,
		producer: producer,
		store:    store,
	}

	if err := target.producer.Ping(); err != nil {
		// To treat "connection refused" errors as errNotConnected.
		if target.store == nil || !isConnRefusedErr(err) {
			return nil, err
		}
	}

	if target.store != nil {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh)
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh)
	}

	return target, nil
}
