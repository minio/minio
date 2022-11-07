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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/nsqio/go-nsq"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	xnet "github.com/minio/pkg/net"
)

// NSQ constants
const (
	NSQAddress       = "nsqd_address"
	NSQTopic         = "topic"
	NSQTLS           = "tls"
	NSQTLSSkipVerify = "tls_skip_verify"
	NSQQueueDir      = "queue_dir"
	NSQQueueLimit    = "queue_limit"

	EnvNSQEnable        = "MINIO_NOTIFY_NSQ_ENABLE"
	EnvNSQAddress       = "MINIO_NOTIFY_NSQ_NSQD_ADDRESS"
	EnvNSQTopic         = "MINIO_NOTIFY_NSQ_TOPIC"
	EnvNSQTLS           = "MINIO_NOTIFY_NSQ_TLS"
	EnvNSQTLSSkipVerify = "MINIO_NOTIFY_NSQ_TLS_SKIP_VERIFY"
	EnvNSQQueueDir      = "MINIO_NOTIFY_NSQ_QUEUE_DIR"
	EnvNSQQueueLimit    = "MINIO_NOTIFY_NSQ_QUEUE_LIMIT"
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

	return nil
}

// NSQTarget - NSQ target.
type NSQTarget struct {
	lazyInit lazyInit

	id         event.TargetID
	args       NSQArgs
	producer   *nsq.Producer
	store      Store
	config     *nsq.Config
	loggerOnce logger.LogOnce
	quitCh     chan struct{}
}

// ID - returns target ID.
func (target *NSQTarget) ID() event.TargetID {
	return target.id
}

// IsActive - Return true if target is up and active
func (target *NSQTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *NSQTarget) isActive() (bool, error) {
	if target.producer == nil {
		producer, err := nsq.NewProducer(target.args.NSQDAddress.String(), target.config)
		if err != nil {
			return false, err
		}
		target.producer = producer
	}

	if err := target.producer.Ping(); err != nil {
		// To treat "connection refused" errors as errNotConnected.
		if IsConnRefusedErr(err) {
			return false, errNotConnected
		}
		return false, err
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the nsq connection is active.
func (target *NSQTarget) Save(eventData event.Event) error {
	if err := target.init(); err != nil {
		return err
	}

	if target.store != nil {
		return target.store.Put(eventData)
	}
	_, err := target.isActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
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
	if err := target.init(); err != nil {
		return err
	}

	_, err := target.isActive()
	if err != nil {
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
	close(target.quitCh)
	if target.producer != nil {
		// this blocks until complete:
		target.producer.Stop()
	}
	return nil
}

func (target *NSQTarget) init() error {
	return target.lazyInit.Do(target.initNSQ)
}

func (target *NSQTarget) initNSQ() error {
	args := target.args

	config := nsq.NewConfig()
	if args.TLS.Enable {
		config.TlsV1 = true
		config.TlsConfig = &tls.Config{
			InsecureSkipVerify: args.TLS.SkipVerify,
		}
	}
	target.config = config

	producer, err := nsq.NewProducer(args.NSQDAddress.String(), config)
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}
	target.producer = producer

	err = target.producer.Ping()
	if err != nil {
		// To treat "connection refused" errors as errNotConnected.
		if !(IsConnRefusedErr(err) || IsConnResetErr(err)) {
			target.loggerOnce(context.Background(), err, target.ID().String())
		}
		target.producer.Stop()
		return err
	}

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return errNotConnected
	}

	return nil
}

// NewNSQTarget - creates new NSQ target.
func NewNSQTarget(id string, args NSQArgs, loggerOnce logger.LogOnce) (*NSQTarget, error) {
	var store Store
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-nsq-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if err := store.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of NSQ `%s`: %w", id, err)
		}
	}

	target := &NSQTarget{
		id:         event.TargetID{ID: id, Name: "nsq"},
		args:       args,
		loggerOnce: loggerOnce,
		store:      store,
		quitCh:     make(chan struct{}),
	}

	if target.store != nil {
		streamEventsFromStore(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}
