/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"net/url"

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

	return nil
}

// NSQTarget - NSQ target.
type NSQTarget struct {
	id       event.TargetID
	args     NSQArgs
	producer *nsq.Producer
}

// ID - returns target ID.
func (target *NSQTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to NSQD.
func (target *NSQTarget) Send(eventData event.Event) (err error) {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	err = target.producer.Publish(target.args.Topic, data)

	return err
}

// Close - closes underneath connections to NSQD server.
func (target *NSQTarget) Close() (err error) {
	// this blocks until complete:
	target.producer.Stop()
	return nil
}

// NewNSQTarget - creates new NSQ target.
func NewNSQTarget(id string, args NSQArgs) (*NSQTarget, error) {
	config := nsq.NewConfig()
	if args.TLS.Enable {
		config.TlsV1 = true
		config.TlsConfig = &tls.Config{
			InsecureSkipVerify: args.TLS.SkipVerify,
		}
	}
	producer, err := nsq.NewProducer(args.NSQDAddress.String(), config)

	if err != nil {
		return nil, err
	}

	return &NSQTarget{
		id:       event.TargetID{ID: id, Name: "nsq"},
		args:     args,
		producer: producer,
	}, nil
}
