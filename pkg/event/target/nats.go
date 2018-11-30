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
	"encoding/json"
	"errors"
	"net/url"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats"
)

// NATSArgs - NATS target arguments.
type NATSArgs struct {
	Enable       bool      `json:"enable"`
	Address      xnet.Host `json:"address"`
	Subject      string    `json:"subject"`
	Username     string    `json:"username"`
	Password     string    `json:"password"`
	Token        string    `json:"token"`
	Secure       bool      `json:"secure"`
	PingInterval int64     `json:"pingInterval"`
	Streaming    struct {
		Enable             bool   `json:"enable"`
		ClusterID          string `json:"clusterID"`
		Async              bool   `json:"async"`
		MaxPubAcksInflight int    `json:"maxPubAcksInflight"`
	} `json:"streaming"`
}

// Validate NATSArgs fields
func (n NATSArgs) Validate() error {
	if !n.Enable {
		return nil
	}

	if n.Address.IsEmpty() {
		return errors.New("empty address")
	}

	if n.Subject == "" {
		return errors.New("empty subject")
	}

	if n.Streaming.Enable {
		if n.Streaming.ClusterID == "" {
			return errors.New("empty cluster id")
		}
	}

	return nil
}

// NATSTarget - NATS target.
type NATSTarget struct {
	id       event.TargetID
	args     NATSArgs
	natsConn *nats.Conn
	stanConn stan.Conn
}

// ID - returns target ID.
func (target *NATSTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to NATS.
func (target *NATSTarget) Send(eventData event.Event) (err error) {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	if target.stanConn != nil {
		if target.args.Streaming.Async {
			_, err = target.stanConn.PublishAsync(target.args.Subject, data, nil)
		} else {
			err = target.stanConn.Publish(target.args.Subject, data)
		}
	} else {
		err = target.natsConn.Publish(target.args.Subject, data)
	}

	return err
}

// Close - closes underneath connections to NATS server.
func (target *NATSTarget) Close() (err error) {
	if target.stanConn != nil {
		err = target.stanConn.Close()
	}

	if target.natsConn != nil {
		target.natsConn.Close()
	}

	return err
}

// NewNATSTarget - creates new NATS target.
func NewNATSTarget(id string, args NATSArgs) (*NATSTarget, error) {
	var natsConn *nats.Conn
	var stanConn stan.Conn
	var clientID string
	var err error

	if args.Streaming.Enable {
		scheme := "nats"
		if args.Secure {
			scheme = "tls"
		}
		addressURL := scheme + "://" + args.Username + ":" + args.Password + "@" + args.Address.String()

		clientID, err = getNewUUID()
		if err != nil {
			return nil, err
		}

		connOpts := []stan.Option{stan.NatsURL(addressURL)}
		if args.Streaming.MaxPubAcksInflight > 0 {
			connOpts = append(connOpts, stan.MaxPubAcksInflight(args.Streaming.MaxPubAcksInflight))
		}

		stanConn, err = stan.Connect(args.Streaming.ClusterID, clientID, connOpts...)
	} else {
		options := nats.DefaultOptions
		options.Url = "nats://" + args.Address.String()
		options.User = args.Username
		options.Password = args.Password
		options.Token = args.Token
		options.Secure = args.Secure
		natsConn, err = options.Connect()
	}
	if err != nil {
		return nil, err
	}

	return &NATSTarget{
		id:       event.TargetID{ID: id, Name: "nats"},
		args:     args,
		stanConn: stanConn,
		natsConn: natsConn,
	}, nil
}
