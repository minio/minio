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
	"net/url"
	"os"
	"path/filepath"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
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
	QueueDir     string    `json:"queueDir"`
	QueueLimit   uint64    `json:"queueLimit"`
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

// To obtain a nats connection from args.
func (n NATSArgs) connectNats() (*nats.Conn, error) {
	options := nats.DefaultOptions
	options.Url = "nats://" + n.Address.String()
	options.User = n.Username
	options.Password = n.Password
	options.Token = n.Token
	options.Secure = n.Secure
	return options.Connect()
}

// To obtain a streaming connection from args.
func (n NATSArgs) connectStan() (stan.Conn, error) {
	scheme := "nats"
	if n.Secure {
		scheme = "tls"
	}
	addressURL := scheme + "://" + n.Username + ":" + n.Password + "@" + n.Address.String()

	clientID, err := getNewUUID()
	if err != nil {
		return nil, err
	}

	connOpts := []stan.Option{stan.NatsURL(addressURL)}
	if n.Streaming.MaxPubAcksInflight > 0 {
		connOpts = append(connOpts, stan.MaxPubAcksInflight(n.Streaming.MaxPubAcksInflight))
	}

	return stan.Connect(n.Streaming.ClusterID, clientID, connOpts...)
}

// NATSTarget - NATS target.
type NATSTarget struct {
	id       event.TargetID
	args     NATSArgs
	natsConn *nats.Conn
	stanConn stan.Conn
	store    Store
}

// ID - returns target ID.
func (target *NATSTarget) ID() event.TargetID {
	return target.id
}

// Save - saves the events to the store which will be replayed when the Nats connection is active.
func (target *NATSTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	if target.args.Streaming.Enable {
		if !target.stanConn.NatsConn().IsConnected() {
			return errNotConnected
		}
	} else {
		if !target.natsConn.IsConnected() {
			return errNotConnected
		}
	}
	return target.send(eventData)
}

// send - sends an event to the Nats.
func (target *NATSTarget) send(eventData event.Event) error {
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

// Send - sends event to Nats.
func (target *NATSTarget) Send(eventKey string) error {
	var connErr error

	if target.args.Streaming.Enable {
		if target.stanConn == nil || target.stanConn.NatsConn() == nil {
			target.stanConn, connErr = target.args.connectStan()
		} else {
			if !target.stanConn.NatsConn().IsConnected() {
				return errNotConnected
			}
		}
	} else {
		if target.natsConn == nil {
			target.natsConn, connErr = target.args.connectNats()
		} else {
			if !target.natsConn.IsConnected() {
				return errNotConnected
			}
		}
	}

	if connErr != nil {
		if connErr.Error() == nats.ErrNoServers.Error() {
			return errNotConnected
		}
		return connErr
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

	return target.store.Del(eventKey)
}

// Close - closes underneath connections to NATS server.
func (target *NATSTarget) Close() (err error) {
	if target.stanConn != nil {
		// closing the streaming connection does not close the provided NATS connection.
		if target.stanConn.NatsConn() != nil {
			target.stanConn.NatsConn().Close()
		}
		err = target.stanConn.Close()
	}

	if target.natsConn != nil {
		target.natsConn.Close()
	}

	return err
}

// NewNATSTarget - creates new NATS target.
func NewNATSTarget(id string, args NATSArgs, doneCh <-chan struct{}) (*NATSTarget, error) {
	var natsConn *nats.Conn
	var stanConn stan.Conn

	var err error

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-nats-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			return nil, oErr
		}
	}

	if args.Streaming.Enable {
		stanConn, err = args.connectStan()
	} else {
		natsConn, err = args.connectNats()
	}

	if err != nil {
		if store == nil || err.Error() != nats.ErrNoServers.Error() {
			return nil, err
		}
	}

	target := &NATSTarget{
		id:       event.TargetID{ID: id, Name: "nats"},
		args:     args,
		stanConn: stanConn,
		natsConn: natsConn,
		store:    store,
	}

	if target.store != nil {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh)
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh)
	}

	return target, nil
}
