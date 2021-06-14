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
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"

	"github.com/minio/minio/internal/event"
	xnet "github.com/minio/pkg/net"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// NATS related constants
const (
	NATSAddress       = "address"
	NATSSubject       = "subject"
	NATSUsername      = "username"
	NATSPassword      = "password"
	NATSToken         = "token"
	NATSTLS           = "tls"
	NATSTLSSkipVerify = "tls_skip_verify"
	NATSPingInterval  = "ping_interval"
	NATSQueueDir      = "queue_dir"
	NATSQueueLimit    = "queue_limit"
	NATSCertAuthority = "cert_authority"
	NATSClientCert    = "client_cert"
	NATSClientKey     = "client_key"

	// Streaming constants
	NATSStreaming                   = "streaming"
	NATSStreamingClusterID          = "streaming_cluster_id"
	NATSStreamingAsync              = "streaming_async"
	NATSStreamingMaxPubAcksInFlight = "streaming_max_pub_acks_in_flight"

	EnvNATSEnable        = "MINIO_NOTIFY_NATS_ENABLE"
	EnvNATSAddress       = "MINIO_NOTIFY_NATS_ADDRESS"
	EnvNATSSubject       = "MINIO_NOTIFY_NATS_SUBJECT"
	EnvNATSUsername      = "MINIO_NOTIFY_NATS_USERNAME"
	EnvNATSPassword      = "MINIO_NOTIFY_NATS_PASSWORD"
	EnvNATSToken         = "MINIO_NOTIFY_NATS_TOKEN"
	EnvNATSTLS           = "MINIO_NOTIFY_NATS_TLS"
	EnvNATSTLSSkipVerify = "MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY"
	EnvNATSPingInterval  = "MINIO_NOTIFY_NATS_PING_INTERVAL"
	EnvNATSQueueDir      = "MINIO_NOTIFY_NATS_QUEUE_DIR"
	EnvNATSQueueLimit    = "MINIO_NOTIFY_NATS_QUEUE_LIMIT"
	EnvNATSCertAuthority = "MINIO_NOTIFY_NATS_CERT_AUTHORITY"
	EnvNATSClientCert    = "MINIO_NOTIFY_NATS_CLIENT_CERT"
	EnvNATSClientKey     = "MINIO_NOTIFY_NATS_CLIENT_KEY"

	// Streaming constants
	EnvNATSStreaming                   = "MINIO_NOTIFY_NATS_STREAMING"
	EnvNATSStreamingClusterID          = "MINIO_NOTIFY_NATS_STREAMING_CLUSTER_ID"
	EnvNATSStreamingAsync              = "MINIO_NOTIFY_NATS_STREAMING_ASYNC"
	EnvNATSStreamingMaxPubAcksInFlight = "MINIO_NOTIFY_NATS_STREAMING_MAX_PUB_ACKS_IN_FLIGHT"
)

// NATSArgs - NATS target arguments.
type NATSArgs struct {
	Enable        bool      `json:"enable"`
	Address       xnet.Host `json:"address"`
	Subject       string    `json:"subject"`
	Username      string    `json:"username"`
	Password      string    `json:"password"`
	Token         string    `json:"token"`
	TLS           bool      `json:"tls"`
	TLSSkipVerify bool      `json:"tlsSkipVerify"`
	Secure        bool      `json:"secure"`
	CertAuthority string    `json:"certAuthority"`
	ClientCert    string    `json:"clientCert"`
	ClientKey     string    `json:"clientKey"`
	PingInterval  int64     `json:"pingInterval"`
	QueueDir      string    `json:"queueDir"`
	QueueLimit    uint64    `json:"queueLimit"`
	Streaming     struct {
		Enable             bool   `json:"enable"`
		ClusterID          string `json:"clusterID"`
		Async              bool   `json:"async"`
		MaxPubAcksInflight int    `json:"maxPubAcksInflight"`
	} `json:"streaming"`

	RootCAs *x509.CertPool `json:"-"`
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

	if n.ClientCert != "" && n.ClientKey == "" || n.ClientCert == "" && n.ClientKey != "" {
		return errors.New("cert and key must be specified as a pair")
	}

	if n.Username != "" && n.Password == "" || n.Username == "" && n.Password != "" {
		return errors.New("username and password must be specified as a pair")
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

	return nil
}

// To obtain a nats connection from args.
func (n NATSArgs) connectNats() (*nats.Conn, error) {
	connOpts := []nats.Option{nats.Name("Minio Notification")}
	if n.Username != "" && n.Password != "" {
		connOpts = append(connOpts, nats.UserInfo(n.Username, n.Password))
	}
	if n.Token != "" {
		connOpts = append(connOpts, nats.Token(n.Token))
	}
	if n.Secure || n.TLS && n.TLSSkipVerify {
		connOpts = append(connOpts, nats.Secure(nil))
	} else if n.TLS {
		connOpts = append(connOpts, nats.Secure(&tls.Config{RootCAs: n.RootCAs}))
	}
	if n.CertAuthority != "" {
		connOpts = append(connOpts, nats.RootCAs(n.CertAuthority))
	}
	if n.ClientCert != "" && n.ClientKey != "" {
		connOpts = append(connOpts, nats.ClientCert(n.ClientCert, n.ClientKey))
	}
	return nats.Connect(n.Address.String(), connOpts...)
}

// To obtain a streaming connection from args.
func (n NATSArgs) connectStan() (stan.Conn, error) {
	scheme := "nats"
	if n.Secure {
		scheme = "tls"
	}

	var addressURL string
	if n.Username != "" && n.Password != "" {
		addressURL = scheme + "://" + n.Username + ":" + n.Password + "@" + n.Address.String()
	} else if n.Token != "" {
		addressURL = scheme + "://" + n.Token + "@" + n.Address.String()
	} else {
		addressURL = scheme + "://" + n.Address.String()
	}

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
	id         event.TargetID
	args       NATSArgs
	natsConn   *nats.Conn
	stanConn   stan.Conn
	store      Store
	loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns target ID.
func (target *NATSTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *NATSTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *NATSTarget) IsActive() (bool, error) {
	var connErr error
	if target.args.Streaming.Enable {
		if target.stanConn == nil || target.stanConn.NatsConn() == nil {
			target.stanConn, connErr = target.args.connectStan()
		} else {
			if !target.stanConn.NatsConn().IsConnected() {
				return false, errNotConnected
			}
		}
	} else {
		if target.natsConn == nil {
			target.natsConn, connErr = target.args.connectNats()
		} else {
			if !target.natsConn.IsConnected() {
				return false, errNotConnected
			}
		}
	}

	if connErr != nil {
		if connErr.Error() == nats.ErrNoServers.Error() {
			return false, errNotConnected
		}
		return false, connErr
	}

	return true, nil
}

// Save - saves the events to the store which will be replayed when the Nats connection is active.
func (target *NATSTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	_, err := target.IsActive()
	if err != nil {
		return err
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
	_, err := target.IsActive()
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
func NewNATSTarget(id string, args NATSArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*NATSTarget, error) {
	var natsConn *nats.Conn
	var stanConn stan.Conn

	var err error

	var store Store

	target := &NATSTarget{
		id:         event.TargetID{ID: id, Name: "nats"},
		args:       args,
		loggerOnce: loggerOnce,
	}

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-nats-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			target.loggerOnce(context.Background(), oErr, target.ID())
			return target, oErr
		}
		target.store = store
	}

	if args.Streaming.Enable {
		stanConn, err = args.connectStan()
		target.stanConn = stanConn
	} else {
		natsConn, err = args.connectNats()
		target.natsConn = natsConn
	}

	if err != nil {
		if store == nil || err.Error() != nats.ErrNoServers.Error() {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}

	if target.store != nil && !test {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
	}

	return target, nil
}
