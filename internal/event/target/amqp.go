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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio/internal/event"
	xnet "github.com/minio/pkg/net"
	"github.com/streadway/amqp"
)

// AMQPArgs - AMQP target arguments.
type AMQPArgs struct {
	Enable            bool     `json:"enable"`
	URL               xnet.URL `json:"url"`
	Exchange          string   `json:"exchange"`
	RoutingKey        string   `json:"routingKey"`
	ExchangeType      string   `json:"exchangeType"`
	DeliveryMode      uint8    `json:"deliveryMode"`
	Mandatory         bool     `json:"mandatory"`
	Immediate         bool     `json:"immediate"`
	Durable           bool     `json:"durable"`
	Internal          bool     `json:"internal"`
	NoWait            bool     `json:"noWait"`
	AutoDeleted       bool     `json:"autoDeleted"`
	PublisherConfirms bool     `json:"publisherConfirms"`
	QueueDir          string   `json:"queueDir"`
	QueueLimit        uint64   `json:"queueLimit"`
}

//lint:file-ignore ST1003 We cannot change these exported names.

// AMQP input constants.
const (
	AmqpQueueDir   = "queue_dir"
	AmqpQueueLimit = "queue_limit"

	AmqpURL               = "url"
	AmqpExchange          = "exchange"
	AmqpRoutingKey        = "routing_key"
	AmqpExchangeType      = "exchange_type"
	AmqpDeliveryMode      = "delivery_mode"
	AmqpMandatory         = "mandatory"
	AmqpImmediate         = "immediate"
	AmqpDurable           = "durable"
	AmqpInternal          = "internal"
	AmqpNoWait            = "no_wait"
	AmqpAutoDeleted       = "auto_deleted"
	AmqpArguments         = "arguments"
	AmqpPublisherConfirms = "publisher_confirms"

	EnvAMQPEnable            = "MINIO_NOTIFY_AMQP_ENABLE"
	EnvAMQPURL               = "MINIO_NOTIFY_AMQP_URL"
	EnvAMQPExchange          = "MINIO_NOTIFY_AMQP_EXCHANGE"
	EnvAMQPRoutingKey        = "MINIO_NOTIFY_AMQP_ROUTING_KEY"
	EnvAMQPExchangeType      = "MINIO_NOTIFY_AMQP_EXCHANGE_TYPE"
	EnvAMQPDeliveryMode      = "MINIO_NOTIFY_AMQP_DELIVERY_MODE"
	EnvAMQPMandatory         = "MINIO_NOTIFY_AMQP_MANDATORY"
	EnvAMQPImmediate         = "MINIO_NOTIFY_AMQP_IMMEDIATE"
	EnvAMQPDurable           = "MINIO_NOTIFY_AMQP_DURABLE"
	EnvAMQPInternal          = "MINIO_NOTIFY_AMQP_INTERNAL"
	EnvAMQPNoWait            = "MINIO_NOTIFY_AMQP_NO_WAIT"
	EnvAMQPAutoDeleted       = "MINIO_NOTIFY_AMQP_AUTO_DELETED"
	EnvAMQPArguments         = "MINIO_NOTIFY_AMQP_ARGUMENTS"
	EnvAMQPPublisherConfirms = "MINIO_NOTIFY_AMQP_PUBLISHING_CONFIRMS"
	EnvAMQPQueueDir          = "MINIO_NOTIFY_AMQP_QUEUE_DIR"
	EnvAMQPQueueLimit        = "MINIO_NOTIFY_AMQP_QUEUE_LIMIT"
)

// Validate AMQP arguments
func (a *AMQPArgs) Validate() error {
	if !a.Enable {
		return nil
	}
	if _, err := amqp.ParseURI(a.URL.String()); err != nil {
		return err
	}
	if a.QueueDir != "" {
		if !filepath.IsAbs(a.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}

	return nil
}

// AMQPTarget - AMQP target
type AMQPTarget struct {
	id         event.TargetID
	args       AMQPArgs
	conn       *amqp.Connection
	connMutex  sync.Mutex
	store      Store
	loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns TargetID.
func (target *AMQPTarget) ID() event.TargetID {
	return target.id
}

// IsActive - Return true if target is up and active
func (target *AMQPTarget) IsActive() (bool, error) {
	ch, _, err := target.channel()
	if err != nil {
		return false, err
	}
	defer func() {
		ch.Close()
	}()
	return true, nil
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *AMQPTarget) HasQueueStore() bool {
	return target.store != nil
}

func (target *AMQPTarget) channel() (*amqp.Channel, chan amqp.Confirmation, error) {
	var err error
	var conn *amqp.Connection
	var ch *amqp.Channel

	isAMQPClosedErr := func(err error) bool {
		if err == amqp.ErrClosed {
			return true
		}

		if nerr, ok := err.(*net.OpError); ok {
			return (nerr.Err.Error() == "use of closed network connection")
		}

		return false
	}

	target.connMutex.Lock()
	defer target.connMutex.Unlock()

	if target.conn != nil {
		ch, err = target.conn.Channel()
		if err == nil {
			if target.args.PublisherConfirms {
				confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
				if err := ch.Confirm(false); err != nil {
					ch.Close()
					return nil, nil, err
				}
				return ch, confirms, nil
			}
			return ch, nil, nil
		}

		if !isAMQPClosedErr(err) {
			return nil, nil, err
		}

		// close when we know this is a network error.
		target.conn.Close()
	}

	conn, err = amqp.Dial(target.args.URL.String())
	if err != nil {
		if IsConnRefusedErr(err) {
			return nil, nil, errNotConnected
		}
		return nil, nil, err
	}

	ch, err = conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	target.conn = conn

	if target.args.PublisherConfirms {
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return nil, nil, err
		}
		return ch, confirms, nil
	}

	return ch, nil, nil
}

// send - sends an event to the AMQP.
func (target *AMQPTarget) send(eventData event.Event, ch *amqp.Channel, confirms chan amqp.Confirmation) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	if err = ch.ExchangeDeclare(target.args.Exchange, target.args.ExchangeType, target.args.Durable,
		target.args.AutoDeleted, target.args.Internal, target.args.NoWait, nil); err != nil {
		return err
	}

	if err = ch.Publish(target.args.Exchange, target.args.RoutingKey, target.args.Mandatory,
		target.args.Immediate, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: target.args.DeliveryMode,
			Body:         data,
		}); err != nil {
		return err
	}

	// check for publisher confirms only if its enabled
	if target.args.PublisherConfirms {
		confirmed := <-confirms
		if !confirmed.Ack {
			return fmt.Errorf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		}
	}

	return nil
}

// Save - saves the events to the store which will be replayed when the amqp connection is active.
func (target *AMQPTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	ch, confirms, err := target.channel()
	if err != nil {
		return err
	}
	defer func() {
		cErr := ch.Close()
		target.loggerOnce(context.Background(), cErr, target.ID())
	}()

	return target.send(eventData, ch, confirms)
}

// Send - sends event to AMQP.
func (target *AMQPTarget) Send(eventKey string) error {
	ch, confirms, err := target.channel()
	if err != nil {
		return err
	}
	defer func() {
		cErr := ch.Close()
		target.loggerOnce(context.Background(), cErr, target.ID())
	}()

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	if err := target.send(eventData, ch, confirms); err != nil {
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - does nothing and available for interface compatibility.
func (target *AMQPTarget) Close() error {
	if target.conn != nil {
		return target.conn.Close()
	}
	return nil
}

// NewAMQPTarget - creates new AMQP target.
func NewAMQPTarget(id string, args AMQPArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{}), test bool) (*AMQPTarget, error) {
	var conn *amqp.Connection
	var err error

	var store Store

	target := &AMQPTarget{
		id:         event.TargetID{ID: id, Name: "amqp"},
		args:       args,
		loggerOnce: loggerOnce,
	}

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-amqp-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			target.loggerOnce(context.Background(), oErr, target.ID())
			return target, oErr
		}
		target.store = store
	}

	conn, err = amqp.Dial(args.URL.String())
	if err != nil {
		if store == nil || !(IsConnRefusedErr(err) || IsConnResetErr(err)) {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}
	target.conn = conn

	if target.store != nil && !test {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())

		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
	}

	return target, nil
}
