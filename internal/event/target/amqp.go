// Copyright (c) 2015-2023 MinIO, Inc.
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
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/rabbitmq/amqp091-go"
)

// AMQPArgs - AMQP target arguments.
type AMQPArgs struct {
	Enable            bool        `json:"enable"`
	URL               amqp091.URI `json:"url"`
	Exchange          string      `json:"exchange"`
	RoutingKey        string      `json:"routingKey"`
	ExchangeType      string      `json:"exchangeType"`
	DeliveryMode      uint8       `json:"deliveryMode"`
	Mandatory         bool        `json:"mandatory"`
	Immediate         bool        `json:"immediate"`
	Durable           bool        `json:"durable"`
	Internal          bool        `json:"internal"`
	NoWait            bool        `json:"noWait"`
	AutoDeleted       bool        `json:"autoDeleted"`
	PublisherConfirms bool        `json:"publisherConfirms"`
	QueueDir          string      `json:"queueDir"`
	QueueLimit        uint64      `json:"queueLimit"`
}

// AMQP input constants.
//
// ST1003 We cannot change these exported names.
//
//nolint:staticcheck
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
	if _, err := amqp091.ParseURI(a.URL.String()); err != nil {
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
	initOnce once.Init

	id         event.TargetID
	args       AMQPArgs
	conn       *amqp091.Connection
	connMutex  sync.Mutex
	store      store.Store[event.Event]
	loggerOnce logger.LogOnce

	quitCh chan struct{}
}

// ID - returns TargetID.
func (target *AMQPTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *AMQPTarget) Name() string {
	return target.ID().String()
}

// Store returns any underlying store if set.
func (target *AMQPTarget) Store() event.TargetStore {
	return target.store
}

// IsActive - Return true if target is up and active
func (target *AMQPTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}

	return target.isActive()
}

func (target *AMQPTarget) isActive() (bool, error) {
	ch, _, err := target.channel()
	if err != nil {
		return false, err
	}
	defer func() {
		ch.Close()
	}()
	return true, nil
}

func (target *AMQPTarget) channel() (*amqp091.Channel, chan amqp091.Confirmation, error) {
	var err error
	var conn *amqp091.Connection
	var ch *amqp091.Channel

	isAMQPClosedErr := func(err error) bool {
		if err == amqp091.ErrClosed {
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
				confirms := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))
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

	conn, err = amqp091.Dial(target.args.URL.String())
	if err != nil {
		if xnet.IsConnRefusedErr(err) {
			return nil, nil, store.ErrNotConnected
		}
		return nil, nil, err
	}

	ch, err = conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	target.conn = conn

	if target.args.PublisherConfirms {
		confirms := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return nil, nil, err
		}
		return ch, confirms, nil
	}

	return ch, nil, nil
}

// send - sends an event to the AMQP091.
func (target *AMQPTarget) send(eventData event.Event, ch *amqp091.Channel, confirms chan amqp091.Confirmation) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	headers := make(amqp091.Table)
	// Add more information here as required, but be aware to not overload headers
	headers["minio-bucket"] = eventData.S3.Bucket.Name
	headers["minio-event"] = eventData.EventName.String()

	if err = ch.ExchangeDeclare(target.args.Exchange, target.args.ExchangeType, target.args.Durable,
		target.args.AutoDeleted, target.args.Internal, target.args.NoWait, nil); err != nil {
		return err
	}

	if err = ch.Publish(target.args.Exchange, target.args.RoutingKey, target.args.Mandatory,
		target.args.Immediate, amqp091.Publishing{
			Headers:      headers,
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
		_, err := target.store.Put(eventData)
		return err
	}
	if err := target.init(); err != nil {
		return err
	}
	ch, confirms, err := target.channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return target.send(eventData, ch, confirms)
}

// SendFromStore - reads an event from store and sends it to AMQP091.
func (target *AMQPTarget) SendFromStore(key store.Key) error {
	if err := target.init(); err != nil {
		return err
	}

	ch, confirms, err := target.channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	eventData, eErr := target.store.Get(key)
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
	return target.store.Del(key)
}

// Close - does nothing and available for interface compatibility.
func (target *AMQPTarget) Close() error {
	close(target.quitCh)
	if target.conn != nil {
		return target.conn.Close()
	}
	return nil
}

func (target *AMQPTarget) init() error {
	return target.initOnce.Do(target.initAMQP)
}

func (target *AMQPTarget) initAMQP() error {
	conn, err := amqp091.Dial(target.args.URL.String())
	if err != nil {
		if xnet.IsConnRefusedErr(err) || xnet.IsConnResetErr(err) {
			target.loggerOnce(context.Background(), err, target.ID().String())
		}
		return err
	}
	target.conn = conn

	return nil
}

// NewAMQPTarget - creates new AMQP target.
func NewAMQPTarget(id string, args AMQPArgs, loggerOnce logger.LogOnce) (*AMQPTarget, error) {
	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-amqp-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of AMQP `%s`: %w", id, err)
		}
	}

	target := &AMQPTarget{
		id:         event.TargetID{ID: id, Name: "amqp"},
		args:       args,
		loggerOnce: loggerOnce,
		store:      queueStore,
		quitCh:     make(chan struct{}),
	}

	if target.store != nil {
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}
