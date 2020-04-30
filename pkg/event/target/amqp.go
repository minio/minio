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
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/streadway/amqp"
)

// AMQPArgs - AMQP target arguments.
type AMQPArgs struct {
	Enable       bool     `json:"enable"`
	URL          xnet.URL `json:"url"`
	Exchange     string   `json:"exchange"`
	RoutingKey   string   `json:"routingKey"`
	ExchangeType string   `json:"exchangeType"`
	DeliveryMode uint8    `json:"deliveryMode"`
	Mandatory    bool     `json:"mandatory"`
	Immediate    bool     `json:"immediate"`
	Durable      bool     `json:"durable"`
	Internal     bool     `json:"internal"`
	NoWait       bool     `json:"noWait"`
	AutoDeleted  bool     `json:"autoDeleted"`
	QueueDir     string   `json:"queueDir"`
	QueueLimit   uint64   `json:"queueLimit"`
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
	AmqpPublishingHeaders = "publishing_headers"

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
	EnvAMQPPublishingHeaders = "MINIO_NOTIFY_AMQP_PUBLISHING_HEADERS"
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
	ch, err := target.channel()
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

func (target *AMQPTarget) channel() (*amqp.Channel, error) {
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
			return ch, nil
		}

		if !isAMQPClosedErr(err) {
			return nil, err
		}
	}

	conn, err = amqp.Dial(target.args.URL.String())
	if err != nil {
		if IsConnRefusedErr(err) {
			return nil, errNotConnected
		}
		return nil, err
	}

	ch, err = conn.Channel()
	if err != nil {
		return nil, err
	}

	target.conn = conn

	return ch, nil
}

// send - sends an event to the AMQP.
func (target *AMQPTarget) send(eventData event.Event, ch *amqp.Channel) error {
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

	if err := ch.Publish(target.args.Exchange, target.args.RoutingKey, target.args.Mandatory,
		target.args.Immediate, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: target.args.DeliveryMode,
			Body:         data,
		}); err != nil {
		return err
	}

	return nil
}

// Save - saves the events to the store which will be replayed when the amqp connection is active.
func (target *AMQPTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	ch, err := target.channel()
	if err != nil {
		return err
	}
	defer func() {
		cErr := ch.Close()
		target.loggerOnce(context.Background(), cErr, target.ID())
	}()

	return target.send(eventData, ch)
}

// Send - sends event to AMQP.
func (target *AMQPTarget) Send(eventKey string) error {
	ch, err := target.channel()
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

	if err := target.send(eventData, ch); err != nil {
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
