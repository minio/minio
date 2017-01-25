// Copyright 2016 Apcera Inc. All rights reserved.

// Package stan is a Go client for the NATS Streaming messaging system (https://nats.io).
package stan

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nuid"
)

// Version is the NATS Streaming Go Client version
const Version = "0.3.4"

const (
	// DefaultNatsURL is the default URL the client connects to
	DefaultNatsURL = "nats://localhost:4222"
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWait = 2 * time.Second
	// DefaultDiscoverPrefix is the prefix subject used to connect to the NATS Streaming server
	DefaultDiscoverPrefix = "_STAN.discover"
	// DefaultACKPrefix is the prefix subject used to send ACKs to the NATS Streaming server
	DefaultACKPrefix = "_STAN.acks"
	// DefaultMaxPubAcksInflight is the default maximum number of published messages
	// without outstanding ACKs from the server
	DefaultMaxPubAcksInflight = 16384
)

// Conn represents a connection to the NATS Streaming subsystem. It can Publish and
// Subscribe to messages within the NATS Streaming cluster.
type Conn interface {
	// Publish
	Publish(subject string, data []byte) error
	PublishAsync(subject string, data []byte, ah AckHandler) (string, error)

	// Subscribe
	Subscribe(subject string, cb MsgHandler, opts ...SubscriptionOption) (Subscription, error)

	// QueueSubscribe
	QueueSubscribe(subject, qgroup string, cb MsgHandler, opts ...SubscriptionOption) (Subscription, error)

	// Close
	Close() error

	// NatsConn returns the underlying NATS conn. Use this with care. For
	// example, closing the wrapped NATS conn will put the NATS Streaming Conn
	// in an invalid state.
	NatsConn() *nats.Conn
}

// Errors
var (
	ErrConnectReqTimeout = errors.New("stan: connect request timeout")
	ErrCloseReqTimeout   = errors.New("stan: close request timeout")
	ErrSubReqTimeout     = errors.New("stan: subscribe request timeout")
	ErrUnsubReqTimeout   = errors.New("stan: unsubscribe request timeout")
	ErrConnectionClosed  = errors.New("stan: connection closed")
	ErrTimeout           = errors.New("stan: publish ack timeout")
	ErrBadAck            = errors.New("stan: malformed ack")
	ErrBadSubscription   = errors.New("stan: invalid subscription")
	ErrBadConnection     = errors.New("stan: invalid connection")
	ErrManualAck         = errors.New("stan: cannot manually ack in auto-ack mode")
	ErrNilMsg            = errors.New("stan: nil message")
	ErrNoServerSupport   = errors.New("stan: not supported by server")
)

// AckHandler is used for Async Publishing to provide status of the ack.
// The func will be passed teh GUID and any error state. No error means the
// message was successfully received by NATS Streaming.
type AckHandler func(string, error)

// Options can be used to a create a customized connection.
type Options struct {
	NatsURL            string
	NatsConn           *nats.Conn
	ConnectTimeout     time.Duration
	AckTimeout         time.Duration
	DiscoverPrefix     string
	MaxPubAcksInflight int
}

// DefaultOptions are the NATS Streaming client's default options
var DefaultOptions = Options{
	NatsURL:            DefaultNatsURL,
	ConnectTimeout:     DefaultConnectWait,
	AckTimeout:         DefaultAckWait,
	DiscoverPrefix:     DefaultDiscoverPrefix,
	MaxPubAcksInflight: DefaultMaxPubAcksInflight,
}

// Option is a function on the options for a connection.
type Option func(*Options) error

// NatsURL is an Option to set the URL the client should connect to.
func NatsURL(u string) Option {
	return func(o *Options) error {
		o.NatsURL = u
		return nil
	}
}

// ConnectWait is an Option to set the timeout for establishing a connection.
func ConnectWait(t time.Duration) Option {
	return func(o *Options) error {
		o.ConnectTimeout = t
		return nil
	}
}

// PubAckWait is an Option to set the timeout for waiting for an ACK for a
// published message.
func PubAckWait(t time.Duration) Option {
	return func(o *Options) error {
		o.AckTimeout = t
		return nil
	}
}

// MaxPubAcksInflight is an Option to set the maximum number of published
// messages without outstanding ACKs from the server.
func MaxPubAcksInflight(max int) Option {
	return func(o *Options) error {
		o.MaxPubAcksInflight = max
		return nil
	}
}

// NatsConn is an Option to set the underlying NATS connection to be used
// by a NATS Streaming Conn object.
func NatsConn(nc *nats.Conn) Option {
	return func(o *Options) error {
		o.NatsConn = nc
		return nil
	}
}

// A conn represents a bare connection to a stan cluster.
type conn struct {
	sync.RWMutex
	clientID         string
	serverID         string
	pubPrefix        string // Publish prefix set by stan, append our subject.
	subRequests      string // Subject to send subscription requests.
	unsubRequests    string // Subject to send unsubscribe requests.
	subCloseRequests string // Subject to send subscription close requests.
	closeRequests    string // Subject to send close requests.
	ackSubject       string // publish acks
	ackSubscription  *nats.Subscription
	hbSubscription   *nats.Subscription
	subMap           map[string]*subscription
	pubAckMap        map[string]*ack
	pubAckChan       chan (struct{})
	opts             Options
	nc               *nats.Conn
	ncOwned          bool // NATS Streaming created the connection, so needs to close it.
}

// Closure for ack contexts.
type ack struct {
	t  *time.Timer
	ah AckHandler
	ch chan error
}

// Connect will form a connection to the NATS Streaming subsystem.
func Connect(stanClusterID, clientID string, options ...Option) (Conn, error) {
	// Process Options
	c := conn{clientID: clientID, opts: DefaultOptions}
	for _, opt := range options {
		if err := opt(&c.opts); err != nil {
			return nil, err
		}
	}
	// Check if the user has provided a connection as an option
	c.nc = c.opts.NatsConn
	// Create a NATS connection if it doesn't exist.
	if c.nc == nil {
		nc, err := nats.Connect(c.opts.NatsURL, nats.Name(clientID))
		if err != nil {
			return nil, err
		}
		c.nc = nc
		c.ncOwned = true
	} else if !c.nc.IsConnected() {
		// Bail if the custom NATS connection is disconnected
		return nil, ErrBadConnection
	}

	// Create a heartbeat inbox
	hbInbox := nats.NewInbox()
	var err error
	if c.hbSubscription, err = c.nc.Subscribe(hbInbox, c.processHeartBeat); err != nil {
		c.Close()
		return nil, err
	}

	// Send Request to discover the cluster
	discoverSubject := c.opts.DiscoverPrefix + "." + stanClusterID
	req := &pb.ConnectRequest{ClientID: clientID, HeartbeatInbox: hbInbox}
	b, _ := req.Marshal()
	reply, err := c.nc.Request(discoverSubject, b, c.opts.ConnectTimeout)
	if err != nil {
		c.Close()
		if err == nats.ErrTimeout {
			return nil, ErrConnectReqTimeout
		}
		return nil, err
	}
	// Process the response, grab server pubPrefix
	cr := &pb.ConnectResponse{}
	err = cr.Unmarshal(reply.Data)
	if err != nil {
		c.Close()
		return nil, err
	}
	if cr.Error != "" {
		c.Close()
		return nil, errors.New(cr.Error)
	}

	// Capture cluster configuration endpoints to publish and subscribe/unsubscribe.
	c.pubPrefix = cr.PubPrefix
	c.subRequests = cr.SubRequests
	c.unsubRequests = cr.UnsubRequests
	c.subCloseRequests = cr.SubCloseRequests
	c.closeRequests = cr.CloseRequests

	// Setup the ACK subscription
	c.ackSubject = DefaultACKPrefix + "." + nuid.Next()
	if c.ackSubscription, err = c.nc.Subscribe(c.ackSubject, c.processAck); err != nil {
		c.Close()
		return nil, err
	}
	c.ackSubscription.SetPendingLimits(1024*1024, 32*1024*1024)
	c.pubAckMap = make(map[string]*ack)

	// Create Subscription map
	c.subMap = make(map[string]*subscription)

	c.pubAckChan = make(chan struct{}, c.opts.MaxPubAcksInflight)

	// Attach a finalizer
	runtime.SetFinalizer(&c, func(sc *conn) { sc.Close() })

	return &c, nil
}

// Close a connection to the stan system.
func (sc *conn) Close() error {
	if sc == nil {
		return ErrBadConnection
	}

	sc.Lock()
	defer sc.Unlock()

	if sc.nc == nil {
		// We are already closed.
		return nil
	}

	// Capture for NATS calls below.
	nc := sc.nc
	if sc.ncOwned {
		defer nc.Close()
	}

	// Signals we are closed.
	sc.nc = nil

	// Now close ourselves.
	if sc.ackSubscription != nil {
		sc.ackSubscription.Unsubscribe()
	}

	req := &pb.CloseRequest{ClientID: sc.clientID}
	b, _ := req.Marshal()
	reply, err := nc.Request(sc.closeRequests, b, sc.opts.ConnectTimeout)
	if err != nil {
		if err == nats.ErrTimeout {
			return ErrCloseReqTimeout
		}
		return err
	}
	cr := &pb.CloseResponse{}
	err = cr.Unmarshal(reply.Data)
	if err != nil {
		return err
	}
	if cr.Error != "" {
		return errors.New(cr.Error)
	}
	return nil
}

// NatsConn returns the underlying NATS conn. Use this with care. For example,
// closing the wrapped NATS conn will put the NATS Streaming Conn in an invalid
// state.
func (sc *conn) NatsConn() *nats.Conn {
	return sc.nc
}

// Process a heartbeat from the NATS Streaming cluster
func (sc *conn) processHeartBeat(m *nats.Msg) {
	// No payload assumed, just reply.
	sc.RLock()
	nc := sc.nc
	sc.RUnlock()
	if nc != nil {
		nc.Publish(m.Reply, nil)
	}
}

// Process an ack from the NATS Streaming cluster
func (sc *conn) processAck(m *nats.Msg) {
	pa := &pb.PubAck{}
	err := pa.Unmarshal(m.Data)
	if err != nil {
		// FIXME, make closure to have context?
		fmt.Printf("Error processing unmarshal\n")
		return
	}

	// Remove
	a := sc.removeAck(pa.Guid)
	if a != nil {
		// Capture error if it exists.
		if pa.Error != "" {
			err = errors.New(pa.Error)
		}
		if a.ah != nil {
			// Perform the ackHandler callback
			a.ah(pa.Guid, err)
		} else if a.ch != nil {
			// Send to channel directly
			a.ch <- err
		}
	}
}

// Publish will publish to the cluster and wait for an ACK.
func (sc *conn) Publish(subject string, data []byte) error {
	ch := make(chan error)
	_, err := sc.publishAsync(subject, data, nil, ch)
	if err == nil {
		err = <-ch
	}
	return err
}

// PublishAsync will publish to the cluster on pubPrefix+subject and asynchronously
// process the ACK or error state. It will return the GUID for the message being sent.
func (sc *conn) PublishAsync(subject string, data []byte, ah AckHandler) (string, error) {
	return sc.publishAsync(subject, data, ah, nil)
}

func (sc *conn) publishAsync(subject string, data []byte, ah AckHandler, ch chan error) (string, error) {
	a := &ack{ah: ah, ch: ch}
	sc.Lock()
	if sc.nc == nil {
		sc.Unlock()
		return "", ErrConnectionClosed
	}

	subj := sc.pubPrefix + "." + subject
	// This is only what we need from PubMsg in the timer below,
	// so do this so that pe doesn't escape (and we same on new object)
	peGUID := nuid.Next()
	pe := &pb.PubMsg{ClientID: sc.clientID, Guid: peGUID, Subject: subject, Data: data}
	b, _ := pe.Marshal()

	// Map ack to guid.
	sc.pubAckMap[peGUID] = a
	// snapshot
	ackSubject := sc.ackSubject
	ackTimeout := sc.opts.AckTimeout
	pac := sc.pubAckChan
	sc.Unlock()

	// Use the buffered channel to control the number of outstanding acks.
	pac <- struct{}{}

	err := sc.nc.PublishRequest(subj, ackSubject, b)
	if err != nil {
		sc.removeAck(peGUID)
		return "", err
	}

	// Setup the timer for expiration.
	sc.Lock()
	a.t = time.AfterFunc(ackTimeout, func() {
		sc.removeAck(peGUID)
		if a.ah != nil {
			ah(peGUID, ErrTimeout)
		} else if a.ch != nil {
			a.ch <- ErrTimeout
		}
	})
	sc.Unlock()

	return peGUID, nil
}

// removeAck removes the ack from the pubAckMap and cancels any state, e.g. timers
func (sc *conn) removeAck(guid string) *ack {
	var t *time.Timer
	sc.Lock()
	a := sc.pubAckMap[guid]
	if a != nil {
		t = a.t
		delete(sc.pubAckMap, guid)
	}
	pac := sc.pubAckChan
	sc.Unlock()

	// Cancel timer if needed.
	if t != nil {
		t.Stop()
	}

	// Remove from channel to unblock PublishAsync
	if a != nil && len(pac) > 0 {
		<-pac
	}
	return a
}

// Process an msg from the NATS Streaming cluster
func (sc *conn) processMsg(raw *nats.Msg) {
	msg := &Msg{}
	err := msg.Unmarshal(raw.Data)
	if err != nil {
		panic("Error processing unmarshal for msg")
	}
	// Lookup the subscription
	sc.RLock()
	nc := sc.nc
	isClosed := nc == nil
	sub := sc.subMap[raw.Subject]
	sc.RUnlock()

	// Check if sub is no longer valid or connection has been closed.
	if sub == nil || isClosed {
		return
	}

	// Store in msg for backlink
	msg.Sub = sub

	sub.RLock()
	cb := sub.cb
	ackSubject := sub.ackInbox
	isManualAck := sub.opts.ManualAcks
	subsc := sub.sc // Can be nil if sub has been unsubscribed.
	sub.RUnlock()

	// Perform the callback
	if cb != nil && subsc != nil {
		cb(msg)
	}

	// Proces auto-ack
	if !isManualAck && nc != nil {
		ack := &pb.Ack{Subject: msg.Subject, Sequence: msg.Sequence}
		b, _ := ack.Marshal()
		if err := nc.Publish(ackSubject, b); err != nil {
			// FIXME(dlc) - Async error handler? Retry?
		}
	}
}
