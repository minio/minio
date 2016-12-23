// Copyright 2016 Apcera Inc. All rights reserved.

// Package stan is a Go client for the NATS Streaming messaging system (https://nats.io).
package stan

import (
	"errors"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming/pb"
)

const (
	// DefaultAckWait indicates how long the server should wait for an ACK before resending a message
	DefaultAckWait = 30 * time.Second
	// DefaultMaxInflight indicates how many messages with outstanding ACKs the server can send
	DefaultMaxInflight = 1024
)

// Msg is the client defined message, which includes proto, then back link to subscription.
type Msg struct {
	pb.MsgProto // MsgProto: Seq, Subject, Reply[opt], Data, Timestamp, CRC32[opt]
	Sub         Subscription
}

// Subscriptions and Options

// Subscription represents a subscription within the NATS Streaming cluster. Subscriptions
// will be rate matched and follow at-least delivery semantics.
type Subscription interface {
	ClearMaxPending() error
	Delivered() (int64, error)
	Dropped() (int, error)
	IsValid() bool
	MaxPending() (int, int, error)
	Pending() (int, int, error)
	PendingLimits() (int, int, error)
	SetPendingLimits(msgLimit, bytesLimit int) error
	// Unsubscribe removes interest in the subscription.
	// For durables, it means that the durable interest is also removed from
	// the server. Restarting a durable with the same name will not resume
	// the subscription, it will be considered a new one.
	Unsubscribe() error

	// Close removes this subscriber from the server, but unlike Unsubscribe(),
	// the durable interest is not removed. If the client has connected to a server
	// for which this feature is not available, Close() will return a ErrNoServerSupport
	// error.
	Close() error
}

// A subscription represents a subscription to a stan cluster.
type subscription struct {
	sync.RWMutex
	sc       *conn
	subject  string
	qgroup   string
	inbox    string
	ackInbox string
	inboxSub *nats.Subscription
	opts     SubscriptionOptions
	cb       MsgHandler
}

// SubscriptionOption is a function on the options for a subscription.
type SubscriptionOption func(*SubscriptionOptions) error

// MsgHandler is a callback function that processes messages delivered to
// asynchronous subscribers.
type MsgHandler func(msg *Msg)

// SubscriptionOptions are used to control the Subscription's behavior.
type SubscriptionOptions struct {
	// DurableName, if set will survive client restarts.
	DurableName string
	// Controls the number of messages the cluster will have inflight without an ACK.
	MaxInflight int
	// Controls the time the cluster will wait for an ACK for a given message.
	AckWait time.Duration
	// StartPosition enum from proto.
	StartAt pb.StartPosition
	// Optional start sequence number.
	StartSequence uint64
	// Optional start time.
	StartTime time.Time
	// Option to do Manual Acks
	ManualAcks bool
}

// DefaultSubscriptionOptions are the default subscriptions' options
var DefaultSubscriptionOptions = SubscriptionOptions{
	MaxInflight: DefaultMaxInflight,
	AckWait:     DefaultAckWait,
}

// MaxInflight is an Option to set the maximum number of messages the cluster will send
// without an ACK.
func MaxInflight(m int) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.MaxInflight = m
		return nil
	}
}

// AckWait is an Option to set the timeout for waiting for an ACK from the cluster's
// point of view for delivered messages.
func AckWait(t time.Duration) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.AckWait = t
		return nil
	}
}

// StartAt sets the desired start position for the message stream.
func StartAt(sp pb.StartPosition) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = sp
		return nil
	}
}

// StartAtSequence sets the desired start sequence position and state.
func StartAtSequence(seq uint64) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = pb.StartPosition_SequenceStart
		o.StartSequence = seq
		return nil
	}
}

// StartAtTime sets the desired start time position and state.
func StartAtTime(start time.Time) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = pb.StartPosition_TimeDeltaStart
		o.StartTime = start
		return nil
	}
}

// StartAtTimeDelta sets the desired start time position and state using the delta.
func StartAtTimeDelta(ago time.Duration) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = pb.StartPosition_TimeDeltaStart
		o.StartTime = time.Now().Add(-ago)
		return nil
	}
}

// StartWithLastReceived is a helper function to set start position to last received.
func StartWithLastReceived() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = pb.StartPosition_LastReceived
		return nil
	}
}

// DeliverAllAvailable will deliver all messages available.
func DeliverAllAvailable() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = pb.StartPosition_First
		return nil
	}
}

// SetManualAckMode will allow clients to control their own acks to delivered messages.
func SetManualAckMode() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.ManualAcks = true
		return nil
	}
}

// DurableName sets the DurableName for the subcriber.
func DurableName(name string) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.DurableName = name
		return nil
	}
}

// Subscribe will perform a subscription with the given options to the NATS Streaming cluster.
func (sc *conn) Subscribe(subject string, cb MsgHandler, options ...SubscriptionOption) (Subscription, error) {
	return sc.subscribe(subject, "", cb, options...)
}

// QueueSubscribe will perform a queue subscription with the given options to the NATS Streaming cluster.
func (sc *conn) QueueSubscribe(subject, qgroup string, cb MsgHandler, options ...SubscriptionOption) (Subscription, error) {
	return sc.subscribe(subject, qgroup, cb, options...)
}

// subscribe will perform a subscription with the given options to the NATS Streaming cluster.
func (sc *conn) subscribe(subject, qgroup string, cb MsgHandler, options ...SubscriptionOption) (Subscription, error) {
	sub := &subscription{subject: subject, qgroup: qgroup, inbox: nats.NewInbox(), cb: cb, sc: sc, opts: DefaultSubscriptionOptions}
	for _, opt := range options {
		if err := opt(&sub.opts); err != nil {
			return nil, err
		}
	}
	sc.Lock()
	if sc.nc == nil {
		sc.Unlock()
		return nil, ErrConnectionClosed
	}

	// Register subscription.
	sc.subMap[sub.inbox] = sub
	nc := sc.nc
	sc.Unlock()

	// Hold lock throughout.
	sub.Lock()
	defer sub.Unlock()

	// Listen for actual messages.
	nsub, err := nc.Subscribe(sub.inbox, sc.processMsg)
	if err != nil {
		return nil, err
	}
	sub.inboxSub = nsub

	// Create a subscription request
	// FIXME(dlc) add others.
	sr := &pb.SubscriptionRequest{
		ClientID:      sc.clientID,
		Subject:       subject,
		QGroup:        qgroup,
		Inbox:         sub.inbox,
		MaxInFlight:   int32(sub.opts.MaxInflight),
		AckWaitInSecs: int32(sub.opts.AckWait / time.Second),
		StartPosition: sub.opts.StartAt,
		DurableName:   sub.opts.DurableName,
	}

	// Conditionals
	switch sr.StartPosition {
	case pb.StartPosition_TimeDeltaStart:
		sr.StartTimeDelta = time.Now().UnixNano() - sub.opts.StartTime.UnixNano()
	case pb.StartPosition_SequenceStart:
		sr.StartSequence = sub.opts.StartSequence
	}

	b, _ := sr.Marshal()
	reply, err := sc.nc.Request(sc.subRequests, b, sc.opts.ConnectTimeout)
	if err != nil {
		sub.inboxSub.Unsubscribe()
		if err == nats.ErrTimeout {
			err = ErrSubReqTimeout
		}
		return nil, err
	}
	r := &pb.SubscriptionResponse{}
	if err := r.Unmarshal(reply.Data); err != nil {
		sub.inboxSub.Unsubscribe()
		return nil, err
	}
	if r.Error != "" {
		sub.inboxSub.Unsubscribe()
		return nil, errors.New(r.Error)
	}
	sub.ackInbox = r.AckInbox

	return sub, nil
}

// ClearMaxPending resets the maximums seen so far.
func (sub *subscription) ClearMaxPending() error {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return ErrBadSubscription
	}
	return sub.inboxSub.ClearMaxPending()
}

// Delivered returns the number of delivered messages for this subscription.
func (sub *subscription) Delivered() (int64, error) {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return -1, ErrBadSubscription
	}
	return sub.inboxSub.Delivered()
}

// Dropped returns the number of known dropped messages for this subscription.
// This will correspond to messages dropped by violations of PendingLimits. If
// the server declares the connection a SlowConsumer, this number may not be
// valid.
func (sub *subscription) Dropped() (int, error) {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return -1, ErrBadSubscription
	}
	return sub.inboxSub.Dropped()
}

// IsValid returns a boolean indicating whether the subscription
// is still active. This will return false if the subscription has
// already been closed.
func (sub *subscription) IsValid() bool {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return false
	}
	return sub.inboxSub.IsValid()
}

// MaxPending returns the maximum number of queued messages and queued bytes seen so far.
func (sub *subscription) MaxPending() (int, int, error) {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return -1, -1, ErrBadSubscription
	}
	return sub.inboxSub.MaxPending()
}

// Pending returns the number of queued messages and queued bytes in the client for this subscription.
func (sub *subscription) Pending() (int, int, error) {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return -1, -1, ErrBadSubscription
	}
	return sub.inboxSub.Pending()
}

// PendingLimits returns the current limits for this subscription.
// If no error is returned, a negative value indicates that the
// given metric is not limited.
func (sub *subscription) PendingLimits() (int, int, error) {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return -1, -1, ErrBadSubscription
	}
	return sub.inboxSub.PendingLimits()
}

// SetPendingLimits sets the limits for pending msgs and bytes for this subscription.
// Zero is not allowed. Any negative value means that the given metric is not limited.
func (sub *subscription) SetPendingLimits(msgLimit, bytesLimit int) error {
	sub.Lock()
	defer sub.Unlock()
	if sub.inboxSub == nil {
		return ErrBadSubscription
	}
	return sub.inboxSub.SetPendingLimits(msgLimit, bytesLimit)
}

// closeOrUnsubscribe performs either close or unsubsribe based on
// given boolean.
func (sub *subscription) closeOrUnsubscribe(doClose bool) error {
	if sub == nil {
		return ErrBadSubscription
	}
	sub.Lock()
	sc := sub.sc
	if sc == nil {
		// Already closed.
		sub.Unlock()
		return ErrBadSubscription
	}
	sub.sc = nil
	sub.inboxSub.Unsubscribe()
	sub.inboxSub = nil
	sub.Unlock()

	if sc == nil {
		return ErrBadSubscription
	}

	sc.Lock()
	if sc.nc == nil {
		sc.Unlock()
		return ErrConnectionClosed
	}

	delete(sc.subMap, sub.inbox)
	reqSubject := sc.unsubRequests
	if doClose {
		reqSubject = sc.subCloseRequests
		if reqSubject == "" {
			sc.Unlock()
			return ErrNoServerSupport
		}
	}

	// Snapshot connection to avoid data race, since the connection may be
	// closing while we try to send the request
	nc := sc.nc
	sc.Unlock()

	usr := &pb.UnsubscribeRequest{
		ClientID: sc.clientID,
		Subject:  sub.subject,
		Inbox:    sub.ackInbox,
	}
	b, _ := usr.Marshal()
	reply, err := nc.Request(reqSubject, b, sc.opts.ConnectTimeout)
	if err != nil {
		if err == nats.ErrTimeout {
			if doClose {
				return ErrCloseReqTimeout
			}
			return ErrUnsubReqTimeout
		}
		return err
	}
	r := &pb.SubscriptionResponse{}
	if err := r.Unmarshal(reply.Data); err != nil {
		return err
	}
	if r.Error != "" {
		return errors.New(r.Error)
	}

	return nil
}

// Unsubscribe implements the Subscription interface
func (sub *subscription) Unsubscribe() error {
	return sub.closeOrUnsubscribe(false)
}

// Close implements the Subscription interface
func (sub *subscription) Close() error {
	return sub.closeOrUnsubscribe(true)
}

// Ack manually acknowledges a message.
// The subscriber had to be created with SetManualAckMode() option.
func (msg *Msg) Ack() error {
	if msg == nil {
		return ErrNilMsg
	}
	// Look up subscription
	sub := msg.Sub.(*subscription)
	if sub == nil {
		return ErrBadSubscription
	}

	sub.RLock()
	ackSubject := sub.ackInbox
	isManualAck := sub.opts.ManualAcks
	sc := sub.sc
	sub.RUnlock()

	// Check for error conditions.
	if sc == nil {
		return ErrBadSubscription
	}
	// Get nc from the connection (needs locking to avoid race)
	sc.RLock()
	nc := sc.nc
	sc.RUnlock()
	if nc == nil {
		return ErrBadConnection
	}
	if !isManualAck {
		return ErrManualAck
	}

	// Ack here.
	ack := &pb.Ack{Subject: msg.Subject, Sequence: msg.Sequence}
	b, _ := ack.Marshal()
	return nc.Publish(ackSubject, b)
}
