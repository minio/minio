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

package grid

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	xioutil "github.com/minio/minio/internal/ioutil"
)

type muxServer struct {
	ID                 uint64
	LastPing           int64
	SendSeq, RecvSeq   uint32
	Resp               chan []byte
	BaseFlags          Flags
	ctx                context.Context
	cancel             context.CancelFunc
	inbound            chan []byte
	parent             *Connection
	sendMu             sync.Mutex
	recvMu             sync.Mutex
	outBlock           chan struct{}
	clientPingInterval time.Duration
}

func newMuxStateless(ctx context.Context, msg message, c *Connection, handler StatelessHandler) *muxServer {
	var cancel context.CancelFunc
	ctx = setCaller(ctx, c.remote)
	if msg.DeadlineMS > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(msg.DeadlineMS)*time.Millisecond)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	m := muxServer{
		ID:        msg.MuxID,
		RecvSeq:   msg.Seq + 1,
		SendSeq:   msg.Seq,
		ctx:       ctx,
		cancel:    cancel,
		parent:    c,
		LastPing:  time.Now().Unix(),
		BaseFlags: c.baseFlags,
	}
	go func() {
		// TODO: Handle
	}()

	return &m
}

func newMuxStream(ctx context.Context, msg message, c *Connection, handler StreamHandler) *muxServer {
	var cancel context.CancelFunc
	ctx = setCaller(ctx, c.remote)
	if len(handler.Subroute) > 0 {
		ctx = setSubroute(ctx, handler.Subroute)
	}
	if msg.DeadlineMS > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(msg.DeadlineMS)*time.Millisecond+c.addDeadline)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	send := make(chan []byte)
	inboundCap, outboundCap := handler.InCapacity, handler.OutCapacity
	if outboundCap <= 0 {
		outboundCap = 1
	}

	m := muxServer{
		ID:                 msg.MuxID,
		RecvSeq:            msg.Seq + 1,
		SendSeq:            msg.Seq,
		ctx:                ctx,
		cancel:             cancel,
		parent:             c,
		inbound:            nil,
		outBlock:           make(chan struct{}, outboundCap),
		LastPing:           time.Now().Unix(),
		BaseFlags:          c.baseFlags,
		clientPingInterval: c.clientPingInterval,
	}
	// Acknowledge Mux created.
	// Send async.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ack message
		ack.Op = OpAckMux
		ack.Flags = m.BaseFlags
		ack.MuxID = m.ID
		m.send(ack)
		if debugPrint {
			fmt.Println("connected stream mux:", ack.MuxID)
		}
	}()

	// Data inbound to the handler
	var handlerIn chan []byte
	if inboundCap > 0 {
		m.inbound = make(chan []byte, inboundCap)
		handlerIn = make(chan []byte, 1)
		go func(inbound chan []byte) {
			wg.Wait()
			defer xioutil.SafeClose(handlerIn)
			m.handleInbound(c, inbound, handlerIn)
		}(m.inbound)
	}
	// Fill outbound block.
	// Each token represents a message that can be sent to the client without blocking.
	// The client will refill the tokens as they confirm delivery of the messages.
	for range outboundCap {
		m.outBlock <- struct{}{}
	}

	// Handler goroutine.
	var handlerErr atomic.Pointer[RemoteErr]
	go func() {
		wg.Wait()
		defer xioutil.SafeClose(send)
		err := m.handleRequests(ctx, msg, send, handler, handlerIn)
		if err != nil {
			handlerErr.Store(err)
		}
	}()

	// Response sender goroutine...
	go func(outBlock <-chan struct{}) {
		wg.Wait()
		defer m.parent.deleteMux(true, m.ID)
		m.sendResponses(ctx, send, c, &handlerErr, outBlock)
	}(m.outBlock)

	// Remote aliveness check if needed.
	if msg.DeadlineMS == 0 || msg.DeadlineMS > uint32(4*c.clientPingInterval/time.Millisecond) {
		go func() {
			wg.Wait()
			m.checkRemoteAlive()
		}()
	}
	return &m
}

// handleInbound sends unblocks when we have delivered the message to the handler.
func (m *muxServer) handleInbound(c *Connection, inbound <-chan []byte, handlerIn chan<- []byte) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case in, ok := <-inbound:
			if !ok {
				return
			}
			select {
			case <-m.ctx.Done():
				return
			case handlerIn <- in:
				m.send(message{Op: OpUnblockClMux, MuxID: m.ID, Flags: c.baseFlags})
			}
		}
	}
}

// sendResponses will send responses to the client.
func (m *muxServer) sendResponses(ctx context.Context, toSend <-chan []byte, c *Connection, handlerErr *atomic.Pointer[RemoteErr], outBlock <-chan struct{}) {
	for {
		// Process outgoing message.
		var payload []byte
		var ok bool
		select {
		case payload, ok = <-toSend:
		case <-ctx.Done():
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-outBlock:
		}
		msg := message{
			MuxID: m.ID,
			Op:    OpMuxServerMsg,
			Flags: c.baseFlags,
		}
		if !ok {
			hErr := handlerErr.Load()
			if debugPrint {
				fmt.Println("muxServer: Mux", m.ID, "send EOF", hErr)
			}
			msg.Flags |= FlagEOF
			if hErr != nil {
				msg.Flags |= FlagPayloadIsErr
				msg.Payload = []byte(*hErr)
			}
			msg.setZeroPayloadFlag()
			m.send(msg)
			return
		}
		msg.Payload = payload
		msg.setZeroPayloadFlag()
		m.send(msg)
	}
}

// handleRequests will handle the requests from the client and call the handler function.
func (m *muxServer) handleRequests(ctx context.Context, msg message, send chan<- []byte, handler StreamHandler, handlerIn <-chan []byte) (handlerErr *RemoteErr) {
	start := time.Now()
	defer func() {
		if debugPrint {
			fmt.Println("Mux", m.ID, "Handler took", time.Since(start).Round(time.Millisecond))
		}
		if r := recover(); r != nil {
			gridLogIf(ctx, fmt.Errorf("grid handler (%v) panic: %v", msg.Handler, r))
			err := RemoteErr(fmt.Sprintf("handler panic: %v", r))
			handlerErr = &err
		}
		if debugPrint {
			fmt.Println("muxServer: Mux", m.ID, "Returned with", handlerErr)
		}
	}()
	// handlerErr is guarded by 'send' channel.
	handlerErr = handler.Handle(ctx, msg.Payload, handlerIn, send)
	return handlerErr
}

// checkRemoteAlive will check if the remote is alive.
func (m *muxServer) checkRemoteAlive() {
	t := time.NewTicker(m.clientPingInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			last := time.Since(time.Unix(atomic.LoadInt64(&m.LastPing), 0))
			if last > 4*m.clientPingInterval {
				gridLogIf(m.ctx, fmt.Errorf("canceling remote connection %s not seen for %v", m.parent, last))
				m.close()
				return
			}
		}
	}
}

// checkSeq will check if sequence number is correct and increment it by 1.
func (m *muxServer) checkSeq(seq uint32) (ok bool) {
	if seq != m.RecvSeq {
		if debugPrint {
			fmt.Printf("expected sequence %d, got %d\n", m.RecvSeq, seq)
		}
		m.disconnect(fmt.Sprintf("receive sequence number mismatch. want %d, got %d", m.RecvSeq, seq), false)
		return false
	}
	m.RecvSeq++
	return true
}

func (m *muxServer) message(msg message) {
	if debugPrint {
		fmt.Printf("muxServer: received message %d, length %d\n", msg.Seq, len(msg.Payload))
	}
	if !m.checkSeq(msg.Seq) {
		return
	}
	m.recvMu.Lock()
	defer m.recvMu.Unlock()
	if cap(m.inbound) == 0 {
		m.disconnect("did not expect inbound message", true)
		return
	}
	// Note, on EOF no value can be sent.
	if msg.Flags&FlagEOF != 0 {
		if len(msg.Payload) > 0 {
			gridLogIf(m.ctx, fmt.Errorf("muxServer: EOF message with payload"))
		}
		if m.inbound != nil {
			xioutil.SafeClose(m.inbound)
			m.inbound = nil
		}
		return
	}

	select {
	case <-m.ctx.Done():
	case m.inbound <- msg.Payload:
		if debugPrint {
			fmt.Printf("muxServer: Sent seq %d to handler\n", msg.Seq)
		}
	default:
		m.disconnect("handler blocked", true)
	}
}

func (m *muxServer) unblockSend(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	m.recvMu.Lock()
	defer m.recvMu.Unlock()
	if m.outBlock == nil {
		// Closed
		return
	}
	select {
	case m.outBlock <- struct{}{}:
	default:
		gridLogIf(m.ctx, errors.New("output unblocked overflow"))
	}
}

func (m *muxServer) ping(seq uint32) pongMsg {
	if !m.checkSeq(seq) {
		msg := fmt.Sprintf("receive sequence number mismatch. want %d, got %d", m.RecvSeq, seq)
		return pongMsg{Err: &msg}
	}
	select {
	case <-m.ctx.Done():
		err := context.Cause(m.ctx).Error()
		return pongMsg{Err: &err}
	default:
		atomic.StoreInt64(&m.LastPing, time.Now().Unix())
		return pongMsg{}
	}
}

// disconnect will disconnect the mux.
// m.recvMu must be locked when calling this function.
func (m *muxServer) disconnect(msg string, locked bool) {
	if debugPrint {
		fmt.Println("Mux", m.ID, "disconnecting. Reason:", msg)
	}
	if msg != "" {
		m.send(message{Op: OpMuxServerMsg, MuxID: m.ID, Flags: FlagPayloadIsErr | FlagEOF, Payload: []byte(msg)})
	} else {
		m.send(message{Op: OpDisconnectClientMux, MuxID: m.ID})
	}
	// Unlock, since we are calling deleteMux, which will call close - which will lock recvMu.
	if locked {
		m.recvMu.Unlock()
		defer m.recvMu.Lock()
	}
	m.parent.deleteMux(true, m.ID)
}

func (m *muxServer) send(msg message) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	msg.MuxID = m.ID
	msg.Seq = m.SendSeq
	m.SendSeq++
	if debugPrint {
		fmt.Printf("Mux %d, Sending %+v\n", m.ID, msg)
	}
	gridLogIf(m.ctx, m.parent.queueMsg(msg, nil))
}

func (m *muxServer) close() {
	m.cancel()
	m.recvMu.Lock()
	defer m.recvMu.Unlock()

	if m.inbound != nil {
		xioutil.SafeClose(m.inbound)
		m.inbound = nil
	}

	if m.outBlock != nil {
		xioutil.SafeClose(m.outBlock)
		m.outBlock = nil
	}
}
