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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/zeebo/xxh3"
)

// muxClient is a stateful connection to a remote.
type muxClient struct {
	MuxID              uint64
	SendSeq, RecvSeq   uint32
	LastPong           int64
	BaseFlags          Flags
	ctx                context.Context
	cancelFn           context.CancelCauseFunc
	parent             *Connection
	respWait           chan<- Response
	respMu             sync.Mutex
	singleResp         bool
	closed             bool
	stateless          bool
	acked              bool
	init               bool
	deadline           time.Duration
	outBlock           chan struct{}
	subroute           *subHandlerID
	respErr            atomic.Pointer[error]
	clientPingInterval time.Duration
}

// Response is a response from the server.
type Response struct {
	Msg []byte
	Err error
}

func newMuxClient(ctx context.Context, muxID uint64, parent *Connection) *muxClient {
	ctx, cancelFn := context.WithCancelCause(ctx)
	return &muxClient{
		MuxID:              muxID,
		ctx:                ctx,
		cancelFn:           cancelFn,
		parent:             parent,
		LastPong:           time.Now().UnixNano(),
		BaseFlags:          parent.baseFlags,
		clientPingInterval: parent.clientPingInterval,
	}
}

// roundtrip performs a roundtrip, returning the first response.
// This cannot be used concurrently.
func (m *muxClient) roundtrip(h HandlerID, req []byte) ([]byte, error) {
	if m.init {
		return nil, errors.New("mux client already used")
	}
	m.init = true
	m.singleResp = true
	msg := message{
		Op:         OpRequest,
		MuxID:      m.MuxID,
		Handler:    h,
		Flags:      m.BaseFlags | FlagEOF,
		Payload:    req,
		DeadlineMS: uint32(m.deadline.Milliseconds()),
	}
	if m.subroute != nil {
		msg.Flags |= FlagSubroute
	}
	ch := make(chan Response, 1)
	m.respMu.Lock()
	if m.closed {
		m.respMu.Unlock()
		return nil, ErrDisconnected
	}
	m.respWait = ch
	m.respMu.Unlock()
	ctx := m.ctx

	// Add deadline if none.
	if msg.DeadlineMS == 0 {
		msg.DeadlineMS = uint32(defaultSingleRequestTimeout / time.Millisecond)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultSingleRequestTimeout)
		defer cancel()
	}
	// Send request
	if err := m.send(msg); err != nil {
		return nil, err
	}
	if debugReqs {
		fmt.Println(m.MuxID, m.parent.String(), "SEND")
	}
	// Wait for response or context.
	select {
	case v, ok := <-ch:
		if !ok {
			return nil, ErrDisconnected
		}
		if debugReqs && v.Err != nil {
			v.Err = fmt.Errorf("%d %s RESP ERR: %w", m.MuxID, m.parent.String(), v.Err)
		}
		return v.Msg, v.Err
	case <-ctx.Done():
		if debugReqs {
			return nil, fmt.Errorf("%d %s ERR: %w", m.MuxID, m.parent.String(), context.Cause(ctx))
		}
		return nil, context.Cause(ctx)
	}
}

// send the message. msg.Seq and msg.MuxID will be set
func (m *muxClient) send(msg message) error {
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if m.closed {
		return errors.New("mux client closed")
	}
	return m.sendLocked(msg)
}

// sendLocked the message. msg.Seq and msg.MuxID will be set.
// m.respMu must be held.
func (m *muxClient) sendLocked(msg message) error {
	dst := GetByteBufferCap(msg.Msgsize())
	msg.Seq = m.SendSeq
	msg.MuxID = m.MuxID
	msg.Flags |= m.BaseFlags
	if debugPrint {
		fmt.Println("Client sending", &msg, "to", m.parent.Remote)
	}
	m.SendSeq++

	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return err
	}
	if msg.Flags&FlagSubroute != 0 {
		if m.subroute == nil {
			return fmt.Errorf("internal error: subroute not defined on client")
		}
		hid := m.subroute.withHandler(msg.Handler)
		before := len(dst)
		dst = append(dst, hid[:]...)
		if debugPrint {
			fmt.Println("Added subroute", hid.String(), "to message", msg, "len", len(dst)-before)
		}
	}
	if msg.Flags&FlagCRCxxh3 != 0 {
		h := xxh3.Hash(dst)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
	}
	return m.parent.send(m.ctx, dst)
}

// RequestStateless will send a single payload request and stream back results.
// req may not be read/written to after calling.
// TODO: Not implemented
func (m *muxClient) RequestStateless(h HandlerID, req []byte, out chan<- Response) {
	if m.init {
		out <- Response{Err: errors.New("mux client already used")}
	}
	m.init = true

	// Try to grab an initial block.
	m.singleResp = false
	msg := message{
		Op:         OpConnectMux,
		Handler:    h,
		Flags:      FlagEOF,
		Payload:    req,
		DeadlineMS: uint32(m.deadline.Milliseconds()),
	}
	msg.setZeroPayloadFlag()
	if m.subroute != nil {
		msg.Flags |= FlagSubroute
	}

	// Send...
	err := m.send(msg)
	if err != nil {
		out <- Response{Err: err}
		return
	}

	// Route directly to output.
	m.respWait = out
}

// RequestStream will send a single payload request and stream back results.
// 'requests' can be nil, in which case only req is sent as input.
// It will however take less resources.
func (m *muxClient) RequestStream(h HandlerID, payload []byte, requests chan []byte, responses chan Response) (*Stream, error) {
	if m.init {
		return nil, errors.New("mux client already used")
	}
	if responses == nil {
		return nil, errors.New("RequestStream: responses channel is nil")
	}
	m.init = true
	m.respMu.Lock()
	if m.closed {
		m.respMu.Unlock()
		return nil, ErrDisconnected
	}
	m.respWait = responses // Route directly to output.
	m.respMu.Unlock()

	// Try to grab an initial block.
	m.singleResp = false
	m.RecvSeq = m.SendSeq // Sync
	if cap(requests) > 0 {
		m.outBlock = make(chan struct{}, cap(requests))
	}
	msg := message{
		Op:         OpConnectMux,
		Handler:    h,
		Payload:    payload,
		DeadlineMS: uint32(m.deadline.Milliseconds()),
	}
	msg.setZeroPayloadFlag()
	if requests == nil {
		msg.Flags |= FlagEOF
	}
	if m.subroute != nil {
		msg.Flags |= FlagSubroute
	}

	// Send...
	err := m.send(msg)
	if err != nil {
		return nil, err
	}
	if debugPrint {
		fmt.Println("Connecting Mux", m.MuxID, ",to", m.parent.Remote)
	}

	// Space for one message and an error.
	responseCh := make(chan Response, 1)

	// Spawn simple disconnect
	if requests == nil {
		go m.handleOneWayStream(responseCh, responses)
		return &Stream{responses: responseCh, Requests: nil, ctx: m.ctx, cancel: m.cancelFn, muxID: m.MuxID}, nil
	}

	// Deliver responses and send unblocks back to the server.
	go m.handleTwowayResponses(responseCh, responses)
	go m.handleTwowayRequests(responses, requests)

	return &Stream{responses: responseCh, Requests: requests, ctx: m.ctx, cancel: m.cancelFn, muxID: m.MuxID}, nil
}

func (m *muxClient) addErrorNonBlockingClose(respHandler chan<- Response, err error) {
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if !m.closed {
		m.respErr.Store(&err)
		// Do not block.
		select {
		case respHandler <- Response{Err: err}:
			xioutil.SafeClose(respHandler)
		default:
			go func() {
				respHandler <- Response{Err: err}
				xioutil.SafeClose(respHandler)
			}()
		}
		gridLogIf(m.ctx, m.sendLocked(message{Op: OpDisconnectServerMux, MuxID: m.MuxID}))
		m.closed = true
	}
}

// respHandler
func (m *muxClient) handleOneWayStream(respHandler chan<- Response, respServer <-chan Response) {
	if debugPrint {
		start := time.Now()
		defer func() {
			fmt.Println("Mux", m.MuxID, "Request took", time.Since(start).Round(time.Millisecond))
		}()
	}
	defer func() {
		// addErrorNonBlockingClose will close the response channel
		// - maybe async, so we shouldn't do it here.
		if m.respErr.Load() == nil {
			xioutil.SafeClose(respHandler)
		}
	}()
	var pingTimer <-chan time.Time
	if m.deadline == 0 || m.deadline > m.clientPingInterval {
		ticker := time.NewTicker(m.clientPingInterval)
		defer ticker.Stop()
		pingTimer = ticker.C
		atomic.StoreInt64(&m.LastPong, time.Now().UnixNano())
	}
	defer m.parent.deleteMux(false, m.MuxID)
	for {
		select {
		case <-m.ctx.Done():
			if debugPrint {
				fmt.Println("Client sending disconnect to mux", m.MuxID)
			}
			err := context.Cause(m.ctx)
			if !errors.Is(err, errStreamEOF) {
				m.addErrorNonBlockingClose(respHandler, err)
			}
			return
		case resp, ok := <-respServer:
			if !ok {
				return
			}
		sendResp:
			select {
			case respHandler <- resp:
				m.respMu.Lock()
				if !m.closed {
					gridLogIf(m.ctx, m.sendLocked(message{Op: OpUnblockSrvMux, MuxID: m.MuxID}))
				}
				m.respMu.Unlock()
			case <-m.ctx.Done():
				// Client canceled. Don't block.
				// Next loop will catch it.
			case <-pingTimer:
				if !m.doPing(respHandler) {
					return
				}
				goto sendResp
			}
		case <-pingTimer:
			if !m.doPing(respHandler) {
				return
			}
		}
	}
}

// doPing checks last ping time and sends another ping.
func (m *muxClient) doPing(respHandler chan<- Response) (ok bool) {
	m.respMu.Lock()
	if m.closed {
		m.respMu.Unlock()
		// Already closed. This is not an error state;
		// we may just be delivering the last responses.
		return true
	}

	// Only check ping when not closed.
	if got := time.Since(time.Unix(0, atomic.LoadInt64(&m.LastPong))); got > m.clientPingInterval*2 {
		m.respMu.Unlock()
		if debugPrint {
			fmt.Printf("Mux %d: last pong %v ago, disconnecting\n", m.MuxID, got)
		}
		m.addErrorNonBlockingClose(respHandler, ErrDisconnected)
		return false
	}

	// Send new ping
	err := m.sendLocked(message{Op: OpPing, MuxID: m.MuxID})
	m.respMu.Unlock()
	if err != nil {
		m.addErrorNonBlockingClose(respHandler, err)
	}
	return err == nil
}

// responseCh is the channel to that goes to the requester.
// internalResp is the channel that comes from the server.
func (m *muxClient) handleTwowayResponses(responseCh chan<- Response, internalResp <-chan Response) {
	defer func() {
		m.parent.deleteMux(false, m.MuxID)
		// addErrorNonBlockingClose will close the response channel.
		xioutil.SafeClose(responseCh)
	}()

	// Cancelation and errors are handled by handleTwowayRequests below.
	for resp := range internalResp {
		m.send(message{Op: OpUnblockSrvMux, MuxID: m.MuxID})
		responseCh <- resp
	}
}

func (m *muxClient) handleTwowayRequests(errResp chan<- Response, requests <-chan []byte) {
	var errState bool
	if debugPrint {
		start := time.Now()
		defer func() {
			fmt.Println("Mux", m.MuxID, "Request took", time.Since(start).Round(time.Millisecond))
		}()
	}

	var pingTimer <-chan time.Time
	if m.deadline == 0 || m.deadline > m.clientPingInterval {
		ticker := time.NewTicker(m.clientPingInterval)
		defer ticker.Stop()
		pingTimer = ticker.C
		atomic.StoreInt64(&m.LastPong, time.Now().UnixNano())
	}

	// Listen for client messages.
reqLoop:
	for !errState {
		select {
		case <-m.ctx.Done():
			if debugPrint {
				fmt.Println("Client sending disconnect to mux", m.MuxID)
			}
			m.addErrorNonBlockingClose(errResp, context.Cause(m.ctx))
			errState = true
			continue
		case <-pingTimer:
			if !m.doPing(errResp) {
				errState = true
				continue
			}
		case req, ok := <-requests:
			if !ok {
				// Done send EOF
				if debugPrint {
					fmt.Println("Client done, sending EOF to mux", m.MuxID)
				}
				msg := message{
					Op:    OpMuxClientMsg,
					MuxID: m.MuxID,
					Flags: FlagEOF,
				}
				msg.setZeroPayloadFlag()
				err := m.send(msg)
				if err != nil {
					m.addErrorNonBlockingClose(errResp, err)
				}
				break reqLoop
			}
			// Grab a send token.
		sendReq:
			select {
			case <-m.ctx.Done():
				m.addErrorNonBlockingClose(errResp, context.Cause(m.ctx))
				errState = true
				continue
			case <-pingTimer:
				if !m.doPing(errResp) {
					errState = true
					continue
				}
				goto sendReq
			case <-m.outBlock:
			}
			msg := message{
				Op:      OpMuxClientMsg,
				MuxID:   m.MuxID,
				Seq:     1,
				Payload: req,
			}
			msg.setZeroPayloadFlag()
			err := m.send(msg)
			PutByteBuffer(req)
			if err != nil {
				m.addErrorNonBlockingClose(errResp, err)
				errState = true
				continue
			}
			msg.Seq++
		}
	}

	if errState {
		// Drain requests.
		for {
			select {
			case r, ok := <-requests:
				if !ok {
					return
				}
				PutByteBuffer(r)
			default:
				return
			}
		}
	}

	for !errState {
		select {
		case <-m.ctx.Done():
			if debugPrint {
				fmt.Println("Client sending disconnect to mux", m.MuxID)
			}
			m.addErrorNonBlockingClose(errResp, context.Cause(m.ctx))
			return
		case <-pingTimer:
			errState = !m.doPing(errResp)
		}
	}
}

// checkSeq will check if sequence number is correct and increment it by 1.
func (m *muxClient) checkSeq(seq uint32) (ok bool) {
	if seq != m.RecvSeq {
		if debugPrint {
			fmt.Printf("MuxID: %d client, expected sequence %d, got %d\n", m.MuxID, m.RecvSeq, seq)
		}
		m.addResponse(Response{Err: ErrIncorrectSequence})
		return false
	}
	m.RecvSeq++
	return true
}

// response will send handleIncoming response to client.
// may never block.
// Should return whether the next call would block.
func (m *muxClient) response(seq uint32, r Response) {
	if debugReqs {
		fmt.Println(m.MuxID, m.parent.String(), "RESP")
	}
	if debugPrint {
		fmt.Printf("mux %d: got msg seqid %d, payload length: %d, err:%v\n", m.MuxID, seq, len(r.Msg), r.Err)
	}
	if !m.checkSeq(seq) {
		if debugReqs {
			fmt.Println(m.MuxID, m.parent.String(), "CHECKSEQ FAIL", m.RecvSeq, seq)
		}
		PutByteBuffer(r.Msg)
		r.Msg = nil
		r.Err = ErrIncorrectSequence
		m.addResponse(r)
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().UnixNano())
	ok := m.addResponse(r)
	if !ok {
		PutByteBuffer(r.Msg)
	}
}

var errStreamEOF = errors.New("stream EOF")

// error is a message from the server to disconnect.
func (m *muxClient) error(err RemoteErr) {
	if debugPrint {
		fmt.Printf("mux %d: got remote err:%v\n", m.MuxID, string(err))
	}
	m.addResponse(Response{Err: &err})
}

func (m *muxClient) ack(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	if m.acked || m.outBlock == nil {
		return
	}
	available := cap(m.outBlock)
	for range available {
		m.outBlock <- struct{}{}
	}
	m.acked = true
}

func (m *muxClient) unblockSend(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	select {
	case m.outBlock <- struct{}{}:
	default:
		gridLogIf(m.ctx, errors.New("output unblocked overflow"))
	}
}

func (m *muxClient) pong(msg pongMsg) {
	if msg.NotFound || msg.Err != nil {
		err := errors.New("remote terminated call")
		if msg.Err != nil {
			err = fmt.Errorf("remove pong failed: %v", &msg.Err)
		}
		m.addResponse(Response{Err: err})
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().UnixNano())
}

// addResponse will add a response to the response channel.
// This function will never block
func (m *muxClient) addResponse(r Response) (ok bool) {
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if m.closed {
		return false
	}
	select {
	case m.respWait <- r:
		if r.Err != nil {
			if debugPrint {
				fmt.Println("Closing mux", m.MuxID, "due to error:", r.Err)
			}
			m.closeLocked()
		}
		return true
	default:
		if m.stateless {
			// Drop message if not stateful.
			return ok
		}
		err := errors.New("INTERNAL ERROR: Response was blocked")
		gridLogIf(m.ctx, err)
		m.closeLocked()
		return false
	}
}

func (m *muxClient) close() {
	if debugPrint {
		fmt.Println("closing outgoing mux", m.MuxID)
	}
	if !m.respMu.TryLock() {
		// Cancel before locking - will unblock any pending sends.
		if m.cancelFn != nil {
			m.cancelFn(context.Canceled)
		}
		// Wait for senders to release.
		m.respMu.Lock()
	}

	defer m.respMu.Unlock()
	m.closeLocked()
}

func (m *muxClient) closeLocked() {
	if m.closed {
		return
	}
	// We hold the lock, so nobody can modify m.respWait while we're closing.
	if m.respWait != nil {
		xioutil.SafeClose(m.respWait)
		m.respWait = nil
	}
	m.closed = true
}
