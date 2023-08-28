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

	"github.com/minio/minio/internal/logger"
	"github.com/zeebo/xxh3"
)

// MuxClient is a stateful
type MuxClient struct {
	MuxID            uint64
	SendSeq, RecvSeq uint32
	LastPong         int64
	BaseFlags        uint8
	Resp             chan []byte
	ctx              context.Context
	cancelFn         context.CancelCauseFunc
	parent           *Connection
	respWait         chan<- Response
	respMu           sync.Mutex
	singleResp       bool
	closed           bool
	stateless        bool
	acked            bool
	init             bool
	deadline         time.Duration
	outBlock         chan struct{}
}

type serverResponse struct {
	Msg []byte
	Err *RemoteErr
}

type Response struct {
	Msg []byte
	Err error
}

func newMuxClient(ctx context.Context, muxID uint64, parent *Connection) *MuxClient {
	ctx, cancelFn := context.WithCancelCause(ctx)
	return &MuxClient{
		MuxID:    muxID,
		Resp:     make(chan []byte, 1),
		ctx:      ctx,
		cancelFn: cancelFn,
		parent:   parent,
		LastPong: time.Now().Unix(),
	}
}

// roundtrip performs a roundtrip, returning the first response.
// This cannot be used concurrently.
func (m *MuxClient) roundtrip(h HandlerID, req []byte) ([]byte, error) {
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
	ch := make(chan Response, 1)
	m.respWait = ch
	// Send...
	if err := m.sendLocked(msg); err != nil {
		return nil, err
	}

	// Wait for response or context.
	select {
	case v := <-ch:
		return v.Msg, v.Err
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

// send the message. msg.Seq and msg.MuxID will be set
func (m *MuxClient) send(msg message) error {
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if m.closed {
		return errors.New("mux client closed")
	}
	return m.sendLocked(msg)
}

// sendLocked the message. msg.Seq and msg.MuxID will be set.
// m.respMu must be held.
func (m *MuxClient) sendLocked(msg message) error {
	dst := GetByteBuffer()[:0]
	msg.Seq = m.SendSeq
	msg.MuxID = m.MuxID
	msg.Flags |= m.BaseFlags
	if debugPrint {
		fmt.Println("Client sending", msg)
	}

	m.SendSeq++

	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return err
	}
	if msg.Flags&FlagCRCxxh3 != 0 {
		h := xxh3.Hash(dst)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
	}
	return m.parent.send(dst)
}

// RequestStateless will send a single payload request and stream back results.
// req may not be read/writen to after calling.
// TODO: Probably unexport this.
func (m *MuxClient) RequestStateless(h HandlerID, req []byte, out chan<- Response) {
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

	// Send...
	err := m.send(msg)
	if err != nil {
		PutByteBuffer(req)
		out <- Response{Err: err}
		return
	}

	// Route directly to output.
	m.respWait = out
}

// clientPingInterval will ping the remote handler every 15 seconds.
// We disconnect when we exceed 2 intervals.
const clientPingInterval = time.Second * 15

// RequestStream will send a single payload request and stream back results.
// 'requests' can be nil, in which case only req is sent as input.
// It will however take less resources.
// TODO: Probably unexport this.
func (m *MuxClient) RequestStream(h HandlerID, payload []byte, requests chan []byte, responses chan Response) (*Stream, error) {
	if m.init {
		return nil, errors.New("mux client already used")
	}
	m.init = true
	// Try to grab an initial block.
	m.singleResp = false
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

	// Send...
	err := m.send(msg)
	if err != nil {
		return nil, err
	}
	if debugPrint {
		fmt.Println("Connecting to", m.parent.Remote)
	}

	// Route directly to output.
	m.respWait = responses
	responseCh := make(chan Response, 1)

	// Spawn simple disconnect
	if requests == nil {
		go func() {
			defer close(responseCh)
			var pingTimer <-chan time.Time
			if m.deadline == 0 || m.deadline > clientPingInterval {
				ticker := time.NewTicker(clientPingInterval)
				defer ticker.Stop()
				pingTimer = ticker.C
				m.LastPong = time.Now().Unix()
			}
			defer m.parent.deleteMux(false, m.MuxID)
			for {
				select {
				case <-m.ctx.Done():
					if debugPrint {
						fmt.Println("Client sending disconnect to mux", m.MuxID)
					}
					m.respMu.Lock()
					defer m.respMu.Unlock() // We always return in this path.
					if !m.closed {
						responseCh <- Response{Err: m.ctx.Err()}
						logger.LogIf(m.ctx, m.sendLocked(message{Op: OpDisconnectServerMux, MuxID: m.MuxID}))
						m.closeLocked()
					}
					return
				case resp, ok := <-responses:
					if !ok {
						return
					}
					select {
					case responseCh <- resp:
						logger.LogIf(m.ctx, m.sendLocked(message{Op: OpUnblockSrvMux, MuxID: m.MuxID}))
					case <-m.ctx.Done():
						// Client canceled. Don't block.
						// Next loop will catch it.
					}
				case <-pingTimer:
					if time.Now().Sub(time.Unix(atomic.LoadInt64(&m.LastPong), 0)) > clientPingInterval*2 {
						m.respMu.Lock()
						defer m.respMu.Unlock() // We always return in this path.
						if !m.closed {
							responseCh <- Response{Err: ErrDisconnected}
							logger.LogIf(m.ctx, m.sendLocked(message{Op: OpDisconnectServerMux, MuxID: m.MuxID}))
							m.closeLocked()
						}
						return
					}
					// Send new ping.
					logger.LogIf(m.ctx, m.send(message{Op: OpPing, MuxID: m.MuxID}))
				}
			}
		}()
		return &Stream{Responses: responseCh, Requests: nil, ctx: m.ctx}, nil
	}

	// Deliver responses and send unblocks back to the server.
	go func() {
		defer m.parent.deleteMux(false, m.MuxID)
		defer close(responseCh)
		for resp := range responses {
			responseCh <- resp
			m.send(message{Op: OpUnblockSrvMux, MuxID: m.MuxID})
		}
	}()

	// Listen for client messages.
	go func() {
		var errState bool
		for {
			select {
			case <-m.ctx.Done():
				if debugPrint {
					fmt.Println("Client sending disconnect to mux", m.MuxID)
				}
				m.respMu.Lock()
				defer m.respMu.Unlock()
				logger.LogIf(m.ctx, m.sendLocked(message{Op: OpDisconnectServerMux, MuxID: m.MuxID}))
				if !m.closed {
					responses <- Response{Err: m.ctx.Err()}
					m.closeLocked()
				}
				return
			case req, ok := <-requests:
				if !ok {
					// Done send EOF
					if debugPrint {
						fmt.Println("Client done, sending EOF to mux", m.MuxID)
					}
					msg := message{
						Op:    OpMuxClientMsg,
						MuxID: m.MuxID,
						Seq:   1,
						Flags: FlagEOF,
					}
					msg.setZeroPayloadFlag()
					err := m.send(msg)
					if err != nil {
						m.respMu.Lock()
						responses <- Response{Err: err}
						m.closeLocked()
						m.respMu.Unlock()
					}
					return
				}
				if errState {
					continue
				}
				// Grab a send token.
				select {
				case <-m.ctx.Done():
					errState = true
					continue
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
				if err != nil {
					PutByteBuffer(req)
					responses <- Response{Err: err}
					m.close()
					errState = true
					continue
				}
				msg.Seq++
			}
		}
	}()

	return &Stream{Responses: responseCh, Requests: requests, ctx: m.ctx}, nil
}

// checkSeq will check if sequence number is correct and increment it by 1.
func (m *MuxClient) checkSeq(seq uint32) (ok bool) {
	if seq != m.RecvSeq {
		if debugPrint {
			fmt.Printf("expected sequence %d, got %d\n", m.RecvSeq, seq)
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
func (m *MuxClient) response(seq uint32, r Response) {
	if debugPrint {
		fmt.Printf("mux %d: got msg seqid %d, payload lenght: %d, err:%v\n", m.MuxID, seq, len(r.Msg), r.Err)
	}
	if !m.checkSeq(seq) {
		PutByteBuffer(r.Msg)
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().Unix())
	ok := m.addResponse(r)
	if !ok {
		PutByteBuffer(r.Msg)
	}
}

// error is a message from the server to disconnect.
func (m *MuxClient) error(err RemoteErr) {
	if debugPrint {
		fmt.Printf("mux %d: got remote err:%v\n", m.MuxID, string(err))
	}
	m.addResponse(Response{Err: &err})
}

func (m *MuxClient) ack(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	if m.acked || m.outBlock == nil {
		return
	}
	available := cap(m.outBlock)
	for i := 0; i < available; i++ {
		m.outBlock <- struct{}{}
	}
	m.acked = true
}

func (m *MuxClient) unblockSend(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	select {
	case m.outBlock <- struct{}{}:
	default:
		logger.LogIf(m.ctx, errors.New("output unblocked overflow"))
	}
}

func (m *MuxClient) pong(msg pongMsg) {
	if msg.NotFound || msg.Err != nil {
		err := errors.New("remote terminated call")
		if msg.Err != nil {
			err = fmt.Errorf("remove pong failed: %v", &msg.Err)
		}
		m.addResponse(Response{Err: err})
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().Unix())
}

// addResponse will add a response to the response channel.
// This function will never block
func (m *MuxClient) addResponse(r Response) (ok bool) {
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
			return
		}
		err := errors.New("INTERNAL ERROR: Response was blocked")
		logger.LogIf(m.ctx, err)
		m.closeLocked()
		return false
	}
}

func (m *MuxClient) close() {
	if debugPrint {
		fmt.Println("closing mux", m.MuxID)
	}
	m.respMu.Lock()
	defer m.respMu.Unlock()
	m.closeLocked()
}

func (m *MuxClient) closeLocked() {
	if m.closed {
		return
	}
	close(m.respWait)
	m.respWait = nil
	m.closed = true
}
