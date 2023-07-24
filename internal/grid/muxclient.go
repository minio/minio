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
	parent           *Connection
	respWait         chan<- Response
	respMu           sync.Mutex
	blocked          bool
	closed           bool
	stateful         bool
	deadline         time.Duration
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
	return &MuxClient{
		MuxID:    muxID,
		Resp:     make(chan []byte, 1),
		ctx:      ctx,
		parent:   parent,
		LastPong: time.Now().Unix(),
	}
}

// roundtrip performs a roundtrip, returning the first response.
// This cannot be used concurrently.
func (m *MuxClient) roundtrip(h HandlerID, req []byte) ([]byte, error) {
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
	if err := m.send(msg); err != nil {
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
	dst := GetByteBuffer()[:0]
	msg.Seq = m.SendSeq
	msg.MuxID = m.MuxID
	msg.Flags |= m.BaseFlags
	m.SendSeq++

	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return err
	}
	if msg.Flags&FlagCRCxxh3 != 0 {
		h := xxh3.Hash(dst)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
	}
	return m.parent.send(m.ctx, dst)
}

// RequestStateless will send a single payload request and stream back results.
// req may not be read/writen to after calling.
func (m *MuxClient) RequestStateless(h HandlerID, req []byte, out chan<- Response) {
	// Try to grab an initial block.
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

// RequestStream will send a single payload request and stream back results.
// 'requests' can be nil, in which case only req is sent as input.
// It will however take less resources.
func (m *MuxClient) RequestStream(h HandlerID, payload []byte, requests chan []byte, responses chan<- Response) error {
	// Try to grab an initial block.
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
		return err
	}
	if debugPrint {
		fmt.Println("Connecting to", m.parent.Remote)
	}
	if requests != nil {
		go func() {
			msg := message{
				Op:    OpMuxClientMsg,
				MuxID: m.MuxID,
				Seq:   1,
			}
			var errState bool
			for req := range requests {
				if errState {
					continue
				}
				msg.setZeroPayloadFlag()
				msg.Payload = req
				err := m.send(msg)
				if err != nil {
					PutByteBuffer(req)
					responses <- Response{Err: err}
					return
				}
				msg.Seq++
			}
			// Done send EOF
			msg.Payload = nil
			msg.Flags = FlagEOF
			err := m.send(msg)
			if err != nil {
				responses <- Response{Err: err}
				return
			}
		}()
	}
	// Route directly to output.
	m.respWait = responses

	return nil
}

// checkSeq will check if sequence number is correct and increment it by 1.
func (m *MuxClient) checkSeq(seq uint32) (ok bool) {
	if seq != m.RecvSeq {
		if debugPrint {
			fmt.Printf("expected sequence %d, got %d\n", m.RecvSeq, seq)
		}
		select {
		case m.respWait <- Response{Err: ErrIncorrectSequence}:
		default:
			go func() {
				m.respWait <- Response{Err: ErrIncorrectSequence}
			}()
		}
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
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if m.closed {
		PutByteBuffer(r.Msg)
		return
	}

	if !m.checkSeq(seq) {
		PutByteBuffer(r.Msg)
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().Unix())
	select {
	case m.respWait <- r:
		logger.LogIf(m.ctx, m.send(message{Op: OpUnblockSrvMux, MuxID: m.MuxID}))
	default:
		// Disconnect stateful.
		PutByteBuffer(r.Msg)
	}
}

func (m *MuxClient) ack(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	// TODO:
}

func (m *MuxClient) unblockSend(seq uint32) {
	if !m.checkSeq(seq) {
		return
	}
	// TODO:
	/*
		select {
		case m.outBlock <- struct{}{}:
		default:
			logger.LogIf(m.ctx, errors.New("output unblocked overflow"))
		}
	*/
}

func (m *MuxClient) pong(msg pongMsg) {
	if msg.NotFound || msg.Err != nil {
		m.respMu.Lock()
		defer m.respMu.Unlock()
		err := errors.New("remote terminated call")
		if msg.Err != nil {
			err = fmt.Errorf("remove pong failed: %v", &msg.Err)
		}
		if !m.closed {
			select {
			case m.respWait <- Response{Err: err}:
			default:
			}
		}
		m.closeLocked()
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().Unix())
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
