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

package wsconn

import (
	"context"
	"sync"
)

// MuxClient is a stateful
type MuxClient struct {
	MuxID            uint64
	SendSeq, RecvSeq uint32
	Resp             chan []byte
	ctx              context.Context
	parent           *Connection
	respWait         chan<- Response
	respMu           sync.Mutex
	blocked          bool
	closed           bool
}

type Response struct {
	msg []byte
	err error
}

func newMuxClient(ctx context.Context, muxID uint64, parent *Connection) *MuxClient {
	return &MuxClient{
		MuxID:  muxID,
		Resp:   make(chan []byte, 1),
		ctx:    ctx,
		parent: parent,
	}
}

// roundtrip performs a roundtrip, returning the first response.
// This cannot be used concurrently.
func (m *MuxClient) roundtrip(h HandlerID, req []byte) ([]byte, error) {
	msg := message{
		Op:      OpRequest,
		MuxID:   m.MuxID,
		Handler: h,
		Seq:     0,
		Flags:   0,
		Payload: req,
	}
	ch := make(chan Response, 1)
	m.respWait = ch
	dst := GetByteBuffer()[:0]
	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return nil, err
	}
	// Send...
	m.parent.send(dst)

	// Wait for response or context.
	select {
	case v := <-ch:
		return v.msg, v.err
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

// RequestBytes will send a single payload request and stream back results.
// The
func (m *MuxClient) RequestBytes(h HandlerID, req []byte, out chan<- Response) {
	// Try to grab an initial block.
	msg := message{
		Op:      OpConnectMux,
		MuxID:   m.MuxID,
		Handler: h,
		Seq:     m.SendSeq,
		Flags:   FlagEOF,
		Payload: req,
	}

	// Send...
	dst := GetByteBuffer()[:0]
	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		out <- Response{err: err}
		return
	}

	// Route directly to output.
	m.respWait = out
}

// response will send incoming response to client.
// may never block.
// Should return whether the next call would block.
func (m *MuxClient) response(seq uint32, r Response) (unblock bool) {
	// Copy before testing.
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if m.closed {
		select {
		case m.respWait <- r:
		default:
			PutByteBuffer(r.msg)
		}
		m.closeLocked()
		return
	}

	if seq != m.RecvSeq {
		m.respWait <- Response{err: ErrIncorrectSequence}
		PutByteBuffer(r.msg)
		return
	}
	m.RecvSeq++
	select {
	case m.respWait <- r:
	default:
		PutByteBuffer(r.msg)
	}
}

func (m *MuxClient) close() {
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
}
