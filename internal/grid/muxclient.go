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

	"github.com/zeebo/xxh3"
)

// MuxClient is a stateful
type MuxClient struct {
	MuxID            uint64
	SendSeq, RecvSeq uint32
	LastPong         int64
	Resp             chan []byte
	ctx              context.Context
	parent           *Connection
	respWait         chan<- Response
	respMu           sync.Mutex
	blocked          bool
	closed           bool
	stateful         bool
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
		Op:      OpRequest,
		Handler: h,
		Flags:   0,
		Payload: req,
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

func (m *MuxClient) send(msg message) error {
	dst := GetByteBuffer()[:0]
	msg.Seq = m.SendSeq
	msg.MuxID = m.MuxID

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

// RequestBytes will send a single payload request and stream back results.
// req may not be read/writen to after calling.
func (m *MuxClient) RequestBytes(h HandlerID, req []byte, out chan<- Response) {
	// Try to grab an initial block.
	msg := message{
		Op:      OpConnectMux,
		MuxID:   m.MuxID,
		Handler: h,
		Flags:   FlagEOF,
		Payload: req,
	}

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

// response will send handleIncoming response to client.
// may never block.
// Should return whether the next call would block.
func (m *MuxClient) response(seq uint32, r Response) {
	// Copy before testing.
	m.respMu.Lock()
	defer m.respMu.Unlock()
	if m.closed {
		PutByteBuffer(r.Msg)
		return
	}

	if seq != m.RecvSeq {
		select {
		case m.respWait <- Response{Err: ErrIncorrectSequence}:
		default:
			go func() {
				m.respWait <- Response{Err: ErrIncorrectSequence}
			}()
		}
		PutByteBuffer(r.Msg)
		return
	}
	atomic.StoreInt64(&m.LastPong, time.Now().Unix())
	m.RecvSeq++
	select {
	case m.respWait <- r:
	default:
		// Disconnect stateful.
		PutByteBuffer(r.Msg)
	}
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
