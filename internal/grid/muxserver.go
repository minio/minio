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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/internal/logger"
)

type muxServer struct {
	ID               uint64
	LastPing         int64
	SendSeq, RecvSeq uint32
	Resp             chan []byte
	ctx              context.Context
	cancel           context.CancelFunc
	outbound         func(msg message) error
	inbound          chan []byte
	parent           *Connection
	sendMu           sync.Mutex
	outBlock         chan struct{}
}

func newMuxStateless(ctx context.Context, msg message, c *Connection, handler StatelessHandler) *muxServer {
	ctx, cancel := context.WithCancel(ctx)
	m := muxServer{
		ID:       msg.MuxID,
		RecvSeq:  msg.Seq,
		ctx:      ctx,
		cancel:   cancel,
		parent:   c,
		LastPing: time.Now().Unix(),
	}
	go func() {
	}()

	return &m
}

func newMuxStream(ctx context.Context, msg message, c *Connection, handler StatefulHandler) *muxServer {
	ctx, cancel := context.WithCancel(ctx)
	receive := make(chan []byte)
	send := make(chan Response)
	inboundCap, outboundCap := handler.InCapacity, handler.OutCapacity
	if inboundCap <= 0 {
		inboundCap = 1
	}
	if outboundCap <= 0 {
		outboundCap = 1
	}

	m := muxServer{
		ID:       msg.MuxID,
		RecvSeq:  msg.Seq,
		SendSeq:  msg.Seq,
		ctx:      ctx,
		cancel:   cancel,
		parent:   c,
		inbound:  make(chan []byte, inboundCap),
		outBlock: make(chan struct{}, outboundCap),
		LastPing: time.Now().Unix(),
	}
	for i := 0; i < outboundCap; i++ {
		m.outBlock <- struct{}{}
	}
	go func() {
		defer m.close()
		handler.Handle(ctx, msg.Payload, receive, send)
	}()
	go func() {
		defer m.parent.deleteMux(true, m.ID)
		for {
			select {
			case <-ctx.Done():
				return
			// Process outgoing message.
			case send, ok := <-send:
				if !ok {
					send.Err = io.EOF
				}
				<-m.outBlock
				msg := message{
					MuxID: m.ID,
				}
				eof := errors.Is(send.Err, io.EOF)
				if send.Err != nil && !eof {
					msg.Flags |= FlagEOF
					msg.Op = OpMuxServerErr
					msg.Payload = []byte(send.Err.Error())
					m.send(msg)
					return
				}
				msg.Op = OpMuxServerMsg
				msg.Payload = send.Msg
				if eof {
					msg.Op = OpMuxServerErr
					m.send(msg)
					return
				}
				m.send(msg)
			}
		}
	}()

	return &m
}

func (m *muxServer) message(msg message) {
	if m.inbound == nil {
		// Shouldn't be sending. Maybe log...
		return
	}
	wantSeq := m.RecvSeq + 1
	if msg.Seq != wantSeq {
		m.disconnect("receive sequence number mismatch")
	}
	m.RecvSeq++
	select {
	case <-m.ctx.Done():
	case m.inbound <- msg.Payload:
		m.send(message{Op: OpUnblockMux, MuxID: m.ID})
	default:
		go func() {
			select {
			case <-m.ctx.Done():
			case m.inbound <- msg.Payload:
				m.send(message{Op: OpUnblockMux, MuxID: m.ID})
			}
		}()
	}
}

func (m *muxServer) unblockSend() {
	select {
	case m.outBlock <- struct{}{}:
	default:
		logger.LogIf(m.ctx, errors.New("output unblocked overflow"))
	}
}

func (m *muxServer) ping() pongMsg {
	select {
	case <-m.ctx.Done():
		err := m.ctx.Err().Error()
		return pongMsg{Err: &err}
	default:
		atomic.StoreInt64(&m.LastPing, time.Now().Unix())
		return pongMsg{}
	}
}

func (m *muxServer) disconnect(msg string) {
	if msg != "" {
		m.send(message{Op: OpMuxServerErr, MuxID: m.ID, Payload: []byte(msg)})
	} else {
		m.send(message{Op: OpDisconnectMux, MuxID: m.ID})
	}
	m.parent.deleteMux(true, m.ID)
}

func (m *muxServer) send(msg message) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	m.SendSeq++
	msg.Seq = m.SendSeq
	if m.inbound == nil {
		logger.LogIf(m.ctx, m.parent.queueMsg(msg, nil))
	}
}

func (m *muxServer) close() {
	m.cancel()
	if m.inbound != nil {
		close(m.inbound)
		m.inbound = nil
	}
}
