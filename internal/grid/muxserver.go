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
		// TODO: Handle
	}()

	return &m
}

func newMuxStream(ctx context.Context, msg message, c *Connection, handler StatefulHandler) *muxServer {
	ctx, cancel := context.WithCancel(ctx)
	send := make(chan ServerResponse)
	inboundCap, outboundCap := handler.InCapacity, handler.OutCapacity
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
		inbound:  nil,
		outBlock: make(chan struct{}, outboundCap),
		LastPing: time.Now().Unix(),
	}
	if inboundCap > 0 {
		m.inbound = make(chan []byte, inboundCap)
	}
	for i := 0; i < outboundCap; i++ {
		m.outBlock <- struct{}{}
	}
	go func() {
		defer m.close()
		handler.Handle(ctx, msg.Payload, m.inbound, send)
	}()
	go func() {
		defer m.parent.deleteMux(true, m.ID)
		for {
			select {
			case <-ctx.Done():
				return
			// Process outgoing message.
			case send, ok := <-send:
				<-m.outBlock
				msg := message{
					MuxID: m.ID,
				}
				if send.Err != nil {
					msg.Flags |= FlagEOF | FlagPayloadIsErr
					msg.Op = OpMuxServerMsg
					msg.Payload = []byte(*send.Err)
					m.send(msg)
					return
				}
				if !ok {
					msg.Flags |= FlagEOF
				}
				msg.Op = OpMuxServerMsg
				msg.Payload = send.Msg
				m.send(msg)
				if !ok {
					return
				}
			}
		}
	}()

	return &m
}

func (m *muxServer) message(msg message) {
	if debugPrint {
		fmt.Printf("muxServer: recevied message %d, length %d\n", msg.Seq, len(msg.Payload))
	}
	if m.inbound == nil {
		m.disconnect("did not expect inbound message")
		return
	}
	wantSeq := m.RecvSeq + 1
	if msg.Seq != wantSeq {
		m.disconnect("receive sequence number mismatch")
		return
	}

	m.RecvSeq++
	if msg.Flags&FlagEOF != 0 {
		m.close()
		PutByteBuffer(msg.Payload)
		return
	}
	select {
	case <-m.ctx.Done():
	case m.inbound <- msg.Payload:
		if debugPrint {
			fmt.Printf("muxServer: Sent seq %d to handler\n", msg.Seq)
		}
		m.send(message{Op: OpUnblockClMux, MuxID: m.ID})
	default:
		go func() {
			select {
			case <-m.ctx.Done():
			case m.inbound <- msg.Payload:
				m.send(message{Op: OpUnblockClMux, MuxID: m.ID})
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
	if debugPrint {
		fmt.Println("Mux", m.ID, "disconnecting. Reason:", msg)
	}
	if msg != "" {
		m.send(message{Op: OpMuxServerMsg, MuxID: m.ID, Flags: FlagPayloadIsErr | FlagEOF, Payload: []byte(msg)})
	} else {
		m.send(message{Op: OpDisconnectMux, MuxID: m.ID})
	}
	m.close()
	m.parent.deleteMux(true, m.ID)
}

func (m *muxServer) send(msg message) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	m.SendSeq++
	msg.MuxID = m.ID
	msg.Seq = m.SendSeq
	if debugPrint {
		fmt.Printf("Mux %d, Sending %+v\n", m.ID, msg)
	}
	logger.LogIf(m.ctx, m.parent.queueMsg(msg, nil))
}

func (m *muxServer) close() {
	m.cancel()
	if m.inbound != nil {
		close(m.inbound)
		m.inbound = nil
	}
}
