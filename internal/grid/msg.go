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
	"encoding/binary"
	"fmt"

	"github.com/tinylib/msgp/msgp"
	"github.com/zeebo/xxh3"
)

// Op is operation type.
//
//go:generate msgp -unexported -file=$GOFILE
//go:generate stringer -type=Op -output=msg_string.go -trimprefix=Op $GOFILE

type Op uint8

// HandlerID is the ID for the handler of a specific type.
type HandlerID uint8

const (
	OpConnect Op = iota + 1
	OpConnectResponse

	// OpPing is a ping request.
	// If a mux id is specified that mux is pinged.
	// Clients sends ping requests.
	OpPing

	// OpPong is a OpPing response returned by the server.
	OpPong

	// OpConnectMux will connect a new mux with optional payload.
	OpConnectMux

	// OpMuxConnectError is an  error while connecting a mux.
	OpMuxConnectError

	// OpDisconnectMux should disconnect a mux
	OpDisconnectMux

	// OpMuxClientMsg contains a message to a client Mux
	OpMuxClientMsg

	// OpMuxServerMsg contains a message to a server Mux
	OpMuxServerMsg

	// OpUnblockMux contains a message that a mux is unblocked.
	// Only Stateful streams has flow control.
	OpUnblockMux

	// OpAckMux acknowledges a mux was created.
	OpAckMux

	// OpDisconnect instructs that remote wants to disconnect
	OpDisconnect
)

const (
	// FlagCRCxxh3 indicates that, the lower 32 bits of xxhash3 of the serialized
	// message will be sent after the serialized message as little endian.
	FlagCRCxxh3 = 1 << iota

	// FlagEOF the stream (either direction) is at EOF.
	FlagEOF

	// FlagStateless indicates the message is stateless.
	// This will retain clients across reconnections or
	// if sequence numbers are unexpected.
	FlagStateless

	// FlagPayloadIsErr can be used by individual ops to signify that
	// The payload is a string error converted to byte slice.
	FlagPayloadIsErr
)

// This struct cannot be changed and retain backwards compatibility.
//
//msgp:tuple message
type message struct {
	MuxID   uint64
	Seq     uint32
	Handler HandlerID
	Op      Op
	Flags   uint8
	Payload []byte
}

// parse an handleIncoming message
func (m *message) parse(b []byte) error {
	if m.Payload == nil {
		m.Payload = GetByteBuffer()[:0]
	}
	h, err := m.UnmarshalMsg(b)
	if err != nil {
		return fmt.Errorf("read write: %v", err)
	}
	if m.Flags&FlagCRCxxh3 != 0 {
		if len(h) < 4 {
			return fmt.Errorf("want crc len 4, got %v", len(h))
		}
		got := uint32(xxh3.Hash(b[:len(b)-len(h)]))
		want := binary.LittleEndian.Uint32(h)
		if got != want {
			return fmt.Errorf("crc mismatch: %08x (given) != %08x (bytes)", want, got)
		}
	}
	return nil
}

func (m *message) setPayload(s sender) error {
	if len(m.Payload) > 0 {
		m.Payload = m.Payload[:0]
	} else {
		m.Payload = GetByteBuffer()[:0]
	}
	m.Op = s.Op()
	var err error
	m.Payload, err = s.MarshalMsg(m.Payload)
	return err
}

type receiver interface {
	msgp.Unmarshaler
	Op() Op
}

type sender interface {
	msgp.MarshalSizer
	Op() Op
}

type connectReq struct {
	ID   [16]byte
	Host string
}

func (_ connectReq) Op() Op {
	return OpConnect
}

type connectResp struct {
	ID             [16]byte
	Accepted       bool
	RejectedReason string
}

func (_ connectResp) Op() Op {
	return OpConnectResponse
}

type muxConnectError struct {
	Error string
}

func (_ muxConnectError) Op() Op {
	return OpMuxConnectError
}

type pongMsg struct {
	NotFound bool    `msg:"f"`
	Err      *string `msg:"e,allownil"`
}

func (_ pongMsg) Op() Op {
	return OpPong
}
