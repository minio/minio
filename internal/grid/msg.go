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
	"strings"
	"time"

	"github.com/tinylib/msgp/msgp"
	"github.com/zeebo/xxh3"
)

// Op is operation type.
//
//go:generate msgp -unexported -file=$GOFILE
//go:generate stringer -type=Op -output=msg_string.go -trimprefix=Op $GOFILE

// Op is operation type messages.
type Op uint8

// HandlerID is the ID for the handler of a specific type.
type HandlerID uint8

const (
	// OpConnect is a connect request.
	OpConnect Op = iota + 1

	// OpConnectResponse is a response to a connect request.
	OpConnectResponse

	// OpPing is a ping request.
	// If a mux id is specified that mux is pinged.
	// Clients send ping requests.
	OpPing

	// OpPong is a OpPing response returned by the server.
	OpPong

	// OpConnectMux will connect a new mux with optional payload.
	OpConnectMux

	// OpMuxConnectError is an  error while connecting a mux.
	OpMuxConnectError

	// OpDisconnectClientMux instructs a client to disconnect a mux
	OpDisconnectClientMux

	// OpDisconnectServerMux instructs a server to disconnect (cancel) a server mux
	OpDisconnectServerMux

	// OpMuxClientMsg contains a message to a client Mux
	OpMuxClientMsg

	// OpMuxServerMsg contains a message to a server Mux
	OpMuxServerMsg

	// OpUnblockSrvMux contains a message that a server mux is unblocked with one.
	// Only Stateful streams has flow control.
	OpUnblockSrvMux

	// OpUnblockClMux contains a message that a client mux is unblocked with one.
	// Only Stateful streams has flow control.
	OpUnblockClMux

	// OpAckMux acknowledges a mux was created.
	OpAckMux

	// OpRequest is a single request + response.
	// MuxID is returned in response.
	OpRequest

	// OpResponse is a response to a single request.
	// FlagPayloadIsErr is used to signify that the payload is a string error converted to byte slice.
	// When a response is received, the mux is already removed from the remote.
	OpResponse

	// OpDisconnect instructs that remote wants to disconnect
	OpDisconnect

	// OpMerged is several operations merged into one.
	OpMerged
)

const (
	// FlagCRCxxh3 indicates that, the lower 32 bits of xxhash3 of the serialized
	// message will be sent after the serialized message as little endian.
	FlagCRCxxh3 Flags = 1 << iota

	// FlagEOF the stream (either direction) is at EOF.
	FlagEOF

	// FlagStateless indicates the message is stateless.
	// This will retain clients across reconnections or
	// if sequence numbers are unexpected.
	FlagStateless

	// FlagPayloadIsErr can be used by individual ops to signify that
	// The payload is a string error converted to byte slice.
	FlagPayloadIsErr

	// FlagPayloadIsZero means that payload is 0-length slice and not nil.
	FlagPayloadIsZero

	// FlagSubroute indicates that the message has subroute.
	// Subroute will be 32 bytes long and added before any CRC.
	FlagSubroute
)

// This struct cannot be changed and retain backwards compatibility.
// If changed, endpoint version must be bumped.
//
//msgp:tuple message
type message struct {
	MuxID      uint64    // Mux to receive message if any.
	Seq        uint32    // Sequence number.
	DeadlineMS uint32    // If non-zero, milliseconds until deadline (max 1193h2m47.295s, ~49 days)
	Handler    HandlerID // ID of handler if invoking a remote handler.
	Op         Op        // Operation. Other fields change based on this value.
	Flags      Flags     // Optional flags.
	Payload    []byte    // Optional payload.
}

// Flags is a set of flags set on a message.
type Flags uint8

func (m message) String() string {
	var res []string
	if m.MuxID != 0 {
		res = append(res, fmt.Sprintf("MuxID: %v", m.MuxID))
	}
	if m.Seq != 0 {
		res = append(res, fmt.Sprintf("Seq: %v", m.Seq))
	}
	if m.DeadlineMS != 0 {
		res = append(res, fmt.Sprintf("Deadline: %vms", m.DeadlineMS))
	}
	if m.Handler != handlerInvalid {
		res = append(res, fmt.Sprintf("Handler: %v", m.Handler))
	}
	if m.Op != 0 {
		res = append(res, fmt.Sprintf("Op: %v", m.Op))
	}
	res = append(res, fmt.Sprintf("Flags: %s", m.Flags.String()))
	if len(m.Payload) != 0 {
		res = append(res, fmt.Sprintf("Payload: %v", bytesOrLength(m.Payload)))
	}
	return "{" + strings.Join(res, ", ") + "}"
}

func (f Flags) String() string {
	var res []string
	if f&FlagCRCxxh3 != 0 {
		res = append(res, "CRC")
	}
	if f&FlagEOF != 0 {
		res = append(res, "EOF")
	}
	if f&FlagStateless != 0 {
		res = append(res, "SL")
	}
	if f&FlagPayloadIsErr != 0 {
		res = append(res, "ERR")
	}
	if f&FlagPayloadIsZero != 0 {
		res = append(res, "ZERO")
	}
	if f&FlagSubroute != 0 {
		res = append(res, "SUB")
	}
	return "[" + strings.Join(res, ",") + "]"
}

// Set one or more flags on f.
func (f *Flags) Set(flags Flags) {
	*f |= flags
}

// Clear one or more flags on f.
func (f *Flags) Clear(flags Flags) {
	*f &^= flags
}

// parse an incoming message.
func (m *message) parse(b []byte) (*subHandlerID, []byte, error) {
	var sub *subHandlerID
	if m.Payload == nil {
		m.Payload = GetByteBuffer()[:0]
	}
	h, err := m.UnmarshalMsg(b)
	if err != nil {
		return nil, nil, fmt.Errorf("read write: %v", err)
	}
	if len(m.Payload) == 0 && m.Flags&FlagPayloadIsZero == 0 {
		PutByteBuffer(m.Payload)
		m.Payload = nil
	}
	if m.Flags&FlagCRCxxh3 != 0 {
		const hashLen = 4
		if len(h) < hashLen {
			return nil, nil, fmt.Errorf("want crc len 4, got %v", len(h))
		}
		got := uint32(xxh3.Hash(b[:len(b)-hashLen]))
		want := binary.LittleEndian.Uint32(h[len(h)-hashLen:])
		if got != want {
			return nil, nil, fmt.Errorf("crc mismatch: 0x%08x (given) != 0x%08x (bytes)", want, got)
		}
		h = h[:len(h)-hashLen]
	}
	// Extract subroute if any.
	if m.Flags&FlagSubroute != 0 {
		if len(h) < 32 {
			return nil, nil, fmt.Errorf("want subroute len 32, got %v", len(h))
		}
		subID := (*[32]byte)(h[len(h)-32:])
		sub = (*subHandlerID)(subID)
		// Add if more modifications to h is needed
		h = h[:len(h)-32]
	}
	return sub, h, nil
}

// setZeroPayloadFlag will clear or set the FlagPayloadIsZero if
// m.Payload is length 0, but not nil.
func (m *message) setZeroPayloadFlag() {
	m.Flags &^= FlagPayloadIsZero
	if len(m.Payload) == 0 && m.Payload != nil {
		m.Flags |= FlagPayloadIsZero
	}
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
	ID    [16]byte
	Host  string
	Time  time.Time
	Token string
}

// addToken will add the token to the connect request.
func (c *connectReq) addToken(fn AuthFn) {
	c.Token = fn()
}

func (connectReq) Op() Op {
	return OpConnect
}

type connectResp struct {
	ID             [16]byte
	Accepted       bool
	RejectedReason string
}

func (connectResp) Op() Op {
	return OpConnectResponse
}

type muxConnectError struct {
	Error string
}

func (muxConnectError) Op() Op {
	return OpMuxConnectError
}

type pongMsg struct {
	NotFound bool      `msg:"nf"`
	Err      *string   `msg:"e,allownil"`
	T        time.Time `msg:"t"`
}

func (pongMsg) Op() Op {
	return OpPong
}

type pingMsg struct {
	T time.Time `msg:"t"`
}

func (pingMsg) Op() Op {
	return OpPing
}
