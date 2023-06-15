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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/zeebo/xxh3"
)

// Op is operation type.
//
//go:generate msgp -unexported -file=$GOFILE
//go:generate stringer -type=Op -output=msg_string.go -trimprefix=Op $GOFILE

type Op uint8

// HandlerID is the ID for the handler of a specific type.
type HandlerID uint16

const (
	OpConnect Op = iota + 1

	// OpRequest is a single request + response.
	// SeqID is used for Handler ID.
	// MuxID is returned in response.
	OpRequest

	// OpResponse is a single response from a request.
	// MuxID will match a response.
	OpResponse

	// OpConnectMux will connect a new mux with optional payload.
	OpConnectMux
	OpDisconnectMux
	OpMsg
	OpUnblock
	OpError
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
)

// This struct cannot be changed and retain backwards compatibility.
//
//msgp:tuple message
type message struct {
	Op      Op
	MuxID   uint64
	Seq     uint32
	Handler HandlerID
	Flags   uint8
	Payload []byte
}

// parse an incoming message
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

var internalByteBuffer = sync.Pool{
	New: func() any { return make([]byte, 0, 4096) },
}

// GetByteBuffer can be replaced with a function that returns a small
// byte buffer.
// When replacing PutByteBuffer should also be replaced
// There is no minimum size.
var GetByteBuffer = func() []byte {
	return internalByteBuffer.Get().([]byte)
}

// PutByteBuffer is for returning byte buffers.
var PutByteBuffer = func(b []byte) {
	if cap(b) > 1024 && cap(b) < 64<<10 {
		internalByteBuffer.Put(b)
	}
}
