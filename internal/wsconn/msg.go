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

//go:generate msgp -unexported -file=$GOFILE

type Op uint8

const (
	OpConnect Op = iota + 1
	OpConnectResponse
	OpDisconnect
	OpConnectMux
	OpDisconnectMux
	OpMsg
	OpUnblock
	OpError
)

const (
	// FlagCRCxxh3 indicates that, the lower 32 bits of xxhash3
	// of the packet content will be sent after the message.
	FlagCRCxxh3 = 1 << iota
)

//msgp:tuple message
type message struct {
	Op      Op
	MuxID   uint64
	Seq     uint32
	Flags   uint8
	Payload []byte
}
