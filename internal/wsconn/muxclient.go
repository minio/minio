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

import "context"

type MuxClient struct {
	ID       uint64
	Seq      uint32
	Resp     chan []byte
	Stateful bool
	ctx      context.Context
	parent   *Connection
}

func newMuxClient(id uint64, parent *Connection, stateful bool) *MuxClient {
	return &MuxClient{
		ID:       id,
		Seq:      0,
		Resp:     make(chan []byte, 1),
		Stateful: stateful,
		ctx:      context.TODO(),
		parent:   parent,
	}
}
