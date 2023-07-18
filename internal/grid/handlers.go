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
	"fmt"
)

const (
	// handlerInvalid is reserved to check for uninitialized values.
	handlerInvalid HandlerID = iota
	handlerTest
	handlerTest2

	// Add more above.
	// If all handlers are used, the type of Handler can be changed.
	// Handlers have no versioning, so non-compatible handler changes must result in new IDs.
	handlerLast
)

func init() {
	// Static check if we exceed 255 handler ids.
	// Extend the type to uint16 when hit.
	if handlerLast > 255 {
		panic(fmt.Sprintf("out of handler IDs. %d > %d", handlerLast, 255))
	}
}

func (h HandlerID) valid() bool {
	return h != handlerInvalid && h < handlerLast
}

// RemoteErr is a remote error type.
// Any error seen on a remote will be returned like this.
type RemoteErr string

func (r RemoteErr) Error() string {
	return string(r)
}

// TODO: Add type safe handlers and clients.
type (
	// SingleHandlerFn is handlers for one to one requests.
	// A non-nil error value will be returned as RemoteErr(msg) to client.
	SingleHandlerFn func(payload []byte) ([]byte, *RemoteErr)

	// StatelessHandlerFn must handle incoming stateless request.
	// A non-nil error value will be returned as RemoteErr(msg) to client.
	StatelessHandlerFn func(ctx context.Context, payload []byte, resp chan<- ServerResponse) *RemoteErr

	// StatelessHandler is handlers for one to many requests,
	// where responses may be dropped.
	StatelessHandler struct {
		Handle StatelessHandlerFn
		// OutCapacity is the output capacity. If <= 0 capacity will be 1.
		OutCapacity int
	}

	StreamHandlerFn func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- ServerResponse)
	// StatefulHandler handles fully bidirectional streams.

	StatefulHandler struct {
		// Handle an incoming request. Initial payload is sent.
		// Additional input packets (if any) are streamed to request.
		// Upstream will block when request channel is full.
		// Response packets can be sent at any time.
		// Any non-nil error sent as response means no more responses are sent.
		Handle StreamHandlerFn

		// OutCapacity is the output capacity. If <= 0 capacity will be 1.
		OutCapacity int

		// InCapacity is the output capacity.
		// If == 0 no input is expected
		InCapacity int
	}
)

type handlers struct {
	single    [handlerLast]SingleHandlerFn
	stateless [handlerLast]*StatelessHandler
	streams   [handlerLast]*StatefulHandler
}

func (h *handlers) hasAny(id HandlerID) bool {
	if !id.valid() {
		return false
	}
	return h.single[id] != nil || h.stateless[id] != nil || h.streams[id] != nil
}
