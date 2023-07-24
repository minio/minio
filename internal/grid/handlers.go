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

	"github.com/tinylib/msgp/msgp"
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

// Is returns if the string representation matches.
func (r *RemoteErr) Is(other *RemoteErr) bool {
	if r == nil || other == nil {
		return r == other
	}
	return *r == *other
}

// TODO: Add type safe handlers and clients.
type (
	// SingleHandlerFn is handlers for one to one requests.
	// A non-nil error value will be returned as RemoteErr(msg) to client.
	// No client information or cancellation (deadline) is available.
	// Include this in payload if needed.
	SingleHandlerFn func(payload []byte) ([]byte, *RemoteErr)

	// StatelessHandlerFn must handle incoming stateless request.
	// A non-nil error value will be returned as RemoteErr(msg) to client.
	StatelessHandlerFn func(ctx context.Context, payload []byte, resp chan<- []byte) *RemoteErr

	// StatelessHandler is handlers for one to many requests,
	// where responses may be dropped.
	// Stateless requests provide no incoming stream and there is no flow control
	// on outgoing messages.
	StatelessHandler struct {
		Handle StatelessHandlerFn
		// OutCapacity is the output capacity on the caller.
		// If <= 0 capacity will be 1.
		OutCapacity int
	}

	// StreamHandlerFn must process a request with an optional initial payload.
	// It must keep consuming from 'in' until it returns.
	// 'in' and 'out' are independent.
	// The handler should never close out.
	// Buffers received from 'in'  can be recycled with PutByteBuffer.
	StreamHandlerFn func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- []byte) *RemoteErr

	// StatefulHandler handles fully bidirectional streams,
	// There is flow control in both directions.
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

// RoundTripper provides an interface for type roundtrip serialization.
type RoundTripper interface {
	msgp.Unmarshaler
	msgp.Marshaler
}

type SingleHandler[Req, Resp RoundTripper] struct {
	newReq    func() Req
	newResp   func() Resp
	id        HandlerID
	reqReuse  func(req Req)
	respReuse func(req Resp)
}

// NewSingleRTHandler creates a typed handler that can provide Marshal/Unmarshal.
// Use Register to register a server handler.
// Use Call to initiate a clientside call.
func NewSingleRTHandler[Req, Resp RoundTripper](h HandlerID, newReq func() Req, newResp func() Resp) *SingleHandler[Req, Resp] {
	return &SingleHandler[Req, Resp]{id: h, newReq: newReq, newResp: newResp}
}

// Register a handler for a Req -> Resp roundtrip.
func (h *SingleHandler[Req, Resp]) Register(m *Manager, handle func(req Req) (resp Resp, err *RemoteErr)) error {
	return m.RegisterSingle(h.id, func(payload []byte) ([]byte, *RemoteErr) {
		req := h.newReq()
		_, err := req.UnmarshalMsg(payload)
		if err != nil {
			PutByteBuffer(payload)
			r := RemoteErr(err.Error())
			return nil, &r
		}
		resp, rerr := handle(req)
		if rerr != nil {
			PutByteBuffer(payload)
			return nil, rerr
		}
		if h.reqReuse != nil {
			h.reqReuse(req)
		}
		payload, err = resp.MarshalMsg(payload[:0])
		if h.respReuse != nil {
			h.respReuse(resp)
		}
		if err != nil {
			PutByteBuffer(payload)
			r := RemoteErr(err.Error())
			return nil, &r
		}
		return payload, nil
	})
}

// SetReqReuse allows setting a request reuse function.
func (h *SingleHandler[Req, Resp]) SetReqReuse(fn func(req Req)) {
	h.reqReuse = fn
}

// SetRespReuse allows setting a request reuse function.
func (h *SingleHandler[Req, Resp]) SetRespReuse(fn func(resp Resp)) {
	h.respReuse = fn
}

// Call the remove with the request and
func (h *SingleHandler[Req, Resp]) Call(ctx context.Context, c *Connection, req Req) (dst Resp, err error) {
	payload, err := req.MarshalMsg(GetByteBuffer()[:0])
	if err != nil {
		return dst, err
	}
	res, err := c.Single(ctx, h.id, payload)
	if err != nil {
		return dst, err
	}
	dst = h.newResp()
	_, err = dst.UnmarshalMsg(res)
	PutByteBuffer(res)
	return dst, err
}

// RemoteClient contains information about the caller.
type RemoteClient struct {
	Name string
}

type ctxCallerKey = struct{}

// GetCaller returns caller information from contexts provided to handlers.
func GetCaller(ctx context.Context) *RemoteClient {
	val, _ := ctx.Value(ctxCallerKey{}).(*RemoteClient)
	return val
}

func setCaller(ctx context.Context, cl *RemoteClient) context.Context {
	return context.WithValue(ctx, ctxCallerKey{}, cl)
}
