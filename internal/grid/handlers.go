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

	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
)

const (
	// handlerInvalid is reserved to check for uninitialized values.
	handlerInvalid HandlerID = iota

	// Add more above here ^^^
	// If all handlers are used, the type of Handler can be changed.
	// Handlers have no versioning, so non-compatible handler changes must result in new IDs.
	handlerTest
	handlerTest2
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

func (h HandlerID) isTestHandler() bool {
	return h >= handlerTest && h <= handlerTest2
}

// RemoteErr is a remote error type.
// Any error seen on a remote will be returned like this.
type RemoteErr string

// NewRemoteErr creates a new remote error.
func NewRemoteErr(msg string) *RemoteErr {
	r := RemoteErr(msg)
	return &r
}

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

	// StreamHandler handles fully bidirectional streams,
	// There is flow control in both directions.
	StreamHandler struct {
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
	streams   [handlerLast]*StreamHandler
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
	res, err := c.Request(ctx, h.id, payload)
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

type StreamTypeHandler[Payload, Req, Resp RoundTripper] struct {
	newPayload func() Payload
	newReq     func() Req
	newResp    func() Resp
	id         HandlerID
	respReuse  func(req Resp)

	// Override the default capacities (1)
	OutCapacity int
	InCapacity  int
}

// NewStream creates a typed handler that can provide Marshal/Unmarshal.
// Use Register to register a server handler.
// Use Call to initiate a clientside call.
// newPayload can be nil. In that case payloads will always be nil.
// newReq can be nil. In that case no input stream is expected and the handler will be called with nil 'in' channel.
func NewStream[Payload, Req, Resp RoundTripper](h HandlerID, newPayload func() Payload, newReq func() Req, newResp func() Resp) *StreamTypeHandler[Payload, Req, Resp] {
	if newPayload == nil {
		panic("newPayload missing in NewStream")
	}
	if newReq == nil {
		panic("newReq missing in NewStream")
	}
	if newResp == nil {
		panic("newResp missing in NewStream")
	}
	return newStreamHandler[Payload, Req, Resp](h, newPayload, newReq, newResp)
}

func newStreamHandler[Payload, Req, Resp RoundTripper](h HandlerID, newPayload func() Payload, newReq func() Req, newResp func() Resp) *StreamTypeHandler[Payload, Req, Resp] {
	inC := 1
	if newReq == nil {
		inC = 0
	}
	return &StreamTypeHandler[Payload, Req, Resp]{id: h, newReq: newReq, newResp: newResp, newPayload: newPayload, InCapacity: inC, OutCapacity: 1}
}

// Register a handler for two-way streaming with payload, input stream and output stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) Register(m *Manager, handle func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr) error {
	if h.newPayload == nil {
		return errors.New("newPayload missing in NewStream")
	}
	if h.newReq == nil {
		return errors.New("newReq missing in NewStream")
	}
	return h.register(m, handle)
}

type StreamNoPayload[Req, Resp RoundTripper] struct {
	h *StreamTypeHandler[RoundTripper, Req, Resp]
}

func NewStreamNoPayload[Req, Resp RoundTripper](h HandlerID, newReq func() Req, newResp func() Resp) *StreamNoPayload[Req, Resp] {
	handler := NewStream[RoundTripper, Req, Resp](h, nil, newReq, newResp)
	return &StreamNoPayload[Req, Resp]{h: handler}
}

// Register registers a handler for two-way streaming with input stream and output stream.
func (h *StreamNoPayload[Req, Resp]) Register(m *Manager, handle func(in <-chan Req, out chan<- Resp) *RemoteErr) error {
	if h.h.newReq == nil {
		return errors.New("newReq missing in NewStream")
	}
	return h.h.register(m, func(_ RoundTripper, in <-chan Req, out chan<- Resp) *RemoteErr {
		return handle(in, out)
	})
}

// RegisterNoInput a handler for one-way streaming with payload and output stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) RegisterNoInput(m *Manager, handle func(p Payload, out chan<- Resp) *RemoteErr) error {
	if h.newPayload == nil {
		return errors.New("newPayload missing in NewStream")
	}
	return h.register(m, func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr {
		return handle(p, out)
	})
}

// Register a handler for two-way streaming with optional payload and input stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) register(m *Manager, handle func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr) error {
	return m.RegisterStreamingHandler(h.id, StreamHandler{
		Handle: func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- []byte) *RemoteErr {
			var plT Payload
			if h.newPayload != nil {
				plT = h.newPayload()
				_, err := plT.UnmarshalMsg(payload)
				PutByteBuffer(payload)
				if err != nil {
					r := RemoteErr(err.Error())
					return &r
				}
			}
			var inT chan Req
			if h.newReq != nil {
				// Don't add extra buffering
				inT = make(chan Req)
				go func() {
					defer close(inT)
					for {
						select {
						case <-ctx.Done():
							return
						case v := <-in:
							input := h.newReq()
							_, err := input.UnmarshalMsg(v)
							if err != nil {
								logger.LogOnceIf(ctx, err, err.Error())
							}
							PutByteBuffer(v)
							// Send input
							select {
							case <-ctx.Done():
								return
							case inT <- input:
							}
						}
					}
				}()
			}
			outT := make(chan Resp)
			outDone := make(chan struct{})
			go func() {
				defer close(outDone)
				dropOutput := false
				for v := range outT {
					if dropOutput {
						continue
					}
					dst := GetByteBuffer()
					dst, err := v.MarshalMsg(dst[:0])
					if err != nil {
						logger.LogOnceIf(ctx, err, err.Error())
					}
					if h.respReuse != nil {
						h.respReuse(v)
					}
					select {
					case <-ctx.Done():
						dropOutput = true
					case out <- dst:
					}
				}
			}()
			rErr := handle(plT, inT, outT)
			close(outT)
			<-outDone
			return rErr
		}, OutCapacity: h.OutCapacity, InCapacity: h.InCapacity,
	})
}

// SetRespReuse allows setting a request reuse function.
func (h *StreamTypeHandler[Payload, Req, Resp]) SetRespReuse(fn func(resp Resp)) {
	h.respReuse = fn
}

type TypedResponse[Resp RoundTripper] struct {
	Msg Resp
	Err error
}

type TypedSteam[Req, Resp RoundTripper] struct {
	// Responses from the remote server.
	// Channel will be closed after error or when remote closes.
	// Responses *must* be read to either an error is returned or the channel is closed.
	Responses <-chan TypedResponse[Resp]

	// Requests sent to the server.
	// If the handler is defined with 0 incoming capacity this will be nil.
	// Channel *must* be closed to signal the end of the stream.
	// If the request context is canceled, the stream will no longer process requests.
	Requests chan<- Req
}

// Call the remove with the request and
func (h *StreamTypeHandler[Payload, Req, Resp]) Call(ctx context.Context, c *Connection, payload Payload) (st *TypedSteam[Req, Resp], err error) {
	b, err := payload.MarshalMsg(GetByteBuffer()[:0])
	if err != nil {
		return nil, err
	}
	stream, err := c.NewStream(ctx, h.id, b)
	if err != nil {
		return nil, err
	}
	respT := make(chan TypedResponse[Resp])
	reqT := make(chan Req)
	// Request handler
	go func() {
		defer close(stream.Requests)
		for req := range reqT {
			b, err := req.MarshalMsg(GetByteBuffer()[:0])
			if err != nil {
				logger.LogOnceIf(ctx, err, err.Error())
			}
			stream.Requests <- b
		}
	}()
	// Response handler
	go func() {
		defer close(respT)
		errState := false
		for resp := range stream.Responses {
			if errState {
				continue
			}
			if resp.Err != nil {
				respT <- TypedResponse[Resp]{Err: resp.Err}
				errState = true
				continue
			}
			tr := TypedResponse[Resp]{Msg: h.newResp()}
			_, err := tr.Msg.UnmarshalMsg(resp.Msg)
			if err != nil {
				tr.Err = err
				errState = true
			}
			respT <- TypedResponse[Resp]{Err: resp.Err}
		}
	}()
	return &TypedSteam[Req, Resp]{Responses: respT, Requests: reqT}, nil
}
