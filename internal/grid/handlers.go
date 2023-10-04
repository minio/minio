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
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/minio/minio/internal/hash/sha256"
	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
)

// HandlerID is a handler identifier.
// It is used to determine request routing on the server.
// Handlers can be registered with a static subroute.
const (
	// handlerInvalid is reserved to check for uninitialized values.
	handlerInvalid HandlerID = iota
	HandlerLockLock
	HandlerLockRLock
	HandlerLockUnlock
	HandlerLockRUnlock
	HandlerLockRefresh
	HandlerLockForceUnlock
	HandlerWalkDir

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
// The error type is not preserved.
func NewRemoteErr(err error) *RemoteErr {
	if err == nil {
		return nil
	}
	r := RemoteErr(err.Error())
	return &r
}

// NewRemoteErrString creates a new remote error from a string.
func NewRemoteErrString(msg string) *RemoteErr {
	r := RemoteErr(msg)
	return &r
}

func (r RemoteErr) Error() string {
	return string(r)
}

// Is returns if the string representation matches.
func (r *RemoteErr) Is(other error) bool {
	if r == nil || other == nil {
		return r == other
	}
	var o RemoteErr
	if errors.As(other, &o) {
		return r == &o
	}
	return false
}

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

		// SubRoute for handler.
		// Subroute must be static and clients should specify a matching subroute.
		// Should not be set unless there are different handlers for the same HandlerID.
		SubRoute string

		// OutCapacity is the output capacity. If <= 0 capacity will be 1.
		OutCapacity int

		// InCapacity is the output capacity.
		// If == 0 no input is expected
		InCapacity int
	}
)

type subHandlerID [32]byte

func makeSubHandlerID(id HandlerID, subRoute string) subHandlerID {
	b := subHandlerID(sha256.Sum256([]byte(subRoute)))
	b[0] = byte(id)
	b[1] = 0 // Reserved
	return b
}

func (s subHandlerID) withHandler(id HandlerID) subHandlerID {
	s[1] = 0 // Reserved
	s[0] = byte(id)
	return s
}

func (s *subHandlerID) String() string {
	if s == nil {
		return ""
	}
	return hex.EncodeToString(s[:])
}

func makeZeroSubHandlerID(id HandlerID) subHandlerID {
	return subHandlerID{byte(id)}
}

type handlers struct {
	single    [handlerLast]SingleHandlerFn
	stateless [handlerLast]*StatelessHandler
	streams   [handlerLast]*StreamHandler

	subSingle    map[subHandlerID]SingleHandlerFn
	subStateless map[subHandlerID]*StatelessHandler
	subStreams   map[subHandlerID]*StreamHandler
}

func (h *handlers) init() {
	h.subSingle = make(map[subHandlerID]SingleHandlerFn)
	h.subStateless = make(map[subHandlerID]*StatelessHandler)
	h.subStreams = make(map[subHandlerID]*StreamHandler)
}

func (h *handlers) hasAny(id HandlerID) bool {
	if !id.valid() {
		return false
	}
	return h.single[id] != nil || h.stateless[id] != nil || h.streams[id] != nil
}

func (h *handlers) hasSubhandler(id subHandlerID) bool {
	return h.subSingle[id] != nil || h.subStateless[id] != nil || h.subStreams[id] != nil
}

// RoundTripper provides an interface for type roundtrip serialization.
type RoundTripper interface {
	msgp.Unmarshaler
	msgp.Marshaler
}

// SingleHandler is a type safe handler for single roundtrip requests.
type SingleHandler[Req, Resp RoundTripper] struct {
	id HandlerID

	reqPool  sync.Pool
	respPool sync.Pool
}

// NewSingleRTHandler creates a typed handler that can provide Marshal/Unmarshal.
// Use Register to register a server handler.
// Use Call to initiate a clientside call.
func NewSingleRTHandler[Req, Resp RoundTripper](h HandlerID, newReq func() Req, newResp func() Resp) *SingleHandler[Req, Resp] {
	s := SingleHandler[Req, Resp]{id: h}
	s.reqPool.New = func() interface{} {
		return newReq()
	}
	s.respPool.New = func() interface{} {
		return newResp()
	}
	return &s
}

// PutResponse will accept a request for reuse.
// These should be returned by the caller.
func (h *SingleHandler[Req, Resp]) PutResponse(r Resp) {
	h.respPool.Put(r)
}

// NewResponse creates a new response.
// Handlers can use this to create a reusable response.
// The response may be reused, so caller should clear any fields.
func (h *SingleHandler[Req, Resp]) NewResponse() Resp {
	return h.respPool.Get().(Resp)
}

// PutRequest will accept a request for reuse.
// These should be returned by the caller.
func (h *SingleHandler[Req, Resp]) PutRequest(r Req) {
	h.reqPool.Put(r)
}

// NewRequest creates a new response.
// Handlers can use this to create a reusable response.
// The request may be reused, so caller should clear any fields.
func (h *SingleHandler[Req, Resp]) NewRequest() Req {
	return h.reqPool.Get().(Req)
}

// Register a handler for a Req -> Resp roundtrip.
func (h *SingleHandler[Req, Resp]) Register(m *Manager, handle func(req Req) (resp Resp, err *RemoteErr)) error {
	return m.RegisterSingle(h.id, func(payload []byte) ([]byte, *RemoteErr) {
		req := h.NewRequest()
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
		h.PutRequest(req)
		payload, err = resp.MarshalMsg(payload[:0])
		h.PutResponse(resp)
		if err != nil {
			PutByteBuffer(payload)
			r := RemoteErr(err.Error())
			return nil, &r
		}
		return payload, nil
	})
}

// Call the remove with the request and return the response.
// The response should be returned with PutResponse when no error.
func (h *SingleHandler[Req, Resp]) Call(ctx context.Context, c *Connection, req Req) (resp Resp, err error) {
	payload, err := req.MarshalMsg(GetByteBuffer()[:0])
	if err != nil {
		return resp, err
	}
	res, err := c.Request(ctx, h.id, payload)
	PutByteBuffer(payload)
	if err != nil {
		return resp, err
	}
	r := h.NewResponse()
	_, err = r.UnmarshalMsg(res)
	if err != nil {
		h.PutResponse(r)
		return resp, err
	}
	PutByteBuffer(res)
	return r, err
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

// StreamTypeHandler is a type safe handler for streaming requests.
type StreamTypeHandler[Payload, Req, Resp RoundTripper] struct {
	reqPool    sync.Pool
	respPool   sync.Pool
	id         HandlerID
	newPayload func() Payload

	WithPayload bool

	// Override the default capacities (1)
	OutCapacity int

	// Set to 0 if no input is expected.
	// Will be 0 if newReq is nil.
	InCapacity int
}

// NewStream creates a typed handler that can provide Marshal/Unmarshal.
// Use Register to register a server handler.
// Use Call to initiate a clientside call.
// newPayload can be nil. In that case payloads will always be nil.
// newReq can be nil. In that case no input stream is expected and the handler will be called with nil 'in' channel.
func NewStream[Payload, Req, Resp RoundTripper](h HandlerID, newPayload func() Payload, newReq func() Req, newResp func() Resp) *StreamTypeHandler[Payload, Req, Resp] {
	if newResp == nil {
		panic("newResp missing in NewStream")
	}

	s := newStreamHandler[Payload, Req, Resp](h)
	if newReq != nil {
		s.reqPool.New = func() interface{} {
			return newReq()
		}
	} else {
		s.InCapacity = 0
	}
	s.respPool.New = func() interface{} {
		return newResp()
	}
	s.newPayload = newPayload
	s.WithPayload = newPayload != nil
	return s
}

// NewPayload creates a new payload.
func (h *StreamTypeHandler[Payload, Req, Resp]) NewPayload() Payload {
	return h.newPayload()
}

// NewRequest creates a new request.
// The struct may be reused, so caller should clear any fields.
func (h *StreamTypeHandler[Payload, Req, Resp]) NewRequest() Req {
	return h.reqPool.Get().(Req)
}

// PutRequest will accept a request for reuse.
// These should be returned by the handler.
func (h *StreamTypeHandler[Payload, Req, Resp]) PutRequest(r Req) {
	h.reqPool.Put(r)
}

// PutResponse will accept a request for reuse.
// These should be returned by the caller.
func (h *StreamTypeHandler[Payload, Req, Resp]) PutResponse(r Resp) {
	h.respPool.Put(r)
}

// NewResponse creates a new response.
// Handlers can use this to create a reusable response.
func (h *StreamTypeHandler[Payload, Req, Resp]) NewResponse() Resp {
	return h.respPool.Get().(Resp)
}

func newStreamHandler[Payload, Req, Resp RoundTripper](h HandlerID) *StreamTypeHandler[Payload, Req, Resp] {
	return &StreamTypeHandler[Payload, Req, Resp]{id: h, InCapacity: 1, OutCapacity: 1}
}

// Register a handler for two-way streaming with payload, input stream and output stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) Register(m *Manager, handle func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr) error {
	return h.register(m, handle)
}

// RegisterNoInput a handler for one-way streaming with payload and output stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) RegisterNoInput(m *Manager, handle func(p Payload, out chan<- Resp) *RemoteErr) error {
	h.InCapacity = 0
	return h.register(m, func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr {
		return handle(p, out)
	})
}

// RegisterNoPayload a handler for one-way streaming with payload and output stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) RegisterNoPayload(m *Manager, handle func(in <-chan Req, out chan<- Resp) *RemoteErr) error {
	h.WithPayload = false
	return h.register(m, func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr {
		return handle(in, out)
	})
}

// Register a handler for two-way streaming with optional payload and input stream.
func (h *StreamTypeHandler[Payload, Req, Resp]) register(m *Manager, handle func(p Payload, in <-chan Req, out chan<- Resp) *RemoteErr) error {
	return m.RegisterStreamingHandler(h.id, StreamHandler{
		Handle: func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- []byte) *RemoteErr {
			var plT Payload
			if h.WithPayload {
				plT = h.NewPayload()
				_, err := plT.UnmarshalMsg(payload)
				PutByteBuffer(payload)
				if err != nil {
					r := RemoteErr(err.Error())
					return &r
				}
			}

			var inT chan Req
			if h.InCapacity > 0 {
				// Don't add extra buffering
				inT = make(chan Req)
				go func() {
					defer close(inT)
					for {
						select {
						case <-ctx.Done():
							return
						case v, ok := <-in:
							if !ok {
								return
							}
							input := h.NewRequest()
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
					h.PutResponse(v)
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

// TypedResponse is a response from the server.
// It will either contain a response or an error.
type TypedResponse[Resp RoundTripper] struct {
	Msg Resp
	Err error
}

// TypedSteam is a stream with specific types.
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
	var payloadB []byte
	if h.WithPayload {
		var err error
		payloadB, err = payload.MarshalMsg(GetByteBuffer()[:0])
		if err != nil {
			return nil, err
		}
	}
	stream, err := c.NewStream(ctx, h.id, payloadB)
	if err != nil {
		return nil, err
	}
	respT := make(chan TypedResponse[Resp])
	var reqT chan Req
	if h.InCapacity > 0 {
		reqT = make(chan Req)
		// Request handler
		go func() {
			defer close(stream.Requests)
			for req := range reqT {
				b, err := req.MarshalMsg(GetByteBuffer()[:0])
				if err != nil {
					logger.LogOnceIf(ctx, err, err.Error())
				}
				h.PutRequest(req)
				stream.Requests <- b
			}
		}()
	} else {
		close(stream.Requests)
	}
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
			tr := TypedResponse[Resp]{Msg: h.NewResponse()}
			_, err := tr.Msg.UnmarshalMsg(resp.Msg)
			if err != nil {
				tr.Err = err
				errState = true
			}
			respT <- tr
		}
	}()
	return &TypedSteam[Req, Resp]{Responses: respT, Requests: reqT}, nil
}

// NoPayload is a type that can be used for handlers that do not use a payload.
type NoPayload struct{}

// UnmarshalMsg satisfies the interface, but is a no-op.
func (NoPayload) UnmarshalMsg(bytes []byte) ([]byte, error) {
	return bytes, nil
}

// MarshalMsg satisfies the interface, but is a no-op.
func (NoPayload) MarshalMsg(bytes []byte) ([]byte, error) {
	return bytes, nil
}
