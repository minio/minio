// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package protorpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/rpc/v2"
)

var null = json.RawMessage([]byte("null"))

// ----------------------------------------------------------------------------
// Request and Response
// ----------------------------------------------------------------------------

// serverRequest represents a ProtoRPC request received by the server.
type serverRequest struct {
	// A String containing the name of the method to be invoked.
	Method string `json:"method"`
	// An Array of objects to pass as arguments to the method.
	Params *json.RawMessage `json:"params"`
	// The request id. This can be of any type. It is used to match the
	// response with the request that it is replying to.
	Id *json.RawMessage `json:"id"`
}

// serverResponse represents a ProtoRPC response returned by the server.
type serverResponse struct {
	// The Object that was returned by the invoked method. This must be null
	// in case there was an error invoking the method.
	Result interface{} `json:"result"`
	// An Error object if there was an error invoking the method. It must be
	// null if there was no error.
	Error interface{} `json:"error"`
	// This must be the same id as the request it is responding to.
	Id *json.RawMessage `json:"id"`
}

// ----------------------------------------------------------------------------
// Codec
// ----------------------------------------------------------------------------

// NewCodec returns a new ProtoRPC Codec.
func NewCodec() *Codec {
	return &Codec{}
}

// Codec creates a CodecRequest to process each request.
type Codec struct {
}

// NewRequest returns a CodecRequest.
func (c *Codec) NewRequest(r *http.Request) rpc.CodecRequest {
	return newCodecRequest(r)
}

// ----------------------------------------------------------------------------
// CodecRequest
// ----------------------------------------------------------------------------

// newCodecRequest returns a new CodecRequest.
func newCodecRequest(r *http.Request) rpc.CodecRequest {
	// Decode the request body and check if RPC method is valid.
	req := new(serverRequest)
	path := r.URL.Path
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return &CodecRequest{request: req, err: fmt.Errorf("rpc: no method: %s", path)}
	}
	req.Method = path[index+1:]
	err := json.NewDecoder(r.Body).Decode(&req.Params)
	r.Body.Close()
	var errr error
	if err != io.EOF {
		errr = err
	}
	return &CodecRequest{request: req, err: errr}
}

// CodecRequest decodes and encodes a single request.
type CodecRequest struct {
	request *serverRequest
	err     error
}

// Method returns the RPC method for the current request.
//
// The method uses a dotted notation as in "Service.Method".
func (c *CodecRequest) Method() (string, error) {
	if c.err == nil {
		return c.request.Method, nil
	}
	return "", c.err
}

// ReadRequest fills the request object for the RPC method.
func (c *CodecRequest) ReadRequest(args interface{}) error {
	if c.err == nil {
		if c.request.Params != nil {
			c.err = json.Unmarshal(*c.request.Params, args)
		} else {
			c.err = errors.New("rpc: method request ill-formed: missing params field")
		}
	}
	return c.err
}

// WriteResponse encodes the response and writes it to the ResponseWriter.
func (c *CodecRequest) WriteResponse(w http.ResponseWriter, reply interface{}) {
	res := &serverResponse{
		Result: reply,
		Error:  &null,
		Id:     c.request.Id,
	}
	c.writeServerResponse(w, 200, res)
}

func (c *CodecRequest) WriteError(w http.ResponseWriter, status int, err error) {
	res := &serverResponse{
		Result: &struct {
			ErrorMessage interface{} `json:"error_message"`
		}{err.Error()},
		Id: c.request.Id,
	}
	c.writeServerResponse(w, status, res)
}

func (c *CodecRequest) writeServerResponse(w http.ResponseWriter, status int, res *serverResponse) {
	b, err := json.Marshal(res.Result)
	if err == nil {
		w.WriteHeader(status)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(b)
	} else {
		// Not sure in which case will this happen. But seems harmless.
		rpc.WriteError(w, 400, err.Error())
	}
}
