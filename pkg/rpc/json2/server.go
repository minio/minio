// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2020 MinIO, Inc. All rights reserved.
// forked from https://github.com/gorilla/rpc/v2
// modified to be used with MinIO under Apache
// 2.0 license that can be found in the LICENSE file.

package json2

import (
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/pkg/rpc"
)

var null = jsoniter.RawMessage([]byte("null"))
var Version = "2.0"

// ----------------------------------------------------------------------------
// Request and Response
// ----------------------------------------------------------------------------

// serverRequest represents a JSON-RPC request received by the server.
type serverRequest struct {
	// JSON-RPC protocol.
	Version string `json:"jsonrpc"`

	// A String containing the name of the method to be invoked.
	Method string `json:"method"`

	// A Structured value to pass as arguments to the method.
	Params *jsoniter.RawMessage `json:"params"`

	// The request id. MUST be a string, number or null.
	// Our implementation will not do type checking for id.
	// It will be copied as it is.
	ID *jsoniter.RawMessage `json:"id"`
}

// serverResponse represents a JSON-RPC response returned by the server.
type serverResponse struct {
	// JSON-RPC protocol.
	Version string `json:"jsonrpc"`

	// The Object that was returned by the invoked method. This must be null
	// in case there was an error invoking the method.
	// As per spec the member will be omitted if there was an error.
	Result interface{} `json:"result,omitempty"`

	// An Error object if there was an error invoking the method. It must be
	// null if there was no error.
	// As per spec the member will be omitted if there was no error.
	Error *Error `json:"error,omitempty"`

	// This must be the same id as the request it is responding to.
	ID *jsoniter.RawMessage `json:"id"`
}

// ----------------------------------------------------------------------------
// Codec
// ----------------------------------------------------------------------------

// NewCustomCodec returns a new JSON Codec based on passed encoder selector.
func NewCustomCodec(encSel rpc.EncoderSelector) *Codec {
	return &Codec{encSel: encSel}
}

// NewCustomCodecWithErrorMapper returns a new JSON Codec based on the passed encoder selector
// and also accepts an errorMapper function.
// The errorMapper function will be called if the Service implementation returns an error, with that
// error as a param, replacing it by the value returned by this function. This function is intended
// to decouple your service implementation from the codec itself, making possible to return abstract
// errors in your service, and then mapping them here to the JSON-RPC error codes.
func NewCustomCodecWithErrorMapper(encSel rpc.EncoderSelector, errorMapper func(error) error) *Codec {
	return &Codec{
		encSel:      encSel,
		errorMapper: errorMapper,
	}
}

// NewCodec returns a new JSON Codec.
func NewCodec() *Codec {
	return NewCustomCodec(rpc.DefaultEncoderSelector)
}

// Codec creates a CodecRequest to process each request.
type Codec struct {
	encSel      rpc.EncoderSelector
	errorMapper func(error) error
}

// NewRequest returns a CodecRequest.
func (c *Codec) NewRequest(r *http.Request) rpc.CodecRequest {
	return newCodecRequest(r, c.encSel.Select(r), c.errorMapper)
}

// ----------------------------------------------------------------------------
// CodecRequest
// ----------------------------------------------------------------------------

// newCodecRequest returns a new CodecRequest.
func newCodecRequest(r *http.Request, encoder rpc.Encoder, errorMapper func(error) error) rpc.CodecRequest {
	// Decode the request body and check if RPC method is valid.
	req := new(serverRequest)
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.NewDecoder(r.Body).Decode(req)

	if err != nil {
		err = &Error{
			Code:    E_PARSE,
			Message: err.Error(),
			Data:    req,
		}
	} else if req.Version != Version {
		err = &Error{
			Code:    E_INVALID_REQ,
			Message: "jsonrpc must be " + Version,
			Data:    req,
		}
	}

	r.Body.Close()
	return &CodecRequest{request: req, err: err, encoder: encoder, errorMapper: errorMapper}
}

// CodecRequest decodes and encodes a single request.
type CodecRequest struct {
	request     *serverRequest
	err         error
	encoder     rpc.Encoder
	errorMapper func(error) error
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
//
// ReadRequest parses request parameters in two supported forms in
// accordance with http://www.jsonrpc.org/specification#parameter_structures
//
// by-position: params MUST be an Array, containing the
// values in the Server expected order.
//
// by-name: params MUST be an Object, with member names
// that match the Server expected parameter names. The
// absence of expected names MAY result in an error being
// generated. The names MUST match exactly, including
// case, to the method's expected parameters.
func (c *CodecRequest) ReadRequest(args interface{}) error {
	if c.err == nil && c.request.Params != nil {
		// Note: if c.request.Params is nil it's not an error, it's an optional member.
		// JSON params structured object. Unmarshal to the args object.
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		if err := json.Unmarshal(*c.request.Params, args); err != nil {
			// Clearly JSON params is not a structured object,
			// fallback and attempt an unmarshal with JSON params as
			// array value and RPC params is struct. Unmarshal into
			// array containing the request struct.
			params := [1]interface{}{args}
			if err = json.Unmarshal(*c.request.Params, &params); err != nil {
				c.err = &Error{
					Code:    E_INVALID_REQ,
					Message: err.Error(),
					Data:    c.request.Params,
				}
			}
		}
	}
	return c.err
}

// WriteResponse encodes the response and writes it to the ResponseWriter.
func (c *CodecRequest) WriteResponse(w http.ResponseWriter, reply interface{}) {
	res := &serverResponse{
		Version: Version,
		Result:  reply,
		ID:      c.request.ID,
	}
	c.writeServerResponse(w, res)
}

func (c *CodecRequest) WriteError(w http.ResponseWriter, status int, err error) {
	err = c.tryToMapIfNotAnErrorAlready(err)
	jsonErr, ok := err.(*Error)
	if !ok {
		jsonErr = &Error{
			Code:    E_SERVER,
			Message: err.Error(),
		}
	}
	res := &serverResponse{
		Version: Version,
		Error:   jsonErr,
		ID:      c.request.ID,
	}
	c.writeServerResponse(w, res)
}

func (c CodecRequest) tryToMapIfNotAnErrorAlready(err error) error {
	if _, ok := err.(*Error); ok || c.errorMapper == nil {
		return err
	}
	return c.errorMapper(err)
}

func (c *CodecRequest) writeServerResponse(w http.ResponseWriter, res *serverResponse) {
	// ID is null for notifications and they don't have a response, unless we couldn't even parse the JSON, in that
	// case we can't know whether it was intended to be a notification
	if c.request.ID != nil || isParseErrorResponse(res) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		encoder := json.NewEncoder(c.encoder.Encode(w))
		err := encoder.Encode(res)

		// Not sure in which case will this happen. But seems harmless.
		if err != nil {
			rpc.WriteError(w, http.StatusInternalServerError, err.Error())
		}
	}
}

func isParseErrorResponse(res *serverResponse) bool {
	return res != nil && res.Error != nil && res.Error.Code == E_PARSE
}

type EmptyResponse struct {
}
