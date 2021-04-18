// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2021 MinIO, Inc. All rights reserved.
// forked from https://github.com/gorilla/rpc/v2
// modified to be used with MinIO under GNU Affero General
// Public License 3.0 license that can be found in
// the LICENSE file.

package json2

import (
	"io"
	"math/rand"

	jsoniter "github.com/json-iterator/go"
)

// ----------------------------------------------------------------------------
// Request and Response
// ----------------------------------------------------------------------------

// clientRequest represents a JSON-RPC request sent by a client.
type clientRequest struct {
	// JSON-RPC protocol.
	Version string `json:"jsonrpc"`

	// A String containing the name of the method to be invoked.
	Method string `json:"method"`

	// Object to pass as request parameter to the method.
	Params interface{} `json:"params"`

	// The request id. This can be of any type. It is used to match the
	// response with the request that it is replying to.
	Id uint64 `json:"id"`
}

// clientResponse represents a JSON-RPC response returned to a client.
type clientResponse struct {
	Version string               `json:"jsonrpc"`
	Result  *jsoniter.RawMessage `json:"result"`
	Error   *jsoniter.RawMessage `json:"error"`
}

// EncodeClientRequest encodes parameters for a JSON-RPC client request.
func EncodeClientRequest(method string, args interface{}) ([]byte, error) {
	c := &clientRequest{
		Version: "2.0",
		Method:  method,
		Params:  args,
		Id:      uint64(rand.Int63()),
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Marshal(c)
}

// DecodeClientResponse decodes the response body of a client request into
// the interface reply.
func DecodeClientResponse(r io.Reader, reply interface{}) error {
	var c clientResponse
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return err
	}
	if c.Error != nil {
		jsonErr := &Error{}
		if err := json.Unmarshal(*c.Error, jsonErr); err != nil {
			return &Error{
				Code:    E_SERVER,
				Message: string(*c.Error),
			}
		}
		return jsonErr
	}

	if c.Result == nil {
		return ErrNullResult
	}

	return json.Unmarshal(*c.Result, reply)
}
