/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"net/http"

	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/probe"
)

// rpcOperation RPC operation
type rpcOperation struct {
	Method  string
	Request interface{}
}

// rpcRequest rpc client request
type rpcRequest struct {
	req       *http.Request
	transport http.RoundTripper
}

// newRPCRequest initiate a new client RPC request
func newRPCRequest(url string, op rpcOperation, transport http.RoundTripper) (*rpcRequest, *probe.Error) {
	params, err := json.EncodeClientRequest(op.Method, op.Request)
	if err != nil {
		return nil, probe.NewError(err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(params))
	if err != nil {
		return nil, probe.NewError(err)
	}
	rpcReq := &rpcRequest{}
	rpcReq.req = req
	rpcReq.req.Header.Set("Content-Type", "application/json")
	if transport == nil {
		transport = http.DefaultTransport
	}
	rpcReq.transport = transport
	return rpcReq, nil
}

// Do - make a http connection
func (r rpcRequest) Do() (*http.Response, *probe.Error) {
	resp, err := r.transport.RoundTrip(r.req)
	if err != nil {
		if err, ok := probe.UnwrapError(err); ok {
			return nil, err.Trace()
		}
		return nil, probe.NewError(err)
	}
	return resp, nil
}

// Get - get value of requested header
func (r rpcRequest) Get(key string) string {
	return r.req.Header.Get(key)
}

// Set - set value of a header key
func (r *rpcRequest) Set(key, value string) {
	r.req.Header.Set(key, value)
}
