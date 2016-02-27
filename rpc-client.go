/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"time"

	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio/pkg/probe"
)

// rpcOperation RPC operation
type rpcOperation struct {
	Method  string
	Request interface{}
}

// newRPCRequest initiate a new client RPC request.
func newRPCRequest(op rpcOperation, url string) (*http.Request, *probe.Error) {
	t := time.Now().UTC()
	params, e := json2.EncodeClientRequest(op.Method, op.Request)
	if e != nil {
		return nil, probe.NewError(e)
	}

	body := bytes.NewReader(params)
	req, e := http.NewRequest("POST", url, body)
	if e != nil {
		return nil, probe.NewError(e)
	}
	req.Header.Set("x-minio-date", t.Format(iso8601Format))
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}
