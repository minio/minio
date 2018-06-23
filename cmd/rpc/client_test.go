/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package rpc

import (
	"net/http"
	"net/http/httptest"
	"testing"

	xnet "github.com/minio/minio/pkg/net"
)

func TestClientCall(t *testing.T) {
	rpcServer := NewServer()
	if err := rpcServer.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	url, err := xnet.ParseURL(httpServer.URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	rpcClient := NewClient(url, nil, DefaultRPCTimeout)

	var reply int
	var boolReply bool
	var intArg int

	testCases := []struct {
		serviceMethod string
		args          interface{}
		reply         interface{}
		expectErr     bool
	}{
		{"Arith.Multiply", Args{7, 8}, &reply, false},
		{"Arith.Multiply", &Args{7, 8}, &reply, false},
		// rpc reply must be a pointer type but found int error.
		{"Arith.Multiply", &Args{7, 8}, reply, true},
		// gob: type mismatch in decoder: want struct type rpc.Args; got non-struct error.
		{"Arith.Multiply", intArg, &reply, true},
		// gob: decoding into local type *bool, received remote type int error.
		{"Arith.Multiply", &Args{7, 8}, &boolReply, true},
	}

	for i, testCase := range testCases {
		err := rpcClient.Call(testCase.serviceMethod, testCase.args, testCase.reply)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
