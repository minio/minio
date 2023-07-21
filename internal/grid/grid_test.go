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
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio/internal/logger/target/testlogger"
)

func getHosts(n int) (hosts []string, listeners []net.Listener) {
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
				panic(fmt.Sprintf("httptest: failed to listen on a port: %v", err))
			}
		}
		addr := l.Addr()
		hosts = append(hosts, "http://"+addr.String())
		listeners = append(listeners, l)
		// Do not close until we have all hosts
	}
	return
}

func startServer(t testing.TB, listener net.Listener, handler http.Handler) (server *httptest.Server) {
	t.Helper()
	server = httptest.NewUnstartedServer(handler)
	server.Config.Addr = listener.Addr().String()
	server.Listener = listener
	t.Log("Starting server on", server.Config.Addr)
	server.Start()
	t.Log("URL:", server.URL)
	return server
}

func TestSingleRoundtrip(t *testing.T) {
	defer testlogger.T.SetErrorTB(t)()
	hosts, listeners := getHosts(2)
	dialer := &net.Dialer{
		Timeout:        5 * time.Second,
		Deadline:       time.Time{},
		LocalAddr:      nil,
		DualStack:      false,
		FallbackDelay:  0,
		KeepAlive:      0,
		Resolver:       nil,
		Control:        nil,
		ControlContext: nil,
	}
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	wrapServer := func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Got a %s request for: %v", r.Method, r.URL)
			handler.ServeHTTP(w, r)
		})
	}
	// We fake a local and remote server.
	localHost := hosts[0]
	remoteHost := hosts[1]
	local, err := NewManager(dialer, localHost, hosts, func(aud string) string {
		return aud
	})
	errFatal(err)

	// 1: Echo
	errFatal(local.RegisterSingle(HandlerID(1), func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("1: server payload: ", len(payload), "bytes.")
		return append([]byte{}, payload...), nil
	}))
	// 2: Return as error
	errFatal(local.RegisterSingle(HandlerID(2), func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("2: server payload: ", len(payload), "bytes.")
		err := RemoteErr(payload)
		return nil, &err
	}))

	remote, err := NewManager(dialer, remoteHost, hosts, func(aud string) string {
		return aud
	})
	errFatal(err)

	localServer := startServer(t, listeners[0], wrapServer(local.Handler()))
	defer localServer.Close()
	remoteServer := startServer(t, listeners[1], wrapServer(remote.Handler()))
	defer remoteServer.Close()

	// 1: Echo
	errFatal(remote.RegisterSingle(HandlerID(1), func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("1: server payload: ", len(payload), "bytes.")
		return append([]byte{}, payload...), nil
	}))
	// 2: Return as error
	errFatal(remote.RegisterSingle(HandlerID(2), func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("2: server payload: ", len(payload), "bytes.")
		err := RemoteErr(payload)
		return nil, &err
	}))

	// local to remote
	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	start := time.Now()
	resp, err := remoteConn.Single(context.Background(), HandlerID(1), []byte(testPayload))
	errFatal(err)
	if string(resp) != testPayload {
		t.Errorf("want %q, got %q", testPayload, string(resp))
	}
	t.Log("Roundtrip:", time.Since(start))

	start = time.Now()
	resp, err = remoteConn.Single(context.Background(), HandlerID(2), []byte(testPayload))
	t.Log("Roundtrip:", time.Since(start))
	if err != RemoteErr(testPayload) {
		t.Errorf("want error %v(%T), got %v(%T)", RemoteErr(testPayload), RemoteErr(testPayload), err, err)
	}
	t.Log("Roundtrip:", time.Since(start))
}

func TestSingleRoundtripGenerics(t *testing.T) {
	defer testlogger.T.SetErrorTB(t)()
	hosts, listeners := getHosts(2)
	dialer := &net.Dialer{
		Timeout:        5 * time.Second,
		Deadline:       time.Time{},
		LocalAddr:      nil,
		DualStack:      false,
		FallbackDelay:  0,
		KeepAlive:      0,
		Resolver:       nil,
		Control:        nil,
		ControlContext: nil,
	}
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	wrapServer := func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Got a %s request for: %v", r.Method, r.URL)
			handler.ServeHTTP(w, r)
		})
	}
	// We fake a local and remote server.
	localHost := hosts[0]
	remoteHost := hosts[1]
	local, err := NewManager(dialer, localHost, hosts, func(aud string) string {
		return aud
	})
	errFatal(err)

	// 1: Echo
	h1 := NewSingleMsgpHandler[*testRequest, *testResponse](HandlerID(1), func() *testRequest {
		return &testRequest{}
	}, func() *testResponse {
		return &testResponse{}
	})
	// Handles incoming requests, returns a response
	handler1 := func(req *testRequest) (resp *testResponse, err *RemoteErr) {
		return &testResponse{
			OrgNum:    req.Num,
			OrgString: req.String,
			Embedded:  *req,
		}, nil
	}
	// Return error
	h2 := NewSingleMsgpHandler[*testRequest, *testResponse](HandlerID(2), func() *testRequest {
		return &testRequest{}
	}, func() *testResponse {
		return &testResponse{}
	})
	handler2 := func(req *testRequest) (resp *testResponse, err *RemoteErr) {
		r := RemoteErr(req.String)
		return nil, &r
	}
	errFatal(h1.Register(local, handler1))
	errFatal(h2.Register(local, handler2))

	remote, err := NewManager(dialer, remoteHost, hosts, func(aud string) string {
		return aud
	})
	errFatal(err)

	errFatal(h1.Register(remote, handler1))
	errFatal(h2.Register(remote, handler2))

	localServer := startServer(t, listeners[0], wrapServer(local.Handler()))
	defer localServer.Close()
	remoteServer := startServer(t, listeners[1], wrapServer(remote.Handler()))
	defer remoteServer.Close()

	// local to remote connection
	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	start := time.Now()
	req := testRequest{Num: 1, String: testPayload}
	resp, err := h1.Call(context.Background(), remoteConn, &req)
	errFatal(err)
	if resp.OrgString != testPayload {
		t.Errorf("want %q, got %q", testPayload, resp.OrgString)
	}
	t.Log("Roundtrip:", time.Since(start))

	start = time.Now()
	resp, err = h2.Call(context.Background(), remoteConn, &testRequest{Num: 1, String: testPayload})
	t.Log("Roundtrip:", time.Since(start))
	if err != RemoteErr(testPayload) {
		t.Errorf("want error %v(%T), got %v(%T)", RemoteErr(testPayload), RemoteErr(testPayload), err, err)
	}
	t.Log("Roundtrip:", time.Since(start))
}

func TestStreamRoundtrip(t *testing.T) {
	defer testlogger.T.SetErrorTB(t)()
	hosts, listeners := getHosts(2)
	dialer := &net.Dialer{
		Timeout:        5 * time.Second,
		Deadline:       time.Time{},
		LocalAddr:      nil,
		DualStack:      false,
		FallbackDelay:  0,
		KeepAlive:      0,
		Resolver:       nil,
		Control:        nil,
		ControlContext: nil,
	}
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	wrapServer := func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Got a %s request for: %v", r.Method, r.URL)
			handler.ServeHTTP(w, r)
		})
	}
	// We fake a local and remote server.
	localHost := hosts[0]
	remoteHost := hosts[1]
	local, err := NewManager(dialer, localHost, hosts, func(aud string) string {
		return aud
	})
	errFatal(err)

	remote, err := NewManager(dialer, remoteHost, hosts, func(aud string) string {
		return aud
	})
	errFatal(err)

	// 1: Echo
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(HandlerID(1), StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- ServerResponse) {
				for in := range request {
					// t.Log("1: Got request", in)
					b := append([]byte{}, payload...)
					b = append(b, in...)
					resp <- ServerResponse{
						Msg: b,
						Err: nil,
					}
					// t.Log("1: Responded Waiting for next incoming", in)
				}
				t.Log("Handler done")
			},
			OutCapacity: 1,
			InCapacity:  1,
		}))
		// 2: Return as error
		errFatal(manager.RegisterStreamingHandler(HandlerID(2), StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- ServerResponse) {
				for in := range request {
					t.Log("2: Got err request", string(in))
					err := RemoteErr(append(payload, in...))
					resp <- ServerResponse{
						Err: &err,
					}
				}
			},
			OutCapacity: 1,
			InCapacity:  1,
		}))
	}
	register(local)
	register(remote)

	localServer := startServer(t, listeners[0], wrapServer(local.Handler()))
	defer localServer.Close()
	remoteServer := startServer(t, listeners[1], wrapServer(remote.Handler()))
	defer remoteServer.Close()

	// local to remote
	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	err = remoteConn.WaitForConnect(context.Background())
	errFatal(err)

	start := time.Now()
	stream, err := remoteConn.NewStream(context.Background(), HandlerID(1), []byte(testPayload))
	errFatal(err)
	var n int
	stream.Requests <- []byte(strconv.Itoa(n))
	for resp := range stream.Responses {
		errFatal(resp.Err)
		t.Logf("got resp: %+v", string(resp.Msg))
		if string(resp.Msg) != testPayload+strconv.Itoa(n) {
			t.Errorf("want %q, got %q", testPayload+strconv.Itoa(n), string(resp.Msg))
		}
		if n == 10 {
			close(stream.Requests)
			break
		}
		n++
		t.Log("sending new client request")
		stream.Requests <- []byte(strconv.Itoa(n))
	}
	t.Log("EOF. 10 Roundtrips:", time.Since(start))
	/*
		=== RUN   TestStreamRoundtrip
		    grid_test.go:315: Starting server on 127.0.0.1:50720
		    grid_test.go:315: URL: http://127.0.0.1:50720
		    grid_test.go:317: Starting server on 127.0.0.1:50721
		    grid_test.go:317: URL: http://127.0.0.1:50721
		    grid_test.go:261: Got a GET request for: /minio/grid/v1
		    grid_test.go:334: got resp: Hello Grid World!0
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!1
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!2
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!3
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!4
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!5
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!6
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!7
		    grid_test.go:343: sending new client request
		    grid_test.go:334: got resp: Hello Grid World!8
		    grid_test.go:343: sending new client request
		    grid_test.go:292: Handler done
		    grid_test.go:333: receive sequence number mismatch. want 19, got 18
	*/
}
