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
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
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
	errFatal(local.RegisterSingle(handlerTest, func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("1: server payload: ", len(payload), "bytes.")
		return append([]byte{}, payload...), nil
	}))
	// 2: Return as error
	errFatal(local.RegisterSingle(handlerTest2, func(payload []byte) ([]byte, *RemoteErr) {
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
	errFatal(remote.RegisterSingle(handlerTest, func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("1: server payload: ", len(payload), "bytes.")
		return append([]byte{}, payload...), nil
	}))
	// 2: Return as error
	errFatal(remote.RegisterSingle(handlerTest2, func(payload []byte) ([]byte, *RemoteErr) {
		t.Log("2: server payload: ", len(payload), "bytes.")
		err := RemoteErr(payload)
		return nil, &err
	}))

	// local to remote
	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	start := time.Now()
	resp, err := remoteConn.Single(context.Background(), handlerTest, []byte(testPayload))
	errFatal(err)
	if string(resp) != testPayload {
		t.Errorf("want %q, got %q", testPayload, string(resp))
	}
	t.Log("Roundtrip:", time.Since(start))

	start = time.Now()
	resp, err = remoteConn.Single(context.Background(), handlerTest2, []byte(testPayload))
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
	h1 := NewSingleRTHandler[*testRequest, *testResponse](handlerTest, func() *testRequest {
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
	h2 := NewSingleRTHandler[*testRequest, *testResponse](handlerTest2, func() *testRequest {
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

func TestStreamSuite(t *testing.T) {
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

	localServer := startServer(t, listeners[0], wrapServer(local.Handler()))
	remoteServer := startServer(t, listeners[1], wrapServer(remote.Handler()))

	conn := local.Connection(remoteHost)
	err = conn.WaitForConnect(context.Background())
	errFatal(err)

	conn = remote.Connection(localHost)
	err = conn.WaitForConnect(context.Background())
	errFatal(err)
	defer func() {
		localServer.Close()
		remoteServer.Close()
	}()

	t.Run("testStreamRoundtrip", func(t *testing.T) {
		defer timeout(5 * time.Second)()
		testStreamRoundtrip(t, local, remote)
	})
	t.Run("testStreamCancel", func(t *testing.T) {
		defer timeout(5 * time.Second)()
		testStreamCancel(t, local, remote)
	})
	t.Run("testStreamDeadline", func(t *testing.T) {
		defer timeout(5 * time.Second)()
		testStreamDeadline(t, local, remote)
	})
}

func testStreamRoundtrip(t *testing.T, local, remote *Manager) {
	defer testlogger.T.SetErrorTB(t)()
	defer timeout(5 * time.Second)()
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// We fake a local and remote server.
	remoteHost := remote.HostName()

	// 1: Echo
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(handlerTest, StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				for in := range request {
					b := append([]byte{}, payload...)
					b = append(b, in...)
					resp <- b
				}
				t.Log(GetCaller(ctx).Name, "Handler done")
				return nil
			},
			OutCapacity: 1,
			InCapacity:  1,
		}))
		// 2: Return as error
		errFatal(manager.RegisterStreamingHandler(handlerTest2, StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				for in := range request {
					t.Log("2: Got err request", string(in))
					err := RemoteErr(append(payload, in...))
					return &err
				}
				return nil
			},
			OutCapacity: 1,
			InCapacity:  1,
		}))
	}
	register(local)
	register(remote)

	// local to remote
	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	start := time.Now()
	stream, err := remoteConn.NewStream(context.Background(), handlerTest, []byte(testPayload))
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
}

func testStreamCancel(t *testing.T, local, remote *Manager) {
	defer testlogger.T.SetErrorTB(t)()
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// We fake a local and remote server.
	remoteHost := remote.HostName()

	// 1: Echo
	serverCanceled := make(chan struct{})
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(handlerTest, StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				select {
				case <-ctx.Done():
					close(serverCanceled)
					t.Log(GetCaller(ctx).Name, "Server Context canceled")
					return nil
				}
			},
			OutCapacity: 1,
			InCapacity:  0,
		}))
		errFatal(manager.RegisterStreamingHandler(handlerTest2, StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				select {
				case <-ctx.Done():
					close(serverCanceled)
					t.Log(GetCaller(ctx).Name, "Server Context canceled")
					return nil
				}
			},
			OutCapacity: 1,
			InCapacity:  1,
		}))
	}
	register(local)
	register(remote)

	// local to remote
	testHandler := func(t *testing.T, handler HandlerID) {
		remoteConn := local.Connection(remoteHost)
		const testPayload = "Hello Grid World!"

		ctx, cancel := context.WithCancel(context.Background())
		st, err := remoteConn.NewStream(ctx, handlerTest, []byte(testPayload))
		errFatal(err)
		clientCanceled := make(chan time.Time, 1)
		go func() {
			for resp := range st.Responses {
				err = resp.Err
			}
			clientCanceled <- time.Now()
			t.Log("Client Context canceled")
		}()
		start := time.Now()
		cancel()
		<-serverCanceled
		t.Log("server cancel time:", time.Since(start))
		clientEnd := <-clientCanceled
		if !errors.Is(err, context.Canceled) {
			t.Error("expected context.Canceled, got", err)
		}
		t.Log("client after", time.Since(clientEnd))
	}
	// local to remote, unbuffered
	t.Run("unbuffered", func(t *testing.T) {
		testHandler(t, handlerTest)
	})

	t.Run("buffered", func(t *testing.T) {
		testHandler(t, handlerTest2)
	})
}

func testStreamDeadline(t *testing.T, local, remote *Manager) {
	defer testlogger.T.SetErrorTB(t)()
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// We fake a local and remote server.
	remoteHost := remote.HostName()

	// 1: Echo
	serverCanceled := make(chan time.Duration, 1)
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(handlerTest, StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				started := time.Now()
				select {
				case <-ctx.Done():
					serverCanceled <- time.Since(started)
					t.Log(GetCaller(ctx).Name, "Server Context canceled")
					return nil
				}
			},
			OutCapacity: 1,
			InCapacity:  0,
		}))
		errFatal(manager.RegisterStreamingHandler(handlerTest2, StatefulHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				started := time.Now()
				select {
				case <-ctx.Done():
					serverCanceled <- time.Since(started)
					t.Log(GetCaller(ctx).Name, "Server Context canceled")
					return nil
				}
			},
			OutCapacity: 1,
			InCapacity:  1,
		}))
	}
	register(local)
	register(remote)

	testHandler := func(t *testing.T, handler HandlerID) {
		remoteConn := local.Connection(remoteHost)
		const testPayload = "Hello Grid World!"

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		st, err := remoteConn.NewStream(ctx, handler, []byte(testPayload))
		errFatal(err)
		clientCanceled := make(chan time.Duration, 1)
		go func() {
			started := time.Now()
			for resp := range st.Responses {
				err = resp.Err
			}
			clientCanceled <- time.Since(started)
			t.Log("Client Context canceled")
		}()
		serverEnd := <-serverCanceled
		clientEnd := <-clientCanceled
		t.Log("server cancel time:", serverEnd)
		t.Log("client cancel time:", clientEnd)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Error("expected context.Canceled, got", err)
		}
	}
	// local to remote, unbuffered
	t.Run("unbuffered", func(t *testing.T) {
		testHandler(t, handlerTest)
	})

	t.Run("buffered", func(t *testing.T) {
		testHandler(t, handlerTest2)
	})
}

func timeout(after time.Duration) (cancel func()) {
	c := time.After(after)
	cc := make(chan struct{})
	go func() {
		select {
		case <-cc:
			return
		case <-c:
			buf := make([]byte, 1<<20)
			stacklen := runtime.Stack(buf, true)
			fmt.Printf("=== Timeout, assuming deadlock ===\n*** goroutine dump...\n%s\n*** end\n", string(buf[:stacklen]))
			os.Exit(2)
		}
	}()
	return func() {
		close(cc)
	}
}
