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
	server.Start()
	t.Log("Started server on", server.Config.Addr, "URL:", server.URL)
	return server
}

func shutdownManagers(t testing.TB, servers ...*Manager) {
	t.Helper()
	for _, s := range servers {
		s.debugMsg(debugShutdown)
	}
	for _, s := range servers {
		s.debugMsg(debugWaitForExit)
	}
}

func TestSingleRoundtrip(t *testing.T) {
	defer testlogger.T.SetErrorTB(t)()
	hosts, listeners := getHosts(2)
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
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
	local, err := NewManager(context.Background(), ManagerOptions{
		Dialer: dialer.DialContext,
		Local:  localHost,
		Hosts:  hosts,
		Auth:   func(aud string) string { return aud },
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

	remote, err := NewManager(context.Background(), ManagerOptions{
		Dialer: dialer.DialContext,
		Local:  remoteHost,
		Hosts:  hosts,
		Auth:   func(aud string) string { return aud },
	})
	errFatal(err)
	defer shutdownManagers(t, local, remote)

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
	remoteConn.WaitForConnect(context.Background())

	start := time.Now()
	resp, err := remoteConn.Request(context.Background(), handlerTest, []byte(testPayload))
	errFatal(err)
	if string(resp) != testPayload {
		t.Errorf("want %q, got %q", testPayload, string(resp))
	}
	t.Log("Roundtrip:", time.Since(start))

	start = time.Now()
	resp, err = remoteConn.Request(context.Background(), handlerTest2, []byte(testPayload))
	t.Log("Roundtrip:", time.Since(start))
	if len(resp) != 0 {
		t.Errorf("want nil, got %q", string(resp))
	}
	if err != RemoteErr(testPayload) {
		t.Errorf("want error %v(%T), got %v(%T)", RemoteErr(testPayload), RemoteErr(testPayload), err, err)
	}
	t.Log("Roundtrip:", time.Since(start))
}

func TestSingleRoundtripGenerics(t *testing.T) {
	defer testlogger.T.SetErrorTB(t)()
	hosts, listeners := getHosts(2)
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
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
	local, err := NewManager(context.Background(), ManagerOptions{
		Dialer: dialer.DialContext,
		Local:  localHost,
		Hosts:  hosts,
		Auth:   func(aud string) string { return aud },
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
		resp = h1.NewResponse()
		*resp = testResponse{
			OrgNum:    req.Num,
			OrgString: req.String,
			Embedded:  *req,
		}
		return resp, nil
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

	remote, err := NewManager(context.Background(), ManagerOptions{
		Dialer: dialer.DialContext,
		Local:  remoteHost,
		Hosts:  hosts,
		Auth:   func(aud string) string { return aud },
	})

	errFatal(err)
	defer shutdownManagers(t, local, remote)

	errFatal(h1.Register(remote, handler1))
	errFatal(h2.Register(remote, handler2))

	localServer := startServer(t, listeners[0], wrapServer(local.Handler()))
	defer localServer.Close()
	remoteServer := startServer(t, listeners[1], wrapServer(remote.Handler()))
	defer remoteServer.Close()

	// local to remote connection
	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"
	remoteConn.WaitForConnect(context.Background())

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
	if resp != nil {
		t.Errorf("want nil, got %q", resp)
	}
	t.Log("Roundtrip:", time.Since(start))
}

func TestStreamSuite(t *testing.T) {
	defer testlogger.T.SetErrorTB(t)()
	hosts, listeners := getHosts(2)
	dialer := &net.Dialer{
		Timeout: 1 * time.Second,
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

	local, err := NewManager(context.Background(), ManagerOptions{
		Dialer: dialer.DialContext,
		Local:  localHost,
		Hosts:  hosts,
		Auth:   func(aud string) string { return aud },
	})

	errFatal(err)

	remote, err := NewManager(context.Background(), ManagerOptions{
		Dialer: dialer.DialContext,
		Local:  remoteHost,
		Hosts:  hosts,
		Auth:   func(aud string) string { return aud },
	})
	errFatal(err)
	defer shutdownManagers(t, local, remote)

	localServer := startServer(t, listeners[0], wrapServer(local.Handler()))
	remoteServer := startServer(t, listeners[1], wrapServer(remote.Handler()))

	connLocalToRemote := local.Connection(remoteHost)
	err = connLocalToRemote.WaitForConnect(context.Background())
	errFatal(err)

	connRemoteLocal := remote.Connection(localHost)
	err = connRemoteLocal.WaitForConnect(context.Background())
	errFatal(err)
	defer func() {
		localServer.Close()
		remoteServer.Close()
	}()

	t.Run("testStreamRoundtrip", func(t *testing.T) {
		defer timeout(5 * time.Second)()
		testStreamRoundtrip(t, local, remote)
		assertNoActive(t, connRemoteLocal)
		assertNoActive(t, connLocalToRemote)
	})
	t.Run("testStreamCancel", func(t *testing.T) {
		defer timeout(5 * time.Second)()
		testStreamCancel(t, local, remote)
		assertNoActive(t, connRemoteLocal)
		assertNoActive(t, connLocalToRemote)
	})
	t.Run("testStreamDeadline", func(t *testing.T) {
		defer timeout(5 * time.Second)()
		testStreamDeadline(t, local, remote)
		assertNoActive(t, connRemoteLocal)
		assertNoActive(t, connLocalToRemote)
	})
	t.Run("testServerOutCongestion", func(t *testing.T) {
		defer timeout(1 * time.Minute)()
		testServerOutCongestion(t, local, remote)
		assertNoActive(t, connRemoteLocal)
		assertNoActive(t, connLocalToRemote)
	})
	t.Run("testServerInCongestion", func(t *testing.T) {
		defer timeout(1 * time.Minute)()
		testServerInCongestion(t, local, remote)
		assertNoActive(t, connRemoteLocal)
		assertNoActive(t, connLocalToRemote)
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
		errFatal(manager.RegisterStreamingHandler(handlerTest, StreamHandler{
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
		errFatal(manager.RegisterStreamingHandler(handlerTest2, StreamHandler{
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
			continue
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
		errFatal(manager.RegisterStreamingHandler(handlerTest, StreamHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				<-ctx.Done()
				serverCanceled <- struct{}{}
				t.Log(GetCaller(ctx).Name, "Server Context canceled")
				return nil
			},
			OutCapacity: 1,
			InCapacity:  0,
		}))
		errFatal(manager.RegisterStreamingHandler(handlerTest2, StreamHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				<-ctx.Done()
				serverCanceled <- struct{}{}
				t.Log(GetCaller(ctx).Name, "Server Context canceled")
				return nil
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
		err = nil
		go func(t *testing.T) {
			for resp := range st.Responses {
				t.Log("got resp:", string(resp.Msg), "err:", resp.Err)
				if err != nil {
					t.Log("ERROR: got second error:", resp.Err, "first:", err)
					continue
				}
				err = resp.Err
			}
			t.Log("Client Context canceled. err state:", err)
			clientCanceled <- time.Now()
		}(t)
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

// testStreamDeadline will test if server
func testStreamDeadline(t *testing.T, local, remote *Manager) {
	defer testlogger.T.SetErrorTB(t)()
	errFatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	const wantDL = 50 * time.Millisecond
	// We fake a local and remote server.
	remoteHost := remote.HostName()

	// 1: Echo
	serverCanceled := make(chan time.Duration, 1)
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(handlerTest, StreamHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				started := time.Now()
				dl, _ := ctx.Deadline()
				if testing.Verbose() {
					fmt.Println(GetCaller(ctx).Name, "Server deadline:", time.Until(dl))
				}
				<-ctx.Done()
				serverCanceled <- time.Since(started)
				if testing.Verbose() {
					fmt.Println(GetCaller(ctx).Name, "Server Context canceled with", ctx.Err(), "after", time.Since(started))
				}
				return nil
			},
			OutCapacity: 1,
			InCapacity:  0,
		}))
		errFatal(manager.RegisterStreamingHandler(handlerTest2, StreamHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				started := time.Now()
				dl, _ := ctx.Deadline()
				if testing.Verbose() {
					fmt.Println(GetCaller(ctx).Name, "Server deadline:", time.Until(dl))
				}
				<-ctx.Done()
				serverCanceled <- time.Since(started)
				if testing.Verbose() {
					fmt.Println(GetCaller(ctx).Name, "Server Context canceled with", ctx.Err(), "after", time.Since(started))
				}
				return nil
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

		ctx, cancel := context.WithTimeout(context.Background(), wantDL)
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

func testServerOutCongestion(t *testing.T, local, remote *Manager) {
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
	serverSent := make(chan struct{})
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(handlerTest, StreamHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				// Send many responses.
				// Test that this doesn't block.
				for i := byte(0); i < 100; i++ {
					select {
					case resp <- []byte{i}:
					// ok
					case <-ctx.Done():
						return NewRemoteErr(ctx.Err())
					}
					if i == 0 {
						close(serverSent)
					}
				}
				return nil
			},
			OutCapacity: 1,
			InCapacity:  0,
		}))
		errFatal(manager.RegisterSingle(handlerTest2, func(payload []byte) ([]byte, *RemoteErr) {
			// Simple roundtrip
			return append([]byte{}, payload...), nil
		}))
	}
	register(local)
	register(remote)

	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := remoteConn.NewStream(ctx, handlerTest, []byte(testPayload))
	errFatal(err)

	// Wait for the server to send the first response.
	<-serverSent

	// Now do 100 other requests to ensure that the server doesn't block.
	for i := 0; i < 100; i++ {
		_, err := remoteConn.Request(ctx, handlerTest2, []byte(testPayload))
		errFatal(err)
	}
	// Drain responses
	got := 0
	for resp := range st.Responses {
		t.Log("got response", resp)
		errFatal(resp.Err)
		if resp.Msg[0] != byte(got) {
			t.Error("expected response", got, "got", resp.Msg[0])
		}
		got++
	}
	if got != 100 {
		t.Error("expected 100 responses, got", got)
	}
}

func testServerInCongestion(t *testing.T, local, remote *Manager) {
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
	processHandler := make(chan struct{})
	register := func(manager *Manager) {
		errFatal(manager.RegisterStreamingHandler(handlerTest, StreamHandler{
			Handle: func(ctx context.Context, payload []byte, request <-chan []byte, resp chan<- []byte) *RemoteErr {
				// Block incoming requests.
				var n byte
				<-processHandler
				for {
					select {
					case in, ok := <-request:
						if !ok {
							return nil
						}
						if in[0] != n {
							return NewRemoteErrString(fmt.Sprintf("expected incoming %d, got %d", n, in[0]))
						}
						n++
						resp <- append([]byte{}, in...)
					case <-ctx.Done():
						return NewRemoteErr(ctx.Err())
					}
				}
			},
			OutCapacity: 5,
			InCapacity:  5,
		}))
		errFatal(manager.RegisterSingle(handlerTest2, func(payload []byte) ([]byte, *RemoteErr) {
			// Simple roundtrip
			return append([]byte{}, payload...), nil
		}))
	}
	register(local)
	register(remote)

	remoteConn := local.Connection(remoteHost)
	const testPayload = "Hello Grid World!"

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := remoteConn.NewStream(ctx, handlerTest, []byte(testPayload))
	errFatal(err)

	// Start sending requests.
	go func() {
		for i := byte(0); i < 100; i++ {
			st.Requests <- []byte{i}
		}
		close(st.Requests)
	}()
	// Now do 100 other requests to ensure that the server doesn't block.
	for i := 0; i < 100; i++ {
		_, err := remoteConn.Request(ctx, handlerTest2, []byte(testPayload))
		errFatal(err)
	}
	// Start processing requests.
	close(processHandler)

	// Drain responses
	got := 0
	for resp := range st.Responses {
		t.Log("got response", resp)
		errFatal(resp.Err)
		if resp.Msg[0] != byte(got) {
			t.Error("expected response", got, "got", resp.Msg[0])
		}
		got++
	}
	if got != 100 {
		t.Error("expected 100 responses, got", got)
	}
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

func assertNoActive(t *testing.T, c *Connection) {
	t.Helper()
	// Tiny bit racy for tests, but we try to play nice.
	for i := 10; i >= 0; i-- {
		runtime.Gosched()
		stats := c.Stats()
		if stats.IncomingStreams != 0 {
			if i > 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			var found []uint64
			c.inStream.Range(func(key uint64, value *muxServer) bool {
				found = append(found, key)
				return true
			})
			t.Errorf("expected no active streams, got %d incoming: %v", stats.IncomingStreams, found)
		}
		if stats.OutgoingStreams != 0 {
			if i > 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			var found []uint64
			c.outgoing.Range(func(key uint64, value *MuxClient) bool {
				found = append(found, key)
				return true
			})
			t.Errorf("expected no active streams, got %d outgoing: %v", stats.OutgoingStreams, found)
		}
		return
	}
}
