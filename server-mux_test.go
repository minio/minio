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
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClose(t *testing.T) {
	// Create ServerMux
	m := NewMuxServer("", nil)

	if err := m.Close(); err != nil {
		t.Error("Server errored while trying to Close", err)
	}
}

func TestMuxServer(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	// Create ServerMux
	m := NewMuxServer("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))

	// Set the test server config to the mux
	ts.Config = &m.Server
	ts.Start()

	// Create a MuxListener
	ml, err := NewMuxListener(ts.Listener, m.WaitGroup, "", "")
	if err != nil {
		t.Fatal(err)
	}
	m.listener = ml

	client := http.Client{}
	res, err := client.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != "hello" {
		t.Errorf("got %q, want hello", string(got))
	}

	// Make sure there is only 1 connection
	m.mu.Lock()
	if len(m.conns) < 1 {
		t.Fatal("Should have 1 connections")
	}
	m.mu.Unlock()

	// Close the server
	m.Close()

	// Make sure there are zero connections
	m.mu.Lock()
	if len(m.conns) > 0 {
		t.Fatal("Should have 0 connections")
	}
	m.mu.Unlock()
}

func TestServerCloseBlocking(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	// Create ServerMux
	m := NewMuxServer("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))

	// Set the test server config to the mux
	ts.Config = &m.Server
	ts.Start()

	// Create a MuxListener
	// var err error
	ml, err := NewMuxListener(ts.Listener, m.WaitGroup, "", "")
	if err != nil {
		t.Fatal(err)
	}
	m.listener = ml

	dial := func() net.Conn {
		c, cerr := net.Dial("tcp", ts.Listener.Addr().String())
		if cerr != nil {
			t.Fatal(err)
		}
		return c
	}

	// Dial to open a StateNew but don't send anything
	cnew := dial()
	defer cnew.Close()

	// Dial another connection but idle after a request to have StateIdle
	cidle := dial()
	defer cidle.Close()
	cidle.Write([]byte("HEAD / HTTP/1.1\r\nHost: foo\r\n\r\n"))
	_, err = http.ReadResponse(bufio.NewReader(cidle), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure we don't block forever.
	m.Close()

	// Make sure there are zero connections
	m.mu.Lock()
	if len(m.conns) > 0 {
		t.Fatal("Should have 0 connections")
	}
	m.mu.Unlock()
}

func TestListenAndServe(t *testing.T) {
	m := NewMuxServer("", nil)
	stopc := make(chan struct{})
	errc := make(chan error)
	go func() { errc <- m.ListenAndServe() }()
	go func() { errc <- m.Close(); close(stopc) }()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-stopc:
		return
	}
}
