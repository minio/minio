// Copyright (c) 2015-2022 MinIO, Inc.
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

package deadlineconn

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Test deadlineconn handles read timeout properly by reading two messages beyond deadline.
func TestBuffConnReadTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("unable to create listener. %v", err)
	}
	defer l.Close()
	serverAddr := l.Addr().String()

	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		t.Fatalf("failed to assert to net.TCPListener")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		tcpConn, terr := tcpListener.AcceptTCP()
		if terr != nil {
			t.Errorf("failed to accept new connection. %v", terr)
			return
		}
		deadlineconn := New(tcpConn)
		deadlineconn.WithReadDeadline(time.Second)
		deadlineconn.WithWriteDeadline(time.Second)
		defer deadlineconn.Close()

		// Read a line
		b := make([]byte, 12)
		_, terr = deadlineconn.Read(b)
		if terr != nil {
			t.Errorf("failed to read from client. %v", terr)
			return
		}
		received := string(b)
		if received != "message one\n" {
			t.Errorf(`server: expected: "message one\n", got: %v`, received)
			return
		}

		// Wait for more than read timeout to simulate processing.
		time.Sleep(3 * time.Second)

		_, terr = deadlineconn.Read(b)
		if terr != nil {
			t.Errorf("failed to read from client. %v", terr)
			return
		}
		received = string(b)
		if received != "message two\n" {
			t.Errorf(`server: expected: "message two\n", got: %v`, received)
			return
		}

		// Send a response.
		_, terr = io.WriteString(deadlineconn, "messages received\n")
		if terr != nil {
			t.Errorf("failed to write to client. %v", terr)
			return
		}
	}()

	c, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("unable to connect to server. %v", err)
	}
	defer c.Close()

	_, err = io.WriteString(c, "message one\n")
	if err != nil {
		t.Fatalf("failed to write to server. %v", err)
	}
	_, err = io.WriteString(c, "message two\n")
	if err != nil {
		t.Fatalf("failed to write to server. %v", err)
	}

	received, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read from server. %v", err)
	}
	if received != "messages received\n" {
		t.Fatalf(`client: expected: "messages received\n", got: %v`, received)
	}

	wg.Wait()
}

// Test deadlineconn handles read timeout properly by reading two messages beyond deadline.
func TestBuffConnReadCheckTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("unable to create listener. %v", err)
	}
	defer l.Close()
	serverAddr := l.Addr().String()

	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		t.Fatalf("failed to assert to net.TCPListener")
	}
	var cerr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		tcpConn, terr := tcpListener.AcceptTCP()
		if terr != nil {
			cerr = fmt.Errorf("failed to accept new connection. %v", terr)
			return
		}
		deadlineconn := New(tcpConn)
		deadlineconn.WithReadDeadline(time.Second)
		deadlineconn.WithWriteDeadline(time.Second)
		defer deadlineconn.Close()

		// Read a line
		b := make([]byte, 12)
		_, terr = deadlineconn.Read(b)
		if terr != nil {
			cerr = fmt.Errorf("failed to read from client. %v", terr)
			return
		}
		received := string(b)
		if received != "message one\n" {
			cerr = fmt.Errorf(`server: expected: "message one\n", got: %v`, received)
			return
		}

		// Set a deadline in the past to indicate we want the next read to fail.
		// Ensure we don't override it on read.
		deadlineconn.SetReadDeadline(time.Unix(1, 0))

		// Be sure to exceed update interval
		time.Sleep(updateInterval * 2)

		_, terr = deadlineconn.Read(b)
		if terr == nil {
			cerr = fmt.Errorf("could read from client, expected error, got %v", terr)
			return
		}
	}()

	c, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("unable to connect to server. %v", err)
	}
	defer c.Close()

	_, err = io.WriteString(c, "message one\n")
	if err != nil {
		t.Fatalf("failed to write to server. %v", err)
	}
	_, _ = io.WriteString(c, "message two\n")

	wg.Wait()
	if cerr != nil {
		t.Fatal(cerr)
	}
}
