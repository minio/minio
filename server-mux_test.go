package main

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestAddress(t *testing.T) {
	addr := "127.0.0.1:9000"
	serverAddr := "127.0.0.1:9000"

	m := &MuxServer{
		Server: http.Server{
			Addr: serverAddr,
			// Adding timeout of 10 minutes for unresponsive client connections.
			ReadTimeout:    10 * time.Minute,
			WriteTimeout:   10 * time.Minute,
			Handler:        nil,
			MaxHeaderBytes: 1 << 20,
		},
		GracefulTimeout: 5 * time.Second,
	}

	if got, want := m.Address(), addr; got != want {
		t.Errorf("Expected '%s' but got '%s'", want, got)
	}
}

func TestStop(t *testing.T) {
	serverAddr := "127.0.0.1:9000"

	m := &MuxServer{
		Server: http.Server{
			Addr: serverAddr,
			// Adding timeout of 10 minutes for unresponsive client connections.
			ReadTimeout:    10 * time.Minute,
			WriteTimeout:   10 * time.Minute,
			Handler:        nil,
			MaxHeaderBytes: 1 << 20,
		},
		wg:              new(sync.WaitGroup),
		GracefulTimeout: 5 * time.Second,
	}

	m.wg.Add(1)

	if err := m.Stop(); err != nil {
		t.Error("Server errored while trying to Stop", err)
	}
}
