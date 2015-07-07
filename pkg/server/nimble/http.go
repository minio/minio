/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

// Package nimble provides easy to use graceful restart for a set of HTTP services
//
// This package originally from https://github.com/facebookgo/grace
//
// Re-licensing with Apache License 2.0, with code modifications
package nimble

import (
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/facebookgo/httpdown"
	"github.com/minio/minio/pkg/iodine"
)

var (
	inheritedListeners = os.Getenv("LISTEN_FDS")
	ppid               = os.Getppid()
)

// An app contains one or more servers and their associated configuration.
type app struct {
	servers   []*http.Server
	listeners []net.Listener
	sds       []httpdown.Server
	net       *nimbleNet
	errors    chan error
}

// listen initailize listeners
func (a *app) listen() error {
	for _, s := range a.servers {
		l, err := a.net.Listen("tcp", s.Addr)
		if err != nil {
			return iodine.New(err, nil)
		}
		if s.TLSConfig != nil {
			l = tls.NewListener(l, s.TLSConfig)
		}
		a.listeners = append(a.listeners, l)
	}
	return nil
}

// serve start serving all listeners
func (a *app) serve() {
	h := &httpdown.HTTP{
		StopTimeout: 10 * time.Second,
		KillTimeout: 1 * time.Second,
	}
	for i, s := range a.servers {
		a.sds = append(a.sds, h.Serve(s, a.listeners[i]))
	}
}

// wait for http server to signal all requests that have been served
func (a *app) wait() {
	var wg sync.WaitGroup
	wg.Add(len(a.sds) * 2) // Wait & Stop
	go a.trapSignal(&wg)
	for _, s := range a.sds {
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Wait(); err != nil {
				a.errors <- iodine.New(err, nil)
			}
		}(s)
	}
	wg.Wait()
}

// trapSignal wait on listed signals for pre-defined behaviors
func (a *app) trapSignal(wg *sync.WaitGroup) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGUSR2, syscall.SIGHUP)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGTERM:
			// this ensures a subsequent TERM will trigger standard go behaviour of terminating
			signal.Stop(ch)
			// roll through all initialized http servers and stop them
			for _, s := range a.sds {
				go func(s httpdown.Server) {
					defer wg.Done()
					if err := s.Stop(); err != nil {
						a.errors <- iodine.New(err, nil)
					}
				}(s)
			}
			return
		case syscall.SIGUSR2:
			fallthrough
		case syscall.SIGHUP:
			// we only return here if there's an error, otherwise the new process
			// will send us a TERM when it's ready to trigger the actual shutdown.
			if _, err := a.net.StartProcess(); err != nil {
				a.errors <- iodine.New(err, nil)
			}
		}
	}
}

// ListenAndServe will serve the given http.Servers and will monitor for signals
// allowing for graceful termination (SIGTERM) or restart (SIGUSR2/SIGHUP).
func ListenAndServe(servers ...*http.Server) error {
	a := &app{
		servers:   servers,
		listeners: make([]net.Listener, 0, len(servers)),
		sds:       make([]httpdown.Server, 0, len(servers)),
		net:       &nimbleNet{},
		errors:    make(chan error, 1+(len(servers)*2)),
	}

	// Acquire Listeners
	if err := a.listen(); err != nil {
		return iodine.New(err, nil)
	}

	// Start serving.
	a.serve()

	// Close the parent if we inherited and it wasn't init that started us.
	if inheritedListeners != "" && ppid != 1 {
		if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
			return iodine.New(err, nil)
		}
	}

	waitdone := make(chan struct{})
	go func() {
		defer close(waitdone)
		a.wait()
		// communicate by sending not by closing a channel
		waitdone <- struct{}{}
	}()

	select {
	case err := <-a.errors:
		if err == nil {
			panic("unexpected nil error")
		}
		return iodine.New(err, nil)
	case <-waitdone:
		return nil
	}
}
