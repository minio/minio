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

	"github.com/facebookgo/httpdown"
	"github.com/minio/minio/pkg/iodine"
)

var (
	inheritedListeners = os.Getenv("LISTEN_FDS")
	ppid               = os.Getppid()
)

// An app contains one or more servers and associated configuration.
type app struct {
	servers   []*http.Server
	net       *nimbleNet
	listeners []net.Listener
	sds       []httpdown.Server
	errors    chan error
}

func newApp(servers []*http.Server) *app {
	return &app{
		servers:   servers,
		net:       &nimbleNet{},
		listeners: make([]net.Listener, 0, len(servers)),
		sds:       make([]httpdown.Server, 0, len(servers)),
		errors:    make(chan error, 1+(len(servers)*2)),
	}
}

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

func (a *app) serve() {
	h := &httpdown.HTTP{}
	for i, s := range a.servers {
		a.sds = append(a.sds, h.Serve(s, a.listeners[i]))
	}
}

func (a *app) wait() {
	var wg sync.WaitGroup
	wg.Add(len(a.sds) * 2) // Wait & Stop
	go a.signalHandler(&wg)
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

func (a *app) term(wg *sync.WaitGroup) {
	for _, s := range a.sds {
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Stop(); err != nil {
				a.errors <- iodine.New(err, nil)
			}
		}(s)
	}
}

func (a *app) signalHandler(wg *sync.WaitGroup) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGUSR2, syscall.SIGHUP)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGTERM:
			// this ensures a subsequent TERM will trigger standard go behaviour of
			// terminating.
			signal.Stop(ch)
			a.term(wg)
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
	a := newApp(servers)

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
