package httpdown_test

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/facebookgo/ensure"
	"github.com/facebookgo/freeport"
	"github.com/facebookgo/httpdown"
	"github.com/facebookgo/stats"
)

type onCloseListener struct {
	net.Listener
	mutex   sync.Mutex
	onClose chan struct{}
}

func (o *onCloseListener) Close() error {
	// Listener is closed twice, once by Grace, and once by the http library, so
	// we guard against a double close of the chan.
	defer func() {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		if o.onClose != nil {
			close(o.onClose)
			o.onClose = nil
		}
	}()
	return o.Listener.Close()
}

func NewOnCloseListener(l net.Listener) (net.Listener, chan struct{}) {
	c := make(chan struct{})
	return &onCloseListener{Listener: l, onClose: c}, c
}

type closeErrListener struct {
	net.Listener
	err error
}

func (c *closeErrListener) Close() error {
	c.Listener.Close()
	return c.err
}

type acceptErrListener struct {
	net.Listener
	err chan error
}

func (c *acceptErrListener) Accept() (net.Conn, error) {
	return nil, <-c.err
}

type closeErrConn struct {
	net.Conn
	unblockClose chan chan struct{}
}

func (c *closeErrConn) Close() error {
	ch := <-c.unblockClose

	// Close gets called multiple times, but only the first one gets this ch
	if ch != nil {
		defer close(ch)
	}

	return c.Conn.Close()
}

type closeErrConnListener struct {
	net.Listener
	unblockClose chan chan struct{}
}

func (l *closeErrConnListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return c, err
	}
	return &closeErrConn{Conn: c, unblockClose: l.unblockClose}, nil
}

func TestHTTPStopWithNoRequest(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)

	statsDone := make(chan struct{}, 2)
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "serve" && val == 1 {
				statsDone <- struct{}{}
			}
			if key == "stop" && val == 1 {
				statsDone <- struct{}{}
			}
		},
	}

	server := &http.Server{}
	down := &httpdown.HTTP{Stats: hc}
	s := down.Serve(server, listener)
	ensure.Nil(t, s.Stop())
	<-statsDone
	<-statsDone
}

func TestHTTPStopWithFinishedRequest(t *testing.T) {
	t.Parallel()
	hello := []byte("hello")
	fin := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(fin)
		w.Write(hello)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)
	actualBody, err := ioutil.ReadAll(res.Body)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualBody, hello)
	ensure.Nil(t, res.Body.Close())

	// At this point the request is finished, and the connection should be alive
	// but idle (because we have keep alive enabled by default in our Transport).
	ensure.Nil(t, s.Stop())
	<-fin

	ensure.Nil(t, s.Wait())
}

func TestHTTPStopWithActiveRequest(t *testing.T) {
	t.Parallel()
	const count = 10000
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.WriteHeader(200)
		for i := 0; i < count; i++ {
			w.Write(hello)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)

	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	actualBody, err := ioutil.ReadAll(res.Body)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualBody, bytes.Repeat(hello, count))
	ensure.Nil(t, res.Body.Close())
	<-finOkHandler
	<-finStop
}

func TestNewRequestAfterStop(t *testing.T) {
	t.Parallel()
	const count = 10000
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	unblockOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.WriteHeader(200)
		const diff = 500
		for i := 0; i < count-diff; i++ {
			w.Write(hello)
		}
		<-unblockOkHandler
		for i := 0; i < diff; i++ {
			w.Write(hello)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	listener, onClose := NewOnCloseListener(listener)
	ensure.Nil(t, err)
	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)

	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	// Wait until the listener is closed.
	<-onClose

	// Now the next request should not be able to connect as the listener is
	// now closed.
	_, err = client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))

	// We should just get "connection refused" here, but sometimes, very rarely,
	// we get a "connection reset" instead. Unclear why this happens.
	ensure.Err(t, err, regexp.MustCompile("(connection refused|connection reset by peer)$"))

	// Unblock the handler and ensure we finish writing the rest of the body
	// successfully.
	close(unblockOkHandler)
	actualBody, err := ioutil.ReadAll(res.Body)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualBody, bytes.Repeat(hello, count))
	ensure.Nil(t, res.Body.Close())
	<-finOkHandler
	<-finStop
}

func TestHTTPListenerCloseError(t *testing.T) {
	t.Parallel()
	expectedError := errors.New("foo")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	listener = &closeErrListener{Listener: listener, err: expectedError}
	ensure.Nil(t, err)
	server := &http.Server{}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	ensure.DeepEqual(t, s.Stop(), expectedError)
}

func TestHTTPServeError(t *testing.T) {
	t.Parallel()
	expectedError := errors.New("foo")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	errChan := make(chan error)
	listener = &acceptErrListener{Listener: listener, err: errChan}
	ensure.Nil(t, err)
	server := &http.Server{}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	errChan <- expectedError
	ensure.DeepEqual(t, s.Wait(), expectedError)
	ensure.Nil(t, s.Stop())
}

func TestHTTPWithinStopTimeout(t *testing.T) {
	t.Parallel()
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.WriteHeader(200)
		w.Write(hello)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{StopTimeout: time.Minute}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)

	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	actualBody, err := ioutil.ReadAll(res.Body)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualBody, hello)
	ensure.Nil(t, res.Body.Close())
	<-finOkHandler
	<-finStop
}

func TestHTTPStopTimeoutMissed(t *testing.T) {
	t.Parallel()

	klock := clock.NewMock()

	const count = 10000
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	unblockOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.Header().Set("Content-Length", fmt.Sprint(len(hello)*count))
		w.WriteHeader(200)
		for i := 0; i < count/2; i++ {
			w.Write(hello)
		}
		<-unblockOkHandler
		for i := 0; i < count/2; i++ {
			w.Write(hello)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{
		StopTimeout: time.Minute,
		Clock:       klock,
	}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)

	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	klock.Wait(clock.Calls{After: 1}) // wait for Stop to call After
	klock.Add(down.StopTimeout)

	_, err = ioutil.ReadAll(res.Body)
	ensure.Err(t, err, regexp.MustCompile("^unexpected EOF$"))
	ensure.Nil(t, res.Body.Close())
	close(unblockOkHandler)
	<-finOkHandler
	<-finStop
}

func TestHTTPKillTimeout(t *testing.T) {
	t.Parallel()

	klock := clock.NewMock()

	statsDone := make(chan struct{}, 1)
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "kill" && val == 1 {
				statsDone <- struct{}{}
			}
		},
	}

	const count = 10000
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	unblockOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.Header().Set("Content-Length", fmt.Sprint(len(hello)*count))
		w.WriteHeader(200)
		for i := 0; i < count/2; i++ {
			w.Write(hello)
		}
		<-unblockOkHandler
		for i := 0; i < count/2; i++ {
			w.Write(hello)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{
		StopTimeout: time.Minute,
		KillTimeout: time.Minute,
		Stats:       hc,
		Clock:       klock,
	}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)

	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	klock.Wait(clock.Calls{After: 1}) // wait for Stop to call After
	klock.Add(down.StopTimeout)

	_, err = ioutil.ReadAll(res.Body)
	ensure.Err(t, err, regexp.MustCompile("^unexpected EOF$"))
	ensure.Nil(t, res.Body.Close())
	close(unblockOkHandler)
	<-finOkHandler
	<-finStop
	<-statsDone
}

func TestHTTPKillTimeoutMissed(t *testing.T) {
	t.Parallel()

	klock := clock.NewMock()

	statsDone := make(chan struct{}, 1)
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "kill.timeout" && val == 1 {
				statsDone <- struct{}{}
			}
		},
	}

	const count = 10000
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	unblockOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.Header().Set("Content-Length", fmt.Sprint(len(hello)*count))
		w.WriteHeader(200)
		for i := 0; i < count/2; i++ {
			w.Write(hello)
		}
		<-unblockOkHandler
		for i := 0; i < count/2; i++ {
			w.Write(hello)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	unblockConnClose := make(chan chan struct{}, 1)
	listener = &closeErrConnListener{
		Listener:     listener,
		unblockClose: unblockConnClose,
	}

	server := &http.Server{Handler: http.HandlerFunc(okHandler)}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{
		StopTimeout: time.Minute,
		KillTimeout: time.Minute,
		Stats:       hc,
		Clock:       klock,
	}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)

	// Start the Stop process.
	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	klock.Wait(clock.Calls{After: 1}) // wait for Stop to call After
	klock.Add(down.StopTimeout)       // trigger stop timeout
	klock.Wait(clock.Calls{After: 2}) // wait for Kill to call After
	klock.Add(down.KillTimeout)       // trigger kill timeout

	// We hit both the StopTimeout & the KillTimeout.
	<-finStop

	// Then we unblock the Close, so we get an unexpected EOF since we close
	// before we finish writing the response.
	connCloseDone := make(chan struct{})
	unblockConnClose <- connCloseDone
	<-connCloseDone
	close(unblockConnClose)

	// Then we unblock the handler which tries to write the rest of the data.
	close(unblockOkHandler)

	_, err = ioutil.ReadAll(res.Body)
	ensure.Err(t, err, regexp.MustCompile("^unexpected EOF$"))
	ensure.Nil(t, res.Body.Close())
	<-finOkHandler
	<-statsDone
}

func TestDoubleStop(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	ensure.Nil(t, s.Stop())
	ensure.Nil(t, s.Stop())
}

func TestExistingConnState(t *testing.T) {
	t.Parallel()
	hello := []byte("hello")
	fin := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(fin)
		w.Write(hello)
	}

	var called int32
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	ensure.Nil(t, err)
	server := &http.Server{
		Handler: http.HandlerFunc(okHandler),
		ConnState: func(c net.Conn, s http.ConnState) {
			atomic.AddInt32(&called, 1)
		},
	}
	transport := &http.Transport{}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{}
	s := down.Serve(server, listener)
	res, err := client.Get(fmt.Sprintf("http://%s/", listener.Addr().String()))
	ensure.Nil(t, err)
	actualBody, err := ioutil.ReadAll(res.Body)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualBody, hello)
	ensure.Nil(t, res.Body.Close())

	ensure.Nil(t, s.Stop())
	<-fin

	ensure.True(t, atomic.LoadInt32(&called) > 0)
}

func TestHTTPDefaultListenError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cant run this test as root")
	}

	statsDone := make(chan struct{}, 1)
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "listen.error" && val == 1 {
				statsDone <- struct{}{}
			}
		},
	}

	t.Parallel()
	down := &httpdown.HTTP{Stats: hc}
	_, err := down.ListenAndServe(&http.Server{})
	ensure.Err(t, err, regexp.MustCompile("listen tcp :80: bind: permission denied"))
	<-statsDone
}

func TestHTTPSDefaultListenError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cant run this test as root")
	}
	t.Parallel()

	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		t.Fatalf("error loading cert: %v", err)
	}

	down := &httpdown.HTTP{}
	_, err = down.ListenAndServe(&http.Server{
		TLSConfig: &tls.Config{
			NextProtos:   []string{"http/1.1"},
			Certificates: []tls.Certificate{cert},
		},
	})
	ensure.Err(t, err, regexp.MustCompile("listen tcp :443: bind: permission denied"))
}

func TestTLS(t *testing.T) {
	t.Parallel()
	port, err := freeport.Get()
	ensure.Nil(t, err)

	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		t.Fatalf("error loading cert: %v", err)
	}
	const count = 10000
	hello := []byte("hello")
	finOkHandler := make(chan struct{})
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(finOkHandler)
		w.WriteHeader(200)
		for i := 0; i < count; i++ {
			w.Write(hello)
		}
	}

	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", port),
		Handler: http.HandlerFunc(okHandler),
		TLSConfig: &tls.Config{
			NextProtos:   []string{"http/1.1"},
			Certificates: []tls.Certificate{cert},
		},
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	client := &http.Client{Transport: transport}
	down := &httpdown.HTTP{}
	s, err := down.ListenAndServe(server)
	ensure.Nil(t, err)
	res, err := client.Get(fmt.Sprintf("https://%s/", server.Addr))
	ensure.Nil(t, err)

	finStop := make(chan struct{})
	go func() {
		defer close(finStop)
		ensure.Nil(t, s.Stop())
	}()

	actualBody, err := ioutil.ReadAll(res.Body)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualBody, bytes.Repeat(hello, count))
	ensure.Nil(t, res.Body.Close())
	<-finOkHandler
	<-finStop
}

// localhostCert is a PEM-encoded TLS cert with SAN IPs
// "127.0.0.1" and "[::1]", expiring at the last second of 2049 (the end
// of ASN.1 time).
// generated from src/pkg/crypto/tls:
// go run generate_cert.go  --rsa-bits 512 --host 127.0.0.1,::1,example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var localhostCert = []byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCASOgAwIBAgIBADALBgkqhkiG9w0BAQUwEjEQMA4GA1UEChMHQWNtZSBD
bzAeFw03MDAxMDEwMDAwMDBaFw00OTEyMzEyMzU5NTlaMBIxEDAOBgNVBAoTB0Fj
bWUgQ28wWjALBgkqhkiG9w0BAQEDSwAwSAJBALyCfqwwip8BvTKgVKGdmjZTU8DD
ndR+WALmFPIRqn89bOU3s30olKiqYEju/SFoEvMyFRT/TWEhXHDaufThqaMCAwEA
AaNoMGYwDgYDVR0PAQH/BAQDAgCkMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA8GA1Ud
EwEB/wQFMAMBAf8wLgYDVR0RBCcwJYILZXhhbXBsZS5jb22HBH8AAAGHEAAAAAAA
AAAAAAAAAAAAAAEwCwYJKoZIhvcNAQEFA0EAr/09uy108p51rheIOSnz4zgduyTl
M+4AmRo8/U1twEZLgfAGG/GZjREv2y4mCEUIM3HebCAqlA5jpRg76Rf8jw==
-----END CERTIFICATE-----`)

// localhostKey is the private key for localhostCert.
var localhostKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIBOQIBAAJBALyCfqwwip8BvTKgVKGdmjZTU8DDndR+WALmFPIRqn89bOU3s30o
lKiqYEju/SFoEvMyFRT/TWEhXHDaufThqaMCAwEAAQJAPXuWUxTV8XyAt8VhNQER
LgzJcUKb9JVsoS1nwXgPksXnPDKnL9ax8VERrdNr+nZbj2Q9cDSXBUovfdtehcdP
qQIhAO48ZsPylbTrmtjDEKiHT2Ik04rLotZYS2U873J6I7WlAiEAypDjYxXyafv/
Yo1pm9onwcetQKMW8CS3AjuV9Axzj6cCIEx2Il19fEMG4zny0WPlmbrcKvD/DpJQ
4FHrzsYlIVTpAiAas7S1uAvneqd0l02HlN9OxQKKlbUNXNme+rnOnOGS2wIgS0jW
zl1jvrOSJeP1PpAHohWz6LOhEr8uvltWkN6x3vE=
-----END RSA PRIVATE KEY-----`)
