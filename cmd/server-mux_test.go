/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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

package cmd

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestListenerAcceptAfterClose(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 10; i++ {
				runTest(t)
			}
		}()
	}
	wg.Wait()
}

func runTest(t *testing.T) {
	const connectionsBeforeClose = 1

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ln = newListenerMux(ln, &tls.Config{})

	addr := ln.Addr().String()
	waitForListener := make(chan error)
	go func() {
		defer close(waitForListener)

		var connCount int
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			connCount++
			if connCount > connectionsBeforeClose {
				waitForListener <- errUnexpected
				return
			}
			conn.Close()
		}
	}()

	for i := 0; i < connectionsBeforeClose; i++ {
		err = dial(addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	ln.Close()
	dial(addr)

	err = <-waitForListener
	if err != nil {
		t.Fatal(err)
	}
}

func dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return err
}

// Tests initializing listeners.
func TestInitListeners(t *testing.T) {
	portTest1 := getFreePort()
	portTest2 := getFreePort()
	testCases := []struct {
		serverAddr string
		shouldPass bool
	}{
		// Test 1 with ip and port.
		{
			serverAddr: "127.0.0.1:" + portTest1,
			shouldPass: true,
		},
		// Test 2 only port.
		{
			serverAddr: ":" + portTest2,
			shouldPass: true,
		},
		// Test 3 with no port error.
		{
			serverAddr: "127.0.0.1",
			shouldPass: false,
		},
		// Test 4 with 'foobar' host not resolvable.
		{
			serverAddr: "foobar:9000",
			shouldPass: false,
		},
	}
	for i, testCase := range testCases {
		listeners, err := initListeners(testCase.serverAddr, &tls.Config{})
		if testCase.shouldPass {
			if err != nil {
				t.Fatalf("Test %d: Unable to initialize listeners %s", i+1, err)
			}
			for _, listener := range listeners {
				if err = listener.Close(); err != nil {
					t.Fatalf("Test %d: Unable to close listeners %s", i+1, err)
				}
			}
		}
		if err == nil && !testCase.shouldPass {
			t.Fatalf("Test %d: Should fail but is successful", i+1)
		}
	}
	// Windows doesn't have 'localhost' hostname.
	if runtime.GOOS != globalWindowsOSName {
		listeners, err := initListeners("localhost:"+getFreePort(), &tls.Config{})
		if err != nil {
			t.Fatalf("Test 3: Unable to initialize listeners %s", err)
		}
		for _, listener := range listeners {
			if err = listener.Close(); err != nil {
				t.Fatalf("Test 3: Unable to close listeners %s", err)
			}
		}
	}
}

func TestClose(t *testing.T) {
	// Create ServerMux
	m := NewServerMux("", nil)

	if err := m.Close(); err != nil {
		t.Error("Server errored while trying to Close", err)
	}

	// Closing again should return an error.
	if err := m.Close(); err.Error() != "Server has been closed" {
		t.Error("Unexepcted error expected \"Server has been closed\", got", err)
	}
}

func TestServerMux(t *testing.T) {
	var err error
	var got []byte
	var res *http.Response

	// Create ServerMux
	m := NewServerMux("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))
	// Start serving requests
	go m.ListenAndServe("", "")

	// Issue a GET request. Since we started server in a goroutine, it could be not ready
	// at this point. So we allow until 5 failed retries before declare there is an error
	for i := 0; i < 5; i++ {
		// Sleep one second
		time.Sleep(1 * time.Second)
		// Check if one listener is ready
		m.mu.Lock()
		listenersCount := len(m.listeners)
		m.mu.Unlock()
		if listenersCount == 0 {
			continue
		}
		m.mu.Lock()
		listenerAddr := m.listeners[0].Addr().String()
		m.mu.Unlock()
		// Issue the GET request
		client := http.Client{}
		res, err = client.Get("http://" + listenerAddr)
		if err != nil {
			continue
		}
		// Read the request response
		got, err = ioutil.ReadAll(res.Body)
		if err != nil {
			continue
		}
		// We've got a response, quit the loop
		break
	}

	// Check for error persisted after 5 times
	if err != nil {
		t.Fatal(err)
	}

	// Check the web service response
	if string(got) != "hello" {
		t.Errorf("got %q, want hello", string(got))
	}
}

func TestServerCloseBlocking(t *testing.T) {
	// Create ServerMux
	m := NewServerMux("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))

	// Start serving requests in a goroutine
	go m.ListenAndServe("", "")

	// Dial, try until 5 times before declaring a failure
	dial := func() (net.Conn, error) {
		var c net.Conn
		var err error
		for i := 0; i < 5; i++ {
			// Sleep one second in case of the server is not ready yet
			time.Sleep(1 * time.Second)
			// Check if there is at least one listener configured
			m.mu.Lock()
			if len(m.listeners) == 0 {
				m.mu.Unlock()
				continue
			}
			m.mu.Unlock()
			// Run the actual Dial
			m.mu.Lock()
			c, err = net.Dial("tcp", m.listeners[0].Addr().String())
			m.mu.Unlock()
			if err != nil {
				continue
			}
		}
		return c, err
	}

	// Dial to open a StateNew but don't send anything
	cnew, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	defer cnew.Close()

	// Dial another connection but idle after a request to have StateIdle
	cidle, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	defer cidle.Close()

	cidle.Write([]byte("HEAD / HTTP/1.1\r\nHost: foo\r\n\r\n"))
	_, err = http.ReadResponse(bufio.NewReader(cidle), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure we don't block forever.
	m.Close()
}

func TestServerListenAndServePlain(t *testing.T) {
	wait := make(chan struct{})
	addr := net.JoinHostPort("127.0.0.1", getFreePort())
	errc := make(chan error)
	once := &sync.Once{}

	// Initialize done channel specifically for each tests.
	globalServiceDoneCh = make(chan struct{}, 1)

	// Create ServerMux and when we receive a request we stop waiting
	m := NewServerMux(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
		once.Do(func() { close(wait) })
	}))

	// ListenAndServe in a goroutine, but we don't know when it's ready
	go func() { errc <- m.ListenAndServe("", "") }()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	// Keep trying the server until it's accepting connections
	go func() {
		client := http.Client{Timeout: time.Millisecond * 10}
		ok := false
		for !ok {
			res, _ := client.Get("http://" + addr)
			if res != nil && res.StatusCode == http.StatusOK {
				ok = true
			}
		}

		wg.Done()
	}()

	wg.Wait()

	// Block until we get an error or wait closed
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-wait:
		m.Close() // Shutdown the ServerMux
		return
	}
}

func TestServerListenAndServeTLS(t *testing.T) {
	wait := make(chan struct{})
	addr := net.JoinHostPort("127.0.0.1", getFreePort())
	errc := make(chan error)
	once := &sync.Once{}

	// Initialize done channel specifically for each tests.
	globalServiceDoneCh = make(chan struct{}, 1)

	// Create ServerMux and when we receive a request we stop waiting
	m := NewServerMux(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
		once.Do(func() { close(wait) })
	}))

	// Create a cert
	err := createConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	certFile := getPublicCertFile()
	keyFile := getPrivateKeyFile()
	defer os.RemoveAll(certFile)
	defer os.RemoveAll(keyFile)

	err = generateTestCert(addr)
	if err != nil {
		t.Error(err)
		return
	}

	// ListenAndServe in a goroutine, but we don't know when it's ready
	go func() { errc <- m.ListenAndServe(certFile, keyFile) }()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	// Keep trying the server until it's accepting connections
	go func() {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client := http.Client{
			Timeout:   time.Millisecond * 10,
			Transport: tr,
		}
		okTLS := false
		for !okTLS {
			res, _ := client.Get("https://" + addr)
			if res != nil && res.StatusCode == http.StatusOK {
				okTLS = true
			}
		}

		okNoTLS := false
		for !okNoTLS {
			res, _ := client.Get("http://" + addr)
			// Without TLS we expect a re-direction from http to https
			// And also the request is not rejected.
			if res != nil && res.StatusCode == http.StatusOK && res.Request.URL.Scheme == httpsScheme {
				okNoTLS = true
			}
		}
		wg.Done()
	}()

	wg.Wait()

	// Block until we get an error or wait closed
	select {
	case err := <-errc:
		if err != nil {
			t.Error(err)
			return
		}
	case <-wait:
		m.Close() // Shutdown the ServerMux
		return
	}
}

// generateTestCert creates a cert and a key used for testing only
func generateTestCert(host string) error {
	certPath := getPublicCertFile()
	keyPath := getPrivateKeyFile()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Minio Test Cert"},
		},
		NotBefore: UTCNow(),
		NotAfter:  UTCNow().Add(time.Minute * 1),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = append(template.IPAddresses, ip)
	}

	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageCertSign

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	keyOut.Close()
	return nil
}
