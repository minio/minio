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

package cmd

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestClose(t *testing.T) {
	// Create ServerMux
	m := NewServerMux("", nil)

	if err := m.Close(); err != nil {
		t.Error("Server errored while trying to Close", err)
	}
}

func TestServerMux(t *testing.T) {
	ts := httptest.NewUnstartedServer(nil)
	defer ts.Close()

	// Create ServerMux
	m := NewServerMux("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))

	// Set the test server config to the mux
	ts.Config = &m.Server
	ts.Start()

	// Create a ListenerMux
	lm := &ListenerMux{
		Listener: ts.Listener,
		config:   &tls.Config{},
	}
	m.listener = lm

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
	ts := httptest.NewUnstartedServer(nil)
	defer ts.Close()

	// Create ServerMux
	m := NewServerMux("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))

	// Set the test server config to the mux
	ts.Config = &m.Server
	ts.Start()

	// Create a ListenerMux.
	lm := &ListenerMux{
		Listener: ts.Listener,
		config:   &tls.Config{},
	}
	m.listener = lm

	dial := func() net.Conn {
		c, cerr := net.Dial("tcp", ts.Listener.Addr().String())
		if cerr != nil {
			t.Fatal(cerr)
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
	_, err := http.ReadResponse(bufio.NewReader(cidle), nil)
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

func TestListenAndServePlain(t *testing.T) {
	wait := make(chan struct{})
	addr := "127.0.0.1:" + strconv.Itoa(getFreePort())
	errc := make(chan error)
	once := &sync.Once{}

	// Create ServerMux and when we receive a request we stop waiting
	m := NewServerMux(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
		once.Do(func() { close(wait) })
	}))

	// ListenAndServe in a goroutine, but we don't know when it's ready
	go func() { errc <- m.ListenAndServe() }()

	// Make sure we don't block by closing wait after a timeout
	tf := time.AfterFunc(time.Millisecond*500, func() { errc <- errors.New("Unable to connect to server") })

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
		tf.Stop() // Cancel the timeout since we made a successful request
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

func TestListenAndServeTLS(t *testing.T) {
	wait := make(chan struct{})
	addr := "127.0.0.1:" + strconv.Itoa(getFreePort())
	errc := make(chan error)
	once := &sync.Once{}

	// Create ServerMux and when we receive a request we stop waiting
	m := NewServerMux(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
		once.Do(func() { close(wait) })
	}))

	// Create a cert
	err := createCertsPath()
	if err != nil {
		t.Fatal(err)
	}
	certFile := mustGetCertFile()
	keyFile := mustGetKeyFile()
	defer os.RemoveAll(certFile)
	defer os.RemoveAll(keyFile)

	err = generateTestCert(addr)
	if err != nil {
		t.Error(err)
		return
	}

	// ListenAndServe in a goroutine, but we don't know when it's ready
	go func() { errc <- m.ListenAndServeTLS(certFile, keyFile) }()

	// Make sure we don't block by closing wait after a timeout
	tf := time.AfterFunc(time.Millisecond*500, func() { errc <- errors.New("Unable to connect to server") })
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
		ok := false
		for !ok {
			res, _ := client.Get("https://" + addr)
			if res != nil && res.StatusCode == http.StatusOK {
				ok = true
			}
		}

		wg.Done()
		tf.Stop() // Cancel the timeout since we made a successful request
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
	certPath := mustGetCertFile()
	keyPath := mustGetKeyFile()
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
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Minute * 1),

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
