/*
/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

// Package http implements a wrapper around golang's own http.Server
package http

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Server struct {
	mux         *http.ServeMux
	listener    net.Listener
	enableTLS   bool
	tlsCertFile string
	tlsKeyFile  string
	mu          sync.Mutex
}

// Enable tls
func (s *Server) EnableTLS(certFile, keyFile string) {
	s.enableTLS = true
	s.tlsCertFile = certFile
	s.tlsKeyFile = keyFile
}

// Get Listen URL
func (s *Server) GetlistenURL() string {
	scheme := "http"
	if s.enableTLS {
		scheme = "https"
	}
	if s.listener != nil {
		if taddr, ok := s.listener.Addr().(*net.TCPAddr); ok {
			if taddr.IP.IsUnspecified() {
				return fmt.Sprintf("%s://localhost:%d", scheme, taddr.Port)
			}
			return fmt.Sprintf("%s://%s", scheme, s.listener.Addr())
		}
	}
	return ""
}

func (s *Server) HandleFunc(pattern string, fn func(http.ResponseWriter, *http.Request)) {
	s.mux.HandleFunc(pattern, fn)
}

func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	s.mux.ServeHTTP(rw, req)
}

// Listen starts listening on the given host:port addr.
func (s *Server) Listen(addr string) error {
	if s.listener != nil {
		return nil
	}
	if addr == "" {
		return fmt.Errorf("<host>:<port> needs to be provided to start listening")
	}

	var err error
	_, _, err = net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("Failed to split %s: %v", addr, err)
	}

	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Failed to listen on %s: %v", addr, err)
	}

	log.Printf("Starting to listen on %s\n", s.GetlistenURL())

	if s.enableTLS {
		config := &tls.Config{
			Rand:       rand.Reader,
			Time:       time.Now,
			NextProtos: []string{"http/1.1"},
		}
		config.Certificates = make([]tls.Certificate, 1)

		config.Certificates[0], err = loadX509KeyPair(s.tlsCertFile, s.tlsKeyFile)
		if err != nil {
			return fmt.Errorf("Failed to load TLS cert: %v", err)
		}
		s.listener = tls.NewListener(s.listener, config)
	}

	return nil
}

func NewServer() *Server {
	return &Server{
		mux: http.NewServeMux(),
	}
}

func loadX509KeyPair(certFile, keyFile string) (cert tls.Certificate, err error) {
	certPEMBlock, err := ioutil.ReadFile(certFile)
	if err != nil {
		return
	}
	keyPEMBlock, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return
	}
	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}
