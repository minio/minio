/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/server/api"
	"github.com/minio/minio/pkg/server/nimble"
)

// startServices start all services
func startServices(errCh chan error, servers ...*http.Server) {
	defer close(errCh)
	errCh <- nimble.ListenAndServe(servers...)
}

// getAPI server instance
func getAPIServer(conf api.Config, apiHandler http.Handler) (*http.Server, error) {
	// Minio server config
	httpServer := &http.Server{
		Addr:           conf.Address,
		Handler:        apiHandler,
		MaxHeaderBytes: 1 << 20,
	}

	if conf.TLS {
		config := &tls.Config{}
		if httpServer.TLSConfig != nil {
			*config = *httpServer.TLSConfig
		}

		var err error
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, iodine.New(err, nil)
		}
	}

	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		return nil, iodine.New(err, nil)
	}

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for _, addr := range addrs {
			if addr.Network() == "ip+net" {
				host := strings.Split(addr.String(), "/")[0]
				if ip := net.ParseIP(host); ip.To4() != nil {
					hosts = append(hosts, host)
				}
			}
		}
	}

	for _, host := range hosts {
		if conf.TLS {
			fmt.Printf("Starting minio server on: https://%s:%s\n", host, port)
		} else {
			fmt.Printf("Starting minio server on: http://%s:%s\n", host, port)
		}

	}
	return httpServer, nil
}

// getRPCServer instance
func getRPCServer(rpcHandler http.Handler) *http.Server {
	// Minio server config
	httpServer := &http.Server{
		Addr:           "127.0.0.1:9001", // TODO make this configurable
		Handler:        rpcHandler,
		MaxHeaderBytes: 1 << 20,
	}
	return httpServer
}

// Start ticket master
func startTM(a api.Minio) {
	for {
		for op := range a.OP {
			op.ProceedCh <- struct{}{}
		}
	}
}

// StartServices starts basic services for a server
func StartServices(conf api.Config, doneCh chan struct{}) error {
	errCh := make(chan error)
	apiHandler, minioAPI := getAPIHandler(conf)
	apiServer, err := getAPIServer(conf, apiHandler)
	if err != nil {
		return iodine.New(err, nil)
	}
	rpcServer := getRPCServer(getRPCHandler())
	go startServices(errCh, apiServer, rpcServer)
	go startTM(minioAPI)

	select {
	case err := <-errCh:
		return iodine.New(err, nil)
	case <-doneCh:
		return nil
	}
}
