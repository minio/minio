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
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/server/api"
)

// Start API listener
func startAPI(errCh chan error, conf api.Config, apiHandler http.Handler) {
	defer close(errCh)

	// Minio server config
	httpServer := &http.Server{
		Addr:           conf.Address,
		Handler:        apiHandler,
		MaxHeaderBytes: 1 << 20,
	}

	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		errCh <- err
		return
	}

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			errCh <- err
			return
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
	switch {
	default:
		for _, host := range hosts {
			fmt.Printf("Starting minio server on: http://%s:%s\n", host, port)
		}
		errCh <- httpServer.ListenAndServe()
	case conf.TLS == true:
		for _, host := range hosts {
			fmt.Printf("Starting minio server on: https://%s:%s\n", host, port)
		}
		errCh <- httpServer.ListenAndServeTLS(conf.CertFile, conf.KeyFile)
	}
}

// Start RPC listener
func startRPC(errCh chan error, rpcHandler http.Handler) {
	defer close(errCh)

	// Minio server config
	httpServer := &http.Server{
		Addr:           "127.0.0.1:9001", // TODO make this configurable
		Handler:        rpcHandler,
		MaxHeaderBytes: 1 << 20,
	}
	errCh <- httpServer.ListenAndServe()
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
func StartServices(conf api.Config) error {
	apiErrCh := make(chan error)
	rpcErrCh := make(chan error)

	apiHandler, minioAPI := getAPIHandler(conf)
	go startAPI(apiErrCh, conf, apiHandler)
	go startRPC(rpcErrCh, getRPCHandler())
	go startTM(minioAPI)

	select {
	case err := <-apiErrCh:
		return err
	case err := <-rpcErrCh:
		return err
	}
}
