/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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
	"os"
	"strings"

	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/server/api"
	"github.com/minio/minio/pkg/server/minhttp"
)

// getAPI server instance
func getAPIServer(conf api.Config, apiHandler http.Handler) (*http.Server, *probe.Error) {
	// Minio server config
	httpServer := &http.Server{
		Addr:           conf.Address,
		Handler:        apiHandler,
		MaxHeaderBytes: 1 << 20,
	}

	if conf.TLS {
		var err error
		httpServer.TLSConfig = &tls.Config{}
		httpServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		httpServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, probe.New(err)
		}
	}

	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		return nil, probe.New(err)
	}

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return nil, probe.New(err)
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
			fmt.Printf("Starting minio server on: https://%s:%s, PID: %d\n", host, port, os.Getpid())
		} else {
			fmt.Printf("Starting minio server on: http://%s:%s, PID: %d\n", host, port, os.Getpid())
		}

	}
	return httpServer, nil
}

// getRPCServer instance
func getRPCServer(rpcHandler http.Handler) *http.Server {
	// Minio server config
	httpServer := &http.Server{
		Addr:           ":9001", // TODO make this configurable
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
func StartServices(conf api.Config) *probe.Error {
	apiHandler, minioAPI := getAPIHandler(conf)
	apiServer, err := getAPIServer(conf, apiHandler)
	if err != nil {
		return err.Trace()
	}
	rpcServer := getRPCServer(getRPCHandler())
	// start ticket master
	go startTM(minioAPI)

	if err := minhttp.ListenAndServeLimited(conf.RateLimit, apiServer, rpcServer); err != nil {
		return err.Trace()
	}
	return nil
}
