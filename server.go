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

package main
<<<<<<< HEAD

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio/pkg/minhttp"
	"github.com/minio/minio/pkg/probe"
)

func configureServer(conf APIConfig, httpServer *http.Server) *probe.Error {
	if conf.TLS {
		var err error
		httpServer.TLSConfig = &tls.Config{}
		httpServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		httpServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return probe.NewError(err)
		}
	}

	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		return probe.NewError(err)
	}

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return probe.NewError(err)
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
	return nil
}

// getAPI server instance
func getAPIServer(conf APIConfig, apiHandler http.Handler) (*http.Server, *probe.Error) {
	// Minio server config
	httpServer := &http.Server{
		Addr:           conf.Address,
		Handler:        apiHandler,
		MaxHeaderBytes: 1 << 20,
	}
	if err := configureServer(conf, httpServer); err != nil {
		return nil, err
	}
	return httpServer, nil
}

// Start ticket master
func startTM(a MinioAPI) {
	for {
		for op := range a.OP {
			op.ProceedCh <- struct{}{}
		}
	}
}

func getServerRPCServer(conf APIConfig, handler http.Handler) (*http.Server, *probe.Error) {
	httpServer := &http.Server{
		Addr:           conf.AddressRPC,
		Handler:        handler,
		MaxHeaderBytes: 1 << 20,
	}
	if err := configureServer(conf, httpServer); err != nil {
		return nil, err
	}
	return httpServer, nil
}

// Start starts a s3 compatible cloud storage server
func StartServer(conf APIConfig) *probe.Error {
	apiHandler, minioAPI := getAPIHandler(conf)
	apiServer, err := getAPIServer(conf, apiHandler)
	if err != nil {
		return err.Trace()
	}
	// start ticket master
	go startTM(minioAPI)
	rpcHandler := getRPCServerHandler()
	rpcServer, err := getServerRPCServer(conf, rpcHandler)
	if err != nil {
		return err.Trace()
	}
	if err := minhttp.ListenAndServeLimited(conf.RateLimit, apiServer, rpcServer); err != nil {
		return err.Trace()
	}
	return nil
}
=======
>>>>>>> Consolidating more codebase and cleanup in server / controller
