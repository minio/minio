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

package api

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// Config - http server config
type Config struct {
	Address   string
	TLS       bool
	CertFile  string
	KeyFile   string
	RateLimit int
}

// Start http server
func Start(a API) <-chan error {
	errCh := make(chan error)
	go start(errCh, a)
	return errCh
}

func start(errCh chan error, a API) {
	defer close(errCh)

	var err error
	// Minio server config
	httpServer := &http.Server{
		Addr:           a.config.Address,
		Handler:        a.handler,
		MaxHeaderBytes: 1 << 20,
	}

	host, port, err := net.SplitHostPort(a.config.Address)
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
		err = httpServer.ListenAndServe()
	case a.config.TLS == true:
		for _, host := range hosts {
			fmt.Printf("Starting minio server on: https://%s:%s\n", host, port)
		}
		err = httpServer.ListenAndServeTLS(a.config.CertFile, a.config.KeyFile)
	}
	if err != nil {
		errCh <- err
	}
	errCh <- nil
	return
}

// API is used to build api server
type API struct {
	config  Config
	handler http.Handler
}

// StartServer APIFactory builds api server
func StartServer(conf Config) error {
	for err := range Start(New(conf)) {
		if err != nil {
			return err
		}
	}
	return nil
}
