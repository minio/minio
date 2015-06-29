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

package httpserver

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

// Server - http server related
type Server struct {
	config  Config
	handler http.Handler
}

// Start http server
func Start(handler http.Handler, config Config) (chan<- string, <-chan error, Server) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	server := Server{
		config:  config,
		handler: handler,
	}
	go start(ctrlChannel, errorChannel, server)
	return ctrlChannel, errorChannel, server
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, server Server) {
	defer close(errorChannel)

	var err error
	// Minio server config
	httpServer := &http.Server{
		Addr:           server.config.Address,
		Handler:        server.handler,
		MaxHeaderBytes: 1 << 20,
	}

	host, port, err := net.SplitHostPort(server.config.Address)
	if err != nil {
		errorChannel <- err
		return
	}

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			errorChannel <- err
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
	case server.config.TLS == true:
		for _, host := range hosts {
			fmt.Printf("Starting minio server on: https://%s:%s\n", host, port)
		}
		err = httpServer.ListenAndServeTLS(server.config.CertFile, server.config.KeyFile)
	}
	if err != nil {
		errorChannel <- err
	}

}
