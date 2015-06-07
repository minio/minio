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
type Server struct{}

// Start http server
func Start(handler http.Handler, config Config) (chan<- string, <-chan error, *Server) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	server := Server{}
	go start(ctrlChannel, errorChannel, handler, config, &server)
	return ctrlChannel, errorChannel, &server
}

func start(ctrlChannel <-chan string, errorChannel chan<- error,
	router http.Handler, config Config, server *Server) {
	var err error

	// Minio server config
	httpServer := &http.Server{
		Addr:           config.Address,
		Handler:        router,
		MaxHeaderBytes: 1 << 20,
	}

	host, port, err := net.SplitHostPort(config.Address)
	errorChannel <- err

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		errorChannel <- err
		for _, addr := range addrs {
			if addr.Network() == "ip+net" {
				h := strings.Split(addr.String(), "/")[0]
				if ip := net.ParseIP(h); ip.To4() != nil {
					hosts = append(hosts, h)
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
	case config.TLS == true:
		fmt.Printf("Starting minio server on: https://%s:%s\n", host, port)
		httpServer.TLSConfig = getDefaultTLSConfig()
		err = httpServer.ListenAndServeTLS(config.CertFile, config.KeyFile)
	}
	errorChannel <- err
	close(errorChannel)
}
