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

package httpserver

import (
	"log"
	"net/http"
	"time"
)

type HttpServer struct {
	Address  string
	TLS      bool
	CertFile string
	KeyFile  string
}

func Start(handler http.Handler, srv HttpServer) (chan<- string, <-chan error) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel, handler, srv)
	return ctrlChannel, errorChannel
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, router http.Handler, srv HttpServer) {
	var err error

	// Minio server config
	server := &http.Server{
		Addr:           srv.Address,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Println("Starting HTTP Server on " + srv.Address)

	if srv.TLS {
		server.TLSConfig = getDefaultTLSConfig()
		err = server.ListenAndServeTLS(srv.CertFile, srv.KeyFile)
	} else {
		err = server.ListenAndServe()
	}
	errorChannel <- err
	close(errorChannel)
}
