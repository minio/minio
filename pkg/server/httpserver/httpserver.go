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
	"net/http"

	"github.com/minio-io/minio/pkg/utils/log"
)

// Config - http server config
type Config struct {
	Address   string
	TLS       bool
	CertFile  string
	KeyFile   string
	Websocket bool // TODO
	//	Domain    string
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
		Addr:    config.Address,
		Handler: router,
		// TODO add this later with a proper timer thread
		//		ReadTimeout:    20 * time.Second,
		//		WriteTimeout:   20 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Println("Starting HTTP Server on:", config.Address)

	if config.TLS {
		httpServer.TLSConfig = getDefaultTLSConfig()
		err = httpServer.ListenAndServeTLS(config.CertFile, config.KeyFile)
	} else {
		err = httpServer.ListenAndServe()
	}
	errorChannel <- err
	close(errorChannel)
}
