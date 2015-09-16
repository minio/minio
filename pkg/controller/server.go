/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package controller

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio/pkg/minhttp"
	"github.com/minio/minio/pkg/probe"
)

// getRPCServer instance
func getRPCServer(rpcHandler http.Handler) (*http.Server, *probe.Error) {
	// Minio server config
	httpServer := &http.Server{
		Addr:           ":9001", // TODO make this configurable
		Handler:        rpcHandler,
		MaxHeaderBytes: 1 << 20,
	}
	var hosts []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, probe.NewError(err)
	}
	for _, addr := range addrs {
		if addr.Network() == "ip+net" {
			host := strings.Split(addr.String(), "/")[0]
			if ip := net.ParseIP(host); ip.To4() != nil {
				hosts = append(hosts, host)
			}
		}
	}
	for _, host := range hosts {
		fmt.Printf("Starting minio server on: http://%s:9001/rpc, PID: %d\n", host, os.Getpid())
	}
	return httpServer, nil
}

// Start starts a controller
func Start() *probe.Error {
	rpcServer, err := getRPCServer(getRPCHandler())
	if err != nil {
		return err.Trace()
	}
	// Setting rate limit to 'zero' no ratelimiting implemented
	if err := minhttp.ListenAndServeLimited(0, rpcServer); err != nil {
		return err.Trace()
	}
	return nil
}
