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

package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/minhttp"
	"github.com/minio/minio/pkg/probe"
)

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "Start minio server.",
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Start minio server
      $ minio {{.Name}}

`,
}

// configureAPIServer configure a new server instance
func configureAPIServer(conf minioConfig, apiHandler http.Handler) (*http.Server, *probe.Error) {
	// Minio server config
	apiServer := &http.Server{
		Addr:           conf.Address,
		Handler:        apiHandler,
		MaxHeaderBytes: 1 << 20,
	}

	if conf.TLS {
		var err error
		apiServer.TLSConfig = &tls.Config{}
		apiServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		apiServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, probe.NewError(err)
		}
	}

	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		return nil, probe.NewError(err)
	}

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
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
	}

	for _, host := range hosts {
		if conf.TLS {
			Printf("Starting minio server on: https://%s:%s, PID: %d\n", host, port, os.Getpid())
		} else {
			Printf("Starting minio server on: http://%s:%s, PID: %d\n", host, port, os.Getpid())
		}
	}
	return apiServer, nil
}

// configureServerRPC configure server rpc port
func configureServerRPC(conf minioConfig, rpcHandler http.Handler) (*http.Server, *probe.Error) {
	// Minio server config
	rpcServer := &http.Server{
		Addr:           conf.RPCAddress,
		Handler:        rpcHandler,
		MaxHeaderBytes: 1 << 20,
	}

	if conf.TLS {
		var err error
		rpcServer.TLSConfig = &tls.Config{}
		rpcServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		rpcServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, probe.NewError(err)
		}
	}
	return rpcServer, nil
}

// Start ticket master
func startTM(api API) {
	for {
		for op := range api.OP {
			op.ProceedCh <- struct{}{}
		}
	}
}

// startServer starts an s3 compatible cloud storage server
func startServer(conf minioConfig) *probe.Error {
	minioAPI := getNewAPI(conf.Anonymous)
	apiHandler := getAPIHandler(conf.Anonymous, minioAPI)
	apiServer, err := configureAPIServer(conf, apiHandler)
	if err != nil {
		return err.Trace()
	}
	rpcServer, err := configureServerRPC(conf, getServerRPCHandler())

	// start ticket master
	go startTM(minioAPI)
	if err := minhttp.ListenAndServe(apiServer, rpcServer); err != nil {
		return err.Trace()
	}
	return nil
}

func getServerConfig(c *cli.Context) minioConfig {
	certFile := c.GlobalString("cert")
	keyFile := c.GlobalString("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		Fatalln("Both certificate and key are required to enable https.")
	}
	tls := (certFile != "" && keyFile != "")
	return minioConfig{
		Address:    c.GlobalString("address"),
		RPCAddress: c.GlobalString("address-server-rpc"),
		Anonymous:  c.GlobalBool("anonymous"),
		TLS:        tls,
		CertFile:   certFile,
		KeyFile:    keyFile,
		RateLimit:  c.GlobalInt("ratelimit"),
	}
}

func serverMain(c *cli.Context) {
	if c.Args().Present() {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}

	apiServerConfig := getServerConfig(c)
	err := startServer(apiServerConfig)
	errorIf(err.Trace(), "Failed to start the minio server.", nil)
}
