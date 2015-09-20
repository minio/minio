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
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/minhttp"
	"github.com/minio/minio/pkg/probe"
)

var controllerCmd = cli.Command{
	Name:   "controller",
	Usage:  "Start minio controller",
	Action: controllerMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Start minio controller
      $ minio {{.Name}}

`,
}

// configureControllerRPC instance
func configureControllerRPC(conf minioConfig, rpcHandler http.Handler) (*http.Server, *probe.Error) {
	// Minio server config
	rpcServer := &http.Server{
		Addr:           conf.ControllerAddress,
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

	host, port, err := net.SplitHostPort(conf.ControllerAddress)
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
			fmt.Printf("Starting minio controller on: https://%s:%s, PID: %d\n", host, port, os.Getpid())
		} else {
			fmt.Printf("Starting minio controller on: http://%s:%s, PID: %d\n", host, port, os.Getpid())
		}
	}
	return rpcServer, nil
}

// startController starts a minio controller
func startController(conf minioConfig) *probe.Error {
	rpcServer, err := configureControllerRPC(conf, getControllerRPCHandler())
	if err != nil {
		return err.Trace()
	}
	// Setting rate limit to 'zero' no ratelimiting implemented
	if err := minhttp.ListenAndServeLimited(0, rpcServer); err != nil {
		return err.Trace()
	}
	return nil
}

func getControllerConfig(c *cli.Context) minioConfig {
	certFile := c.GlobalString("cert")
	keyFile := c.GlobalString("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		Fatalln("Both certificate and key are required to enable https.")
	}
	tls := (certFile != "" && keyFile != "")
	return minioConfig{
		ControllerAddress: c.GlobalString("address-controller"),
		TLS:               tls,
		CertFile:          certFile,
		KeyFile:           keyFile,
		RateLimit:         c.GlobalInt("ratelimit"),
	}
}

func controllerMain(c *cli.Context) {
	if c.Args().Present() {
		cli.ShowCommandHelpAndExit(c, "controller", 1)
	}

	err := startController(getControllerConfig(c))
	errorIf(err.Trace(), "Failed to start minio controller.", nil)
}
