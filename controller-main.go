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
	"encoding/json"
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
  minio {{.Name}} [OPTION]

EXAMPLES:
  1. Start minio controller
      $ minio {{.Name}}

  2. Fetch stored access keys
      $ minio {{.Name}} keys
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
			Printf("Starting minio controller on: https://%s:%s, PID: %d\n", host, port, os.Getpid())
		} else {
			Printf("Starting minio controller on: http://%s:%s, PID: %d\n", host, port, os.Getpid())
		}
	}
	return rpcServer, nil
}

// startController starts a minio controller
func startController(conf minioConfig) *probe.Error {
	rpcServer, err := configureControllerRPC(conf, getControllerRPCHandler(conf.Anonymous))
	if err != nil {
		return err.Trace()
	}
	// Setting rate limit to 'zero' no ratelimiting implemented
	if err := minhttp.ListenAndServeLimited(0, rpcServer); err != nil {
		return err.Trace()
	}
	return nil
}

func genAuthFirstTime() (*AuthConfig, *probe.Error) {
	if isAuthConfigFileExists() {
		return nil, nil
	}
	// Initialize new config, since config file doesn't exist yet
	config := &AuthConfig{}
	config.Version = "0.0.1"
	config.Users = make(map[string]*AuthUser)

	config.Users["admin"] = &AuthUser{
		Name:            "admin",
		AccessKeyID:     "admin",
		SecretAccessKey: string(mustGenerateSecretAccessKey()),
	}
	config.Users["user"] = &AuthUser{
		Name:            "user",
		AccessKeyID:     string(mustGenerateAccessKeyID()),
		SecretAccessKey: string(mustGenerateSecretAccessKey()),
	}
	if err := SaveConfig(config); err != nil {
		return nil, err.Trace()
	}
	return config, nil
}

func getAuth() (*AuthConfig, *probe.Error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, err.Trace()
	}
	return config, nil
}

type accessKeys struct {
	*AuthUser
}

func (a accessKeys) String() string {
	return colorizeMessage(fmt.Sprintf("Username: %s, AccessKey: %s, SecretKey: %s", a.Name, a.AccessKeyID, a.SecretAccessKey))
}

// JSON - json formatted output
func (a accessKeys) JSON() string {
	b, err := json.Marshal(a)
	errorIf(probe.NewError(err), "Unable to marshal json", nil)
	return string(b)
}

// firstTimeAuth first time authorization
func firstTimeAuth() *probe.Error {
	conf, err := genAuthFirstTime()
	if err != nil {
		return err.Trace()
	}
	if conf != nil {
		Println("Running for first time, generating access keys.")
		for _, user := range conf.Users {
			if globalJSONFlag {
				Println(accessKeys{user}.JSON())
			} else {
				Println(accessKeys{user})
			}

		}
		Println("To fetch your keys again.")
		Println("  $ minio controller keys")
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
	if c.Args().Present() && c.Args().First() != "keys" {
		cli.ShowCommandHelpAndExit(c, "controller", 1)
	}

	if c.Args().First() == "keys" {
		conf, err := getAuth()
		fatalIf(err.Trace(), "Failed to fetch keys for minio controller.", nil)
		if conf != nil {
			for _, user := range conf.Users {
				if globalJSONFlag {
					Println(accessKeys{user}.JSON())
				} else {
					Println(accessKeys{user})
				}
			}
		}
		return
	}

	err := firstTimeAuth()
	fatalIf(err.Trace(), "Failed to generate keys for minio.", nil)

	err = startController(getControllerConfig(c))
	fatalIf(err.Trace(), "Failed to start minio controller.", nil)
}
