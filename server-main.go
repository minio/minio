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

import "github.com/minio/cli"

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

func getServerConfig(c *cli.Context) APIConfig {
	certFile := c.GlobalString("cert")
	keyFile := c.GlobalString("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		Fatalln("Both certificate and key are required to enable https.")
	}
	tls := (certFile != "" && keyFile != "")

	return APIConfig{
		Address:    c.GlobalString("address"),
		AddressRPC: c.GlobalString("address-rpcserver"),
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
	err := StartServer(apiServerConfig)
	errorIf(err.Trace(), "Failed to start the minio server.", nil)
}
