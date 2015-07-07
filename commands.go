/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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
	"os"
	"os/user"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/controller"
	"github.com/minio/minio/pkg/server"
	"github.com/minio/minio/pkg/server/api"
)

var commands = []cli.Command{
	serverCmd,
	controllerCmd,
}

var serverCmd = cli.Command{
	Name:        "server",
	Description: "Server mode",
	Action:      runServer,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Start in server mode
      $ minio {{.Name}}

`,
}

var controllerCmd = cli.Command{
	Name:        "controller",
	Description: "Control mode",
	Action:      runController,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Get disks from controller
      $ minio {{.Name}} disks http://localhost:9001/rpc

  2. Get memstats from controller
      $ minio {{.Name}} mem http://localhost:9001/rpc

`,
}

func getServerConfig(c *cli.Context) api.Config {
	certFile := c.GlobalString("cert")
	keyFile := c.GlobalString("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		Fatalln("Both certificate and key are required to enable https.")
	}
	tls := (certFile != "" && keyFile != "")
	return api.Config{
		Address:   c.GlobalString("address"),
		TLS:       tls,
		CertFile:  certFile,
		KeyFile:   keyFile,
		RateLimit: c.GlobalInt("ratelimit"),
	}
}

func runServer(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	apiServerConfig := getServerConfig(c)
	err = server.StartServices(apiServerConfig)
	if err != nil {
		Fatalln(err)
	}
}

func runController(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	if len(c.Args()) <= 2 || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "controller", 1) // last argument is exit code
	}
	switch c.Args().First() {
	case "disks":
		disks, err := controller.GetDisks(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(disks)
	case "mem":
		memstats, err := controller.GetMemStats(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(string(memstats))
	case "donut":
		hostname, _ := os.Hostname()
		err := controller.SetDonut(c.Args().Tail().First(), hostname, c.Args().Tail().Tail())
		if err != nil {
			Fatalln(err)
		}
	}
}
