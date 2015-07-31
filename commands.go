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
	"os"
	"os/user"
	"path/filepath"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/controller"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/server"
	"github.com/minio/minio/pkg/server/api"
)

var commands = []cli.Command{
	serverCmd,
	controllerCmd,
	donutCmd,
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

var donutSubCommands = []cli.Command{
	{
		Name:        "make",
		Description: "make a donut",
		Action:      runMkdonut,
		CustomHelpTemplate: `NAME:
  donut {{.Name}} - {{.Description}}

USAGE:
  donut {{.Name}} DONUTNAME [DISKS...]

EXAMPLES:
  1. Make a donut with 4 exports
      $ donut {{.Name}} mongodb-backup /mnt/export1 /mnt/export2 /mnt/export3 /mnt/export4

  2. Make a donut with 16 exports
      $ donut {{.Name}} operational-data /mnt/export1 /mnt/export2 /mnt/export3 /mnt/export4 /mnt/export5 \
       /mnt/export6 /mnt/export7 /mnt/export8 /mnt/export9 /mnt/export10 /mnt/export11 \
       /mnt/export12 /mnt/export13 /mnt/export14 /mnt/export15 /mnt/export16
`,
	},
}

var donutCmd = cli.Command{
	Name:        "donut",
	Description: "Donut maker",
	Subcommands: donutSubCommands,
}

func runMkdonut(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "make", 1)
	}
	donutName := c.Args().First()
	if c.Args().First() != "" {
		if !donut.IsValidDonut(donutName) {
			Fatalf("Invalid donutname %s\n", donutName)
		}
	}
	var disks []string
	for _, disk := range c.Args().Tail() {
		if _, err := isUsable(disk); err != nil {
			Fatalln(err)
		}
		disks = append(disks, disk)
	}
	for _, disk := range disks {
		if err := os.MkdirAll(filepath.Join(disk, donutName), 0700); err != nil {
			Fatalln(err)
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		Fatalln(err)
	}
	donutConfig := &donut.Config{}
	donutConfig.Version = "0.0.1"
	donutConfig.DonutName = donutName
	donutConfig.NodeDiskMap = make(map[string][]string)
	// keep it in exact order as it was specified, do not try to sort disks
	donutConfig.NodeDiskMap[hostname] = disks
	// default cache is unlimited
	donutConfig.MaxSize = 512000000

	if err := donut.SaveConfig(donutConfig); err != nil {
		Fatalln(err)
	}

	Infoln("Success!")
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
	if c.Args().Present() {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}
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
	if len(c.Args()) < 2 || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "controller", 1) // last argument is exit code
	}
	switch c.Args().First() {
	case "mem":
		memstats, err := controller.GetMemStats(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(string(memstats))
	case "sysinfo":
		sysinfo, err := controller.GetSysInfo(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(string(sysinfo))
	case "auth":
		keys, err := controller.GetAuthKeys(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(string(keys))
	}
}
