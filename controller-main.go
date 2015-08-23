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
	"github.com/minio/cli"
	"github.com/minio/minio/pkg/controller"
)

var controllerCmd = cli.Command{
	Name:   "controller",
	Usage:  "Get|Set server configuration",
	Action: controllerMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}} [get|set] [INFOTYPE] [SERVERURL]

EXAMPLES:
  1. Get disks from controller
      $ minio {{.Name}} get disks http://localhost:9001/rpc

  2. Get memstats from controller
      $ minio {{.Name}} get mem http://localhost:9001/rpc

`,
}

func controllerMain(c *cli.Context) {
	if len(c.Args()) < 2 || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "controller", 1) // last argument is exit code
	}
	if c.Args().First() == "get" {
		newArgs := c.Args().Tail()
		switch newArgs.First() {
		case "mem":
			memstats, err := controller.GetMemStats(newArgs.Tail().First())
			if err != nil {
				Fatalln(err)
			}
			Println(string(memstats))
		case "sysinfo":
			sysinfo, err := controller.GetSysInfo(newArgs.Tail().First())
			if err != nil {
				Fatalln(err)
			}
			Println(string(sysinfo))
		case "auth":
			keys, err := controller.GetAuthKeys(newArgs.Tail().First())
			if err != nil {
				Fatalln(err)
			}
			Println(string(keys))
		}
	}
	if c.Args().First() == "set" {
		Fatalln("Not supported yet")
	}
}
