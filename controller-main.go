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

func controllerMain(c *cli.Context) {
	if c.Args().Present() {
		cli.ShowCommandHelpAndExit(c, "controller", 1)
	}
	err := controller.StartController()
	if err != nil {
		Fatalln(err)
	}
}
