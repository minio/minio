/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"

	"net/rpc"
	"net/url"

	"github.com/minio/cli"
)

var controlCmd = cli.Command{
	Name:   "control",
	Usage:  "Minio control commands.",
	Action: mainControl,
	Subcommands: []cli.Command{
		healCmd,
	},
}

func mainControl(c *cli.Context) {
	fmt.Println("mainControl")
	cli.ShowCommandHelp(c, "")
}

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "healing",
	Action: healControl,
}

func healControl(c *cli.Context) {
	if len(c.Args()) != 1 {
		fmt.Println("should have 1 argument")
	}

	parsedURL, err := url.ParseRequestURI(c.Args()[0])
	fatalIf(err, "parse heal URL")

	_, err = rpc.DialHTTPPath("tcp", parsedURL.Host, healPath)
	fatalIf(err, "error connecting to %s", parsedURL.Host)
}
