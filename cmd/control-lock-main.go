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

package cmd

import (
	"encoding/json"
	"fmt"
	"net/rpc"
	"net/url"
	"path"

	"github.com/minio/cli"
)

var lockCmd = cli.Command{
	Name:   "lock",
	Usage:  "info about the locks in the node.",
	Action: lockControl,
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}} http://localhost:9000/

EAMPLES:
  1. Get all the info about the blocked/held locks in the node:
    $ minio control lock http://localhost:9000/
`,
}

// "minio control lock" entry point.
func lockControl(c *cli.Context) {
	if len(c.Args()) != 1 {
		cli.ShowCommandHelpAndExit(c, "lock", 1)
	}

	parsedURL, err := url.ParseRequestURI(c.Args()[0])
	fatalIf(err, "Unable to parse URL")

	client, err := rpc.DialHTTPPath("tcp", parsedURL.Host, path.Join(reservedBucket, controlPath))
	fatalIf(err, "Unable to connect to %s", parsedURL.Host)

	args := &struct{}{}
	reply := &SystemLockState{}
	err = client.Call("Control.LockInfo", args, reply)
	fatalIf(err, "RPC Control.LockInfo call failed")
	b, err := json.MarshalIndent(*reply, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}
