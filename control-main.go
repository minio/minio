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
	"strings"

	"net/rpc"
	"net/url"

	"github.com/minio/cli"
)

var controlCmd = cli.Command{
	Name:   "control",
	Usage:  "minio control commands.",
	Action: mainControl,
	Subcommands: []cli.Command{
		healCmd,
	},
}

func mainControl(c *cli.Context) {
	cli.ShowCommandHelp(c, "")
}

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "heal objects",
	Action: healControl,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Usage}}

USAGE:
  minio {{.Name}} [OPTIONS] PATH [PATH...]

EAMPLES:
  1. Heal an object.
     $ minio control heal http://localhost:9000/songs/classical/western/piano.mp3

  2. Heal all objects in a bucket recursively.
     $ minio control heal http://localhost:9000/songs

  3. Heall all objects with a given prefix recursively.
     $ minio control heal http://localhost:9000/songs/classical/
`,
}

func healControl(c *cli.Context) {
	if len(c.Args()) != 1 {
		cli.ShowCommandHelpAndExit(c, "heal", 1)
	}
	var bucket string
	var object string
	parsedURL, err := url.ParseRequestURI(c.Args()[0])
	fatalIf(err, "Unable to parse URL")

	path := parsedURL.Path
	if path == "" || path == slashSeparator {
		cli.ShowCommandHelpAndExit(c, "heal", 1)
	}

	path = path[1:]
	slashIndex := strings.Index(path, slashSeparator)
	if slashIndex == -1 {
		bucket = path
	} else {
		bucket = path[:slashIndex]
		object = path[slashIndex+1:]
	}
	client, err := rpc.DialHTTPPath("tcp", parsedURL.Host, healPath)
	fatalIf(err, "error connecting to %s", parsedURL.Host)

	if object != "" && !strings.HasSuffix(object, slashSeparator) {
		args := &HealObjectArgs{bucket, object}
		reply := &HealObjectReply{}
		err := client.Call("Heal.HealObject", args, reply)
		fatalIf(err, "RPC Heal.HealObject call failed")
		return
	}
	prefix := object
	marker := ""
	for {
		args := HealListArgs{bucket, prefix, marker, slashSeparator, 1000}
		reply := &HealListReply{}
		err := client.Call("Heal.ListObjects", args, reply)
		fatalIf(err, "RPC Heal.ListObjects call failed")
		for _, obj := range reply.Objects {
			reply := &HealObjectReply{}
			err := client.Call("Heal.HealObject", HealObjectArgs{bucket, obj}, reply)
			fatalIf(err, "RPC Heal.HealObject call failed")
		}
		if !reply.IsTruncated {
			break
		}
		marker = reply.NextMarker
	}
}
