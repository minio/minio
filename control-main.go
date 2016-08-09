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
	"strings"

	"net/rpc"
	"net/url"

	"github.com/minio/cli"
)

// "minio control" command.
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
	Usage:  "To heal objects.",
	Action: healControl,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Usage}}

USAGE:
  minio {{.Name}} [OPTIONS] URL

EAMPLES:
  1. Heal an object.
     $ minio control heal http://localhost:9000/songs/classical/western/piano.mp3

  2. Heal all objects in a bucket recursively.
     $ minio control heal http://localhost:9000/songs

  3. Heall all objects with a given prefix recursively.
     $ minio control heal http://localhost:9000/songs/classical/
`,
}

// "minio control heal" entry point.
func healControl(c *cli.Context) {
	// Parse bucket and object from url.URL.Path
	parseBucketObject := func(path string) (bucket string, object string) {
		path = path[1:]
		slashIndex := strings.Index(path, slashSeparator)
		if slashIndex == -1 {
			bucket = path
		} else {
			bucket = path[:slashIndex]
			object = path[slashIndex+1:]
		}
		return bucket, object
	}

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

	bucket, object = parseBucketObject(path)

	client, err := rpc.DialHTTPPath("tcp", parsedURL.Host, healPath)
	fatalIf(err, "error connecting to %s", parsedURL.Host)

	// If object does not have trailing "/" then it's an object, hence heal it.
	if object != "" && !strings.HasSuffix(object, slashSeparator) {
		fmt.Printf("Healing : /%s/%s", bucket, object)
		args := &HealObjectArgs{bucket, object}
		reply := &HealObjectReply{}
		err = client.Call("Heal.HealObject", args, reply)
		fatalIf(err, "RPC Heal.HealObject call failed")
		fmt.Println()
		return
	}

	// Recursively list and heal the objects.
	prefix := object
	marker := ""
	for {
		args := HealListArgs{bucket, prefix, marker, "", 1000}
		reply := &HealListReply{}
		err = client.Call("Heal.ListObjects", args, reply)
		fatalIf(err, "RPC Heal.ListObjects call failed")

		// Heal the objects returned in the ListObjects reply.
		for _, obj := range reply.Objects {
			fmt.Printf("Healing : /%s/%s", bucket, obj)
			reply := &HealObjectReply{}
			err = client.Call("Heal.HealObject", HealObjectArgs{bucket, obj}, reply)
			fatalIf(err, "RPC Heal.HealObject call failed")
			fmt.Println()
		}
		if !reply.IsTruncated {
			// End of listing.
			break
		}
		marker = reply.NextMarker
	}
}
