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
	"fmt"
	"net/rpc"
	"net/url"
	"path"
	"strings"

	"github.com/minio/cli"
)

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "To heal objects.",
	Action: healControl,
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}}

EAMPLES:
  1. Heal an object.
     $ minio control {{.Name}} http://localhost:9000/songs/classical/western/piano.mp3

  2. Heal all objects in a bucket recursively.
     $ minio control {{.Name}}  http://localhost:9000/songs

  3. Heall all objects with a given prefix recursively.
     $ minio control {{.Name}} http://localhost:9000/songs/classical/
`,
}

// "minio control heal" entry point.
func healControl(ctx *cli.Context) {
	// Parse bucket and object from url.URL.Path
	parseBucketObject := func(path string) (bucketName string, objectName string) {
		splits := strings.SplitN(path, string(slashSeparator), 3)
		switch len(splits) {
		case 0, 1:
			bucketName = ""
			objectName = ""
		case 2:
			bucketName = splits[1]
			objectName = ""
		case 3:
			bucketName = splits[1]
			objectName = splits[2]

		}
		return bucketName, objectName
	}

	if len(ctx.Args()) != 1 {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1)
	}

	parsedURL, err := url.Parse(ctx.Args()[0])
	fatalIf(err, "Unable to parse URL")

	bucketName, objectName := parseBucketObject(parsedURL.Path)
	if bucketName == "" {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1)
	}

	client, err := rpc.DialHTTPPath("tcp", parsedURL.Host, path.Join(reservedBucket, controlPath))
	fatalIf(err, "Unable to connect to %s", parsedURL.Host)

	// If object does not have trailing "/" then it's an object, hence heal it.
	if objectName != "" && !strings.HasSuffix(objectName, slashSeparator) {
		fmt.Printf("Healing : /%s/%s", bucketName, objectName)
		args := &HealObjectArgs{bucketName, objectName}
		reply := &HealObjectReply{}
		err = client.Call("Control.HealObject", args, reply)
		fatalIf(err, "RPC Control.HealObject call failed")
		fmt.Println()
		return
	}

	// Recursively list and heal the objects.
	prefix := objectName
	marker := ""
	for {
		args := HealListArgs{bucketName, prefix, marker, "", 1000}
		reply := &HealListReply{}
		err = client.Call("Control.ListObjectsHeal", args, reply)
		fatalIf(err, "RPC Heal.ListObjects call failed")

		// Heal the objects returned in the ListObjects reply.
		for _, obj := range reply.Objects {
			fmt.Printf("Healing : /%s/%s", bucketName, obj)
			reply := &HealObjectReply{}
			err = client.Call("Control.HealObject", HealObjectArgs{bucketName, obj}, reply)
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
