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
	"net/url"
	"path"
	"strings"

	"github.com/minio/cli"
)

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "To heal objects.",
	Action: healControl,
	Flags:  globalFlags,
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}}

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}

EXAMPLES:
  1. Heal an object.
     $ minio control {{.Name}} http://localhost:9000/songs/classical/western/piano.mp3

  2. Heal all objects in a bucket recursively.
     $ minio control {{.Name}}  http://localhost:9000/songs

  3. Heall all objects with a given prefix recursively.
     $ minio control {{.Name}} http://localhost:9000/songs/classical/
`,
}

func checkHealControlSyntax(ctx *cli.Context) {
	if len(ctx.Args()) != 1 {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1)
	}
}

// "minio control heal" entry point.
func healControl(ctx *cli.Context) {

	checkHealControlSyntax(ctx)

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

	parsedURL, err := url.Parse(ctx.Args()[0])
	fatalIf(err, "Unable to parse URL")

	authCfg := &authConfig{
		accessKey:   serverConfig.GetCredential().AccessKeyID,
		secretKey:   serverConfig.GetCredential().SecretAccessKey,
		address:     parsedURL.Host,
		path:        path.Join(reservedBucket, controlPath),
		loginMethod: "Controller.LoginHandler",
	}
	client := newAuthClient(authCfg)

	// Always try to fix disk metadata
	fmt.Print("Checking and healing disk metadata..")
	args := &GenericArgs{}
	reply := &GenericReply{}
	err = client.Call("Controller.HealDiskMetadataHandler", args, reply)
	fatalIf(err, "Unable to heal disk metadata.")
	fmt.Println(" ok")

	bucketName, objectName := parseBucketObject(parsedURL.Path)
	if bucketName == "" {
		return
	}

	// If object does not have trailing "/" then it's an object, hence heal it.
	if objectName != "" && !strings.HasSuffix(objectName, slashSeparator) {
		fmt.Printf("Healing : /%s/%s\n", bucketName, objectName)
		args := &HealObjectArgs{Bucket: bucketName, Object: objectName}
		reply := &HealObjectReply{}
		err = client.Call("Controller.HealObjectHandler", args, reply)
		errorIf(err, "Healing object %s failed.", objectName)
		return
	}

	// Recursively list and heal the objects.
	prefix := objectName
	marker := ""
	for {
		args := &HealListArgs{
			Bucket:    bucketName,
			Prefix:    prefix,
			Marker:    marker,
			Delimiter: "",
			MaxKeys:   1000,
		}
		reply := &HealListReply{}
		err = client.Call("Controller.ListObjectsHealHandler", args, reply)
		fatalIf(err, "Unable to list objects for healing.")

		// Heal the objects returned in the ListObjects reply.
		for _, obj := range reply.Objects {
			fmt.Printf("Healing : /%s/%s\n", bucketName, obj)
			reply := &GenericReply{}
			healArgs := &HealObjectArgs{Bucket: bucketName, Object: obj}
			err = client.Call("Controller.HealObjectHandler", healArgs, reply)
			errorIf(err, "Healing object %s failed.", obj)
		}

		if !reply.IsTruncated {
			// End of listing.
			break
		}

		// Set the marker to list the next set of keys.
		marker = reply.NextMarker
	}
}
