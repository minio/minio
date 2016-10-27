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
	"errors"
	"fmt"
	"net/url"
	"path"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "To heal objects.",
	Action: healControl,
	Flags:  globalFlags,
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}} http[s]://[access_key[:secret_key]@]server_ip:port/

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}

EXAMPLES:
  1. Heal missing on-disk format across all inconsistent nodes.
     $ minio control {{.Name}} http://localhost:9000

  2. Heals a specific object.
     $ minio control {{.Name}} http://localhost:9000/songs/classical/western/piano.mp3

  3. Heal bucket and all objects in a bucket recursively.
     $ minio control {{.Name}} http://localhost:9000/songs

  4. Heal all objects with a given prefix recursively.
     $ minio control {{.Name}} http://localhost:9000/songs/classical/  
`,
}

// heals backend storage format, useful in restoring `format.json` missing on a
// fresh or corrupted disks.  This call does deep inspection of backend layout
// and applies appropriate `format.json` to the disk.
func healStorageFormat(authClnt *AuthRPCClient) error {
	args := &GenericArgs{}
	reply := &GenericReply{}
	return authClnt.Call("Control.HealFormatHandler", args, reply)
}

// lists all objects which needs to be healed, this is a precursor helper function called before
// calling actual healing operation. Returns a maximum of 1000 objects that needs healing at a time.
// Marker indicates the next entry point where the listing will start.
func listObjectsHeal(authClnt *AuthRPCClient, bucketName, prefixName, markerName string) (*HealListReply, error) {
	args := &HealListArgs{
		Bucket:    bucketName,
		Prefix:    prefixName,
		Marker:    markerName,
		Delimiter: "",
		MaxKeys:   1000,
	}
	reply := &HealListReply{}
	err := authClnt.Call("Control.ListObjectsHealHandler", args, reply)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// Internal custom struct encapsulates pretty msg to be printed by the caller.
type healMsg struct {
	Msg string
	Err error
}

// Prettifies heal results and returns them over a channel, caller reads from this channel and prints.
func prettyHealResults(healedObjects []ObjectInfo, healReply *HealObjectReply) <-chan healMsg {
	var msgCh = make(chan healMsg)

	// Starts writing to message channel for the list of results sent back
	// by a previous healing operation.
	go func(msgCh chan<- healMsg) {
		defer close(msgCh)
		// Go through all the results and validate if we have success or failure.
		for i, healStr := range healReply.Results {
			objPath := path.Join(healedObjects[i].Bucket, healedObjects[i].Name)
			// TODO: We need to still print heal error cause.
			if healStr != "" {
				msgCh <- healMsg{
					Msg: fmt.Sprintf("%s  %s", colorRed("FAILED"), objPath),
					Err: errors.New(healStr),
				}
				continue
			}
			msgCh <- healMsg{
				Msg: fmt.Sprintf("%s  %s", colorGreen("SUCCESS"), objPath),
			}
		}
	}(msgCh)

	// Return ..
	return msgCh
}

var scanBar = scanBarFactory()

// Heals all the objects under a given bucket, optionally you can specify an
// object prefix to heal objects under this prefix.
func healObjects(authClnt *AuthRPCClient, bucketName, prefixName string) error {
	if authClnt == nil || bucketName == "" {
		return errInvalidArgument
	}
	// Save marker for the next request.
	var markerName string
	for {
		healListReply, err := listObjectsHeal(authClnt, bucketName, prefixName, markerName)
		if err != nil {
			return err
		}

		// Attempt to heal only if there are any objects to heal.
		if len(healListReply.Objects) > 0 {
			healArgs := &HealObjectArgs{
				Bucket:  bucketName,
				Objects: healListReply.Objects,
			}

			healReply := &HealObjectReply{}
			err = authClnt.Call("Control.HealObjectsHandler", healArgs, healReply)
			if err != nil {
				return err
			}

			// Pretty print all the heal results.
			for msg := range prettyHealResults(healArgs.Objects, healReply) {
				if msg.Err != nil {
					// TODO we need to print the error cause as well.
					scanBar(msg.Msg)
					continue
				}
				// Success.
				scanBar(msg.Msg)
			}
		}

		// End of listing objects for healing.
		if !healListReply.IsTruncated {
			break
		}

		// Set the marker to list the next set of keys.
		markerName = healListReply.NextMarker

	}
	return nil
}

// Heals your bucket for any missing entries.
func healBucket(authClnt *AuthRPCClient, bucketName string) error {
	if authClnt == nil || bucketName == "" {
		return errInvalidArgument
	}
	return authClnt.Call("Control.HealBucketHandler", &HealBucketArgs{
		Bucket: bucketName,
	}, &GenericReply{})
}

// Entry point for minio control heal command.
func healControl(ctx *cli.Context) {
	if ctx.Args().Present() && len(ctx.Args()) != 1 {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1)
	}

	parsedURL, err := url.Parse(ctx.Args().Get(0))
	fatalIf(err, "Unable to parse URL %s", ctx.Args().Get(0))

	accessKey := serverConfig.GetCredential().AccessKeyID
	secretKey := serverConfig.GetCredential().SecretAccessKey
	// Username and password specified in URL will override prior configuration
	if parsedURL.User != nil {
		accessKey = parsedURL.User.Username()
		if key, set := parsedURL.User.Password(); set {
			secretKey = key
		}
	}

	authCfg := &authConfig{
		accessKey:   accessKey,
		secretKey:   secretKey,
		secureConn:  parsedURL.Scheme == "https",
		address:     parsedURL.Host,
		path:        path.Join(reservedBucket, controlPath),
		loginMethod: "Control.LoginHandler",
	}

	client := newAuthClient(authCfg)
	if parsedURL.Path == "/" || parsedURL.Path == "" {
		err = healStorageFormat(client)
		fatalIf(err, "Unable to heal disk metadata.")
		return
	}
	bucketName, prefixName := urlPathSplit(parsedURL.Path)
	// Heal the bucket.
	err = healBucket(client, bucketName)
	fatalIf(err, "Unable to heal bucket %s", bucketName)
	// Heal all the objects.
	err = healObjects(client, bucketName, prefixName)
	fatalIf(err, "Unable to heal objects on bucket %s at prefix %s", bucketName, prefixName)
	console.Println()
}
