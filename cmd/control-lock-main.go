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
	"net/url"
	"path"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var lockFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "older-than",
		Usage: "List locks older than given time.",
		Value: "24h",
	},
	cli.BoolFlag{
		Name:  "verbose",
		Usage: "Lists more information about locks.",
	},
}

var lockCmd = cli.Command{
	Name:   "lock",
	Usage:  "Prints current lock information.",
	Action: lockControl,
	Flags:  append(lockFlags, globalFlags...),
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}} [list|clear] http://localhost:9000/

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}
EAMPLES:
  1. List all currently active locks from all nodes. Defaults to list locks held longer than 24hrs.
    $ minio control {{.Name}} list http://localhost:9000/

  2. List all currently active locks from all nodes. Request locks from older than 1minute.
    $ minio control {{.Name}} --older-than=1m list http://localhost:9000/
`,
}

// printLockStateVerbose - pretty prints systemLockState, additionally this filters out based on a given duration.
func printLockStateVerbose(lkStateRep map[string]SystemLockState, olderThan time.Duration) {
	console.Println("Duration     Server     LockType     LockAcquired     Status     LockOrigin     Resource")
	for server, lockState := range lkStateRep {
		for _, lockInfo := range lockState.LocksInfoPerObject {
			lockedResource := path.Join(lockInfo.Bucket, lockInfo.Object)
			for _, lockDetails := range lockInfo.LockDetailsOnObject {
				if lockDetails.Duration < olderThan {
					continue
				}
				console.Println(lockDetails.Duration, server,
					lockDetails.LockType, lockDetails.Since,
					lockDetails.Status, lockDetails.LockOrigin,
					lockedResource)
			}
		}
	}
}

// printLockState - pretty prints systemLockState, additionally this filters out based on a given duration.
func printLockState(lkStateRep map[string]SystemLockState, olderThan time.Duration) {
	console.Println("Duration     Server     LockType     Resource")
	for server, lockState := range lkStateRep {
		for _, lockInfo := range lockState.LocksInfoPerObject {
			lockedResource := path.Join(lockInfo.Bucket, lockInfo.Object)
			for _, lockDetails := range lockInfo.LockDetailsOnObject {
				if lockDetails.Duration < olderThan {
					continue
				}
				console.Println(lockDetails.Duration, server,
					lockDetails.LockType, lockedResource)
			}
		}
	}
}

// "minio control lock" entry point.
func lockControl(c *cli.Context) {
	if !c.Args().Present() && len(c.Args()) != 2 {
		cli.ShowCommandHelpAndExit(c, "lock", 1)
	}

	parsedURL, err := url.Parse(c.Args().Get(1))
	fatalIf(err, "Unable to parse URL.")

	// Parse older than string.
	olderThanStr := c.String("older-than")
	olderThan, err := time.ParseDuration(olderThanStr)
	fatalIf(err, "Unable to parse older-than time duration.")

	// Verbose flag.
	verbose := c.Bool("verbose")

	authCfg := &authConfig{
		accessKey:   serverConfig.GetCredential().AccessKeyID,
		secretKey:   serverConfig.GetCredential().SecretAccessKey,
		secureConn:  parsedURL.Scheme == "https",
		address:     parsedURL.Host,
		path:        path.Join(reservedBucket, controlPath),
		loginMethod: "Control.LoginHandler",
	}
	client := newAuthClient(authCfg)

	args := &GenericArgs{
		// This is necessary so that the remotes,
		// don't end up sending requests back and forth.
		Remote: true,
	}

	subCommand := c.Args().Get(0)
	switch subCommand {
	case "list":
		lkStateRep := make(map[string]SystemLockState)
		// Request lock info, fetches from all the nodes in the cluster.
		err = client.Call("Control.LockInfo", args, &lkStateRep)
		fatalIf(err, "Unable to fetch system lockInfo.")
		if !verbose {
			printLockState(lkStateRep, olderThan)
		} else {
			printLockStateVerbose(lkStateRep, olderThan)
		}
	case "clear":
		// TODO. Defaults to clearing all locks.
	default:
		fatalIf(errInvalidArgument, "Unsupported lock control operation %s", c.Args().Get(0))
	}

}
