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

	"github.com/minio/cli"
)

var shutdownFlags = []cli.Flag{
	cli.BoolFlag{
		Name:  "restart",
		Usage: "Restart the server.",
	},
}

var shutdownCmd = cli.Command{
	Name:   "shutdown",
	Usage:  "Shutdown or restart the server.",
	Action: shutdownControl,
	Flags:  append(shutdownFlags, globalFlags...),
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}} http://localhost:9000/

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}

EXAMPLES:
  1. Shutdown the server:
    $ minio control shutdown http://localhost:9000/

  2. Reboot the server:
    $ minio control shutdown --restart http://localhost:9000/
`,
}

// "minio control shutdown" entry point.
func shutdownControl(c *cli.Context) {
	if len(c.Args()) != 1 {
		cli.ShowCommandHelpAndExit(c, "shutdown", 1)
	}

	parsedURL, err := url.Parse(c.Args()[0])
	fatalIf(err, "Unable to parse URL.")

	authCfg := &authConfig{
		accessKey:   serverConfig.GetCredential().AccessKeyID,
		secretKey:   serverConfig.GetCredential().SecretAccessKey,
		address:     parsedURL.Host,
		path:        path.Join(reservedBucket, controlPath),
		loginMethod: "Controller.LoginHandler",
	}
	client := newAuthClient(authCfg)

	args := &ShutdownArgs{Restart: c.Bool("restart")}
	err = client.Call("Controller.ShutdownHandler", args, &GenericReply{})
	errorIf(err, "Shutting down Minio server at %s failed.", parsedURL.Host)
}
