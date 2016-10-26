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
	"github.com/minio/mc/pkg/console"
)

var serviceCmd = cli.Command{
	Name:   "service",
	Usage:  "Service command line to manage Minio server.",
	Action: serviceControl,
	Flags:  globalFlags,
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}} [status|restart|stop] URL

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}
EXAMPLES:
  1. Prints current status information of the cluster.
    $ minio control service status http://10.1.10.92:9000/

  2. Restarts the url and all the servers in the cluster.
    $ minio control service restart http://localhost:9000/

  3. Shuts down the url and all the servers in the cluster.
    $ minio control service stop http://localhost:9000/
`,
}

// "minio control service" entry point.
func serviceControl(c *cli.Context) {
	if !c.Args().Present() && len(c.Args()) != 2 {
		cli.ShowCommandHelpAndExit(c, "service", 1)
	}

	var signal serviceSignal
	switch c.Args().Get(0) {
	case "status":
		signal = serviceStatus
	case "restart":
		signal = serviceRestart
	case "stop":
		signal = serviceStop
	default:
		fatalIf(errInvalidArgument, "Unrecognized service %s", c.Args().Get(0))
	}

	parsedURL, err := url.Parse(c.Args().Get(1))
	fatalIf(err, "Unable to parse URL %s", c.Args().Get(1))

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

	args := &ServiceArgs{
		Signal: signal,
	}
	// This is necessary so that the remotes,
	// don't end up sending requests back and forth.
	args.Remote = true
	reply := &ServiceReply{}
	err = client.Call("Control.ServiceHandler", args, reply)
	fatalIf(err, "Service command %s failed for %s", c.Args().Get(0), parsedURL.Host)
	if signal == serviceStatus {
		console.Println(getStorageInfoMsg(reply.StorageInfo))
	}
}
