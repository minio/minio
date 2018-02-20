/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package nas

import (
	"github.com/minio/cli"
	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
)

const (
	nasBackend = "nas"
)

func init() {
	const nasGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  path to NAS mount point.

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of minimum 3 characters in length.
     MINIO_SECRET_KEY: Password or secret key of minimum 8 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  UPDATE:
     MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".

EXAMPLES:
  1. Start minio gateway server for NAS backend.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}} /shared/nasvol
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               nasBackend,
		Usage:              "Network-attached storage (NAS).",
		Action:             nasGatewayMain,
		CustomHelpTemplate: nasGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway nas' command line.
func nasGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	host := ctx.Args().First()
	if host == "" {
		cli.ShowCommandHelpAndExit(ctx, "nas", 1)
	}
	// Validate gateway arguments.
	minio.StartGateway(ctx, &NAS{host})
}

// NAS implements Gateway.
type NAS struct {
	host string
}

// Name implements Gateway interface.
func (g *NAS) Name() string {
	return nasBackend
}

// NewGatewayLayer returns nas gatewaylayer.
func (g *NAS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	var err error
	newObject, err := minio.NewFSObjectLayer(g.host)
	if err != nil {
		return nil, err
	}
	return &nasObjects{newObject.(*minio.FSObjects)}, nil
}

// Production - nas gateway is production ready.
func (g *NAS) Production() bool {
	return true
}

// nasObjects implements gateway for Minio and S3 compatible object storage servers.
type nasObjects struct {
	*minio.FSObjects
}

// IsNotificationSupported returns whether notifications are applicable for this layer.
func (l *nasObjects) IsNotificationSupported() bool {
	return false
}

// GetBucketPolicy will get policy on bucket
func (l *nasObjects) GetBucketPolicy(bucket string) (policy.BucketAccessPolicy, error) {
	return minio.ReadBucketPolicy(bucket, l)
}
