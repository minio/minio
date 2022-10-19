/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package panfs

import (
	"context"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
)

func init() {
	const panfsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  path to PANFS mount point

EXAMPLES:
  1. Start minio gateway server for PANFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} /shared/panfsvol

  2. Start minio gateway server for PANFS with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}} /shared/panfsvol
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.PANFSBackendGateway,
		Usage:              "Network-attached storage (PANFS)",
		Action:             panfsGatewayMain,
		CustomHelpTemplate: panfsGatewayTemplate,
		HideHelpCommand:    false,
	})
}

// Handler for 'minio gateway panfs' command line.
func panfsGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.PANFSBackendGateway, 1)
	}

	minio.StartGateway(ctx, &PANFS{ctx.Args().First()})
}

// PANFS implements Gateway.
type PANFS struct {
	path string
}

// Name implements Gateway interface.
func (g *PANFS) Name() string {
	return minio.PANFSBackendGateway
}

// NewGatewayLayer returns panfs gatewaylayer.
func (g *PANFS) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	var err error
	newObject, err := minio.NewPANFSObjectLayer(g.path)
	if err != nil {
		return nil, err
	}
	return &panfsObjects{newObject}, nil
}

// IsListenSupported returns whether listen bucket notification is applicable for this gateway.
func (n *panfsObjects) IsListenSupported() bool {
	return false
}

func (n *panfsObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si, errs := n.ObjectLayer.StorageInfo(ctx)
	si.Backend.GatewayOnline = si.Backend.Type == madmin.FS
	si.Backend.Type = madmin.Gateway
	return si, errs
}

// panfsObjects implements gateway for MinIO and S3 compatible object storage servers.
type panfsObjects struct {
	minio.ObjectLayer
}

func (n *panfsObjects) IsTaggingSupported() bool {
	return true
}
