// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zip"
	"github.com/minio/cli"
)

var fmtGenFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "parity",
		Usage: "specify erasure code parity",
	},
	cli.StringFlag{
		Name:  "deployment-id",
		Usage: "deployment-id of the MinIO cluster for which format.json is needed",
	},
	cli.StringFlag{
		Name:   "address",
		Value:  ":" + GlobalMinioDefaultPort,
		Usage:  "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
		EnvVar: "MINIO_ADDRESS",
	},
}

var fmtGenCmd = cli.Command{
	Name:   "fmt-gen",
	Usage:  "Generate format.json files for an erasure server pool",
	Flags:  append(fmtGenFlags, GlobalFlags...),
	Action: fmtGenMain,
	Hidden: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
  1. Generate format.json.zip containing format.json files for all drives in a distributed MinIO server pool of 32 nodes with 32 drives each.
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export{1...32}

`,
}

func fmtGenMain(ctxt *cli.Context) {
	deploymentID := ctxt.String("deployment-id")
	err := buildServerCtxt(ctxt, &globalServerCtxt)
	if err != nil {
		log.Fatalln(err)
	}
	handleCommonArgs(globalServerCtxt)
	pools, _, err := createServerEndpoints(globalMinioAddr, globalServerCtxt.Layout.pools, globalServerCtxt.Layout.legacy)
	if err != nil {
		log.Fatalln(err)
	}

	zipFile, err := os.Create("format.json.zip")
	if err != nil {
		log.Fatalf("failed to create format.json.zip: %v", err)
	}
	defer zipFile.Close()
	fmtZipW := zip.NewWriter(zipFile)
	defer fmtZipW.Close()
	for _, pool := range pools { // for each pool
		setCount, setDriveCount := pool.SetCount, pool.DrivesPerSet
		format := newFormatErasureV3(setCount, setDriveCount)
		format.ID = deploymentID
		for i := range setCount { // for each erasure set
			for j := range setDriveCount {
				newFormat := format.Clone()
				newFormat.Erasure.This = format.Erasure.Sets[i][j]
				if deploymentID != "" {
					newFormat.ID = deploymentID
				}
				drive := pool.Endpoints[i*setDriveCount+j]
				fmtBytes, err := json.Marshal(newFormat)
				if err != nil {
					//nolint:gocritic
					log.Fatalf("failed to marshal format.json for %s: %v", drive.String(), err)
				}
				fmtJSON := filepath.Join(drive.Host, drive.Path, minioMetaBucket, "format.json")
				embedFileInZip(fmtZipW, fmtJSON, fmtBytes, 0o600)
			}
		}
	}
}
