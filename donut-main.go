/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"os"
	"path/filepath"

	"github.com/minio/minio/internal/github.com/minio/cli"
	"github.com/minio/minio/pkg/donut"
)

var (
	donutSubCommands = []cli.Command{
		{
			Name:        "make",
			Description: "make a donut",
			Action:      makeDonutMain,
			CustomHelpTemplate: `NAME:
  minio donut {{.Name}} - {{.Description}}

USAGE:
  minio donut {{.Name}} DONUTNAME [DISKS...]

EXAMPLES:
  1. Make a donut with 4 exports
      $ minio donut {{.Name}} mongodb-backup /mnt/export1 /mnt/export2 /mnt/export3 /mnt/export4

  2. Make a donut with 16 exports
      $ minio donut {{.Name}} operational-data /mnt/export1 /mnt/export2 /mnt/export3 /mnt/export4 /mnt/export5 \
       /mnt/export6 /mnt/export7 /mnt/export8 /mnt/export9 /mnt/export10 /mnt/export11 \
       /mnt/export12 /mnt/export13 /mnt/export14 /mnt/export15 /mnt/export16
`,
		},
	}

	donutCmd = cli.Command{
		Name:        "donut",
		Usage:       "Create and manage a donut configuration",
		Subcommands: donutSubCommands,
	}
)

func makeDonutMain(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "make", 1)
	}
	donutName := c.Args().First()
	if c.Args().First() != "" {
		if !donut.IsValidDonut(donutName) {
			Fatalf("Invalid donutname %s\n", donutName)
		}
	}
	var disks []string
	for _, disk := range c.Args().Tail() {
		if _, err := isUsable(disk); err != nil {
			Fatalln(err)
		}
		disks = append(disks, disk)
	}
	for _, disk := range disks {
		if err := os.MkdirAll(filepath.Join(disk, donutName), 0700); err != nil {
			Fatalln(err)
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		Fatalln(err)
	}
	donutConfig := &donut.Config{}
	donutConfig.Version = "0.0.1"
	donutConfig.DonutName = donutName
	donutConfig.NodeDiskMap = make(map[string][]string)
	// keep it in exact order as it was specified, do not try to sort disks
	donutConfig.NodeDiskMap[hostname] = disks
	// default cache is unlimited
	donutConfig.MaxSize = 512000000

	if err := donut.SaveConfig(donutConfig); err != nil {
		Fatalln(err)
	}

	Infoln("Success!")
}
