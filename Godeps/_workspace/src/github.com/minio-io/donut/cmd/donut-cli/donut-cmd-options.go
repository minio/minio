/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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
	"github.com/minio-io/cli"
)

var makeDonutCmd = cli.Command{
	Name:        "make",
	Usage:       "make donut",
	Description: "Make a new donut",
	Action:      doMakeDonutCmd,
}

var listDonutCmd = cli.Command{
	Name:        "list",
	Usage:       "list donuts",
	Description: "list all donuts locally or remote",
	Action:      doListDonutCmd,
}

var attachDiskCmd = cli.Command{
	Name:        "attach",
	Usage:       "attach disk",
	Description: "Attach disk to an existing donut",
	Action:      doAttachDiskCmd,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "name",
			Usage: "Donut name",
		},
	},
}

var detachDiskCmd = cli.Command{
	Name:        "detach",
	Usage:       "detach disk",
	Description: "Detach disk from an existing donut",
	Action:      doDetachDiskCmd,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "name",
			Usage: "Donut name",
		},
	},
}

var healDonutCmd = cli.Command{
	Name:        "heal",
	Usage:       "heal donut",
	Description: "Heal donut with any errors",
	Action:      doHealDonutCmd,
}

var rebalanceDonutCmd = cli.Command{
	Name:        "rebalance",
	Usage:       "rebalance donut",
	Description: "Rebalance data on donut after adding disks",
	Action:      doRebalanceDonutCmd,
}

var infoDonutCmd = cli.Command{
	Name:        "info",
	Usage:       "information about donut",
	Description: "Pretty print donut information",
	Action:      doInfoDonutCmd,
}

var donutOptions = []cli.Command{
	makeDonutCmd,
	listDonutCmd,
	attachDiskCmd,
	detachDiskCmd,
	healDonutCmd,
	rebalanceDonutCmd,
	infoDonutCmd,
}

func doHealDonutCmd(c *cli.Context) {
}
