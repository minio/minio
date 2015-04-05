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
	"fmt"
	"log"

	"github.com/minio-io/cli"
)

func doDetachDiskCmd(c *cli.Context) {
	if !c.Args().Present() {
		log.Fatalln("no args?")
	}
	disks := c.Args()
	mcDonutConfigData, err := loadDonutConfig()
	if err != nil {
		log.Fatalln(err.Error())
	}
	donutName := c.String("name")
	if donutName == "" {
		log.Fatalln("Invalid --donut <name> is needed for attach")
	}
	if _, ok := mcDonutConfigData.Donuts[donutName]; !ok {
		msg := fmt.Sprintf("Requested donut name <%s> does not exist, please use ``mc donut make`` first", donutName)
		log.Fatalln(msg)
	}
	if _, ok := mcDonutConfigData.Donuts[donutName].Node["localhost"]; !ok {
		msg := fmt.Sprintf("Corrupted donut config, please consult donut experts")
		log.Fatalln(msg)
	}

	inactiveDisks := mcDonutConfigData.Donuts[donutName].Node["localhost"].InactiveDisks
	activeDisks := mcDonutConfigData.Donuts[donutName].Node["localhost"].ActiveDisks
	for _, disk := range disks {
		if isStringInSlice(activeDisks, disk) {
			activeDisks = deleteFromSlice(activeDisks, disk)
			inactiveDisks = appendUniq(inactiveDisks, disk)
		} else {
			msg := fmt.Sprintf("Cannot detach disk: <%s>, not part of donut <%s>", disk, donutName)
			log.Println(msg)
		}
	}
	mcDonutConfigData.Donuts[donutName].Node["localhost"] = nodeConfig{
		ActiveDisks:   activeDisks,
		InactiveDisks: inactiveDisks,
	}
	if err := saveDonutConfig(mcDonutConfigData); err != nil {
		log.Fatalln(err.Error())
	}
}
