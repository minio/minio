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
	"os"

	"github.com/minio-io/cli"
)

func newDonutConfig(donutName string) (*mcDonutConfig, error) {
	mcDonutConfigData := new(mcDonutConfig)
	mcDonutConfigData.Donuts = make(map[string]donutConfig)
	mcDonutConfigData.Donuts[donutName] = donutConfig{
		Node: make(map[string]nodeConfig),
	}
	mcDonutConfigData.Donuts[donutName].Node["localhost"] = nodeConfig{
		ActiveDisks:   make([]string, 0),
		InactiveDisks: make([]string, 0),
	}
	return mcDonutConfigData, nil
}

// doMakeDonutCmd creates a new donut
func doMakeDonutCmd(c *cli.Context) {
	if !c.Args().Present() {
		log.Fatalln("no args?")
	}
	if len(c.Args()) != 1 {
		log.Fatalln("invalid number of args")
	}
	donutName := c.Args().First()
	if !isValidDonutName(donutName) {
		log.Fatalln("Invalid donutName")
	}
	mcDonutConfigData, err := loadDonutConfig()
	if os.IsNotExist(err) {
		mcDonutConfigData, err = newDonutConfig(donutName)
		if err != nil {
			log.Fatalln(err)
		}
		if err := saveDonutConfig(mcDonutConfigData); err != nil {
			log.Fatalln(err)
		}
		return
	} else if err != nil {
		log.Fatalln(err)
	}
	if _, ok := mcDonutConfigData.Donuts[donutName]; !ok {
		mcDonutConfigData.Donuts[donutName] = donutConfig{
			Node: make(map[string]nodeConfig),
		}
		mcDonutConfigData.Donuts[donutName].Node["localhost"] = nodeConfig{
			ActiveDisks:   make([]string, 0),
			InactiveDisks: make([]string, 0),
		}
		if err := saveDonutConfig(mcDonutConfigData); err != nil {
			log.Fatalln(err)
		}
	} else {
		msg := fmt.Sprintf("donut: %s already exists", donutName)
		log.Println(msg)
	}
}
