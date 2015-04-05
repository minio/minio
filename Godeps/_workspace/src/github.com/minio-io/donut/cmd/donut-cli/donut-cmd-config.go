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
	"os/user"
	"path"

	"encoding/json"
	"io/ioutil"
)

const (
	donutConfigDir      = ".minio/donut"
	donutConfigFilename = "donuts.json"
)

type nodeConfig struct {
	ActiveDisks   []string
	InactiveDisks []string
}

type donutConfig struct {
	Node map[string]nodeConfig
}

type mcDonutConfig struct {
	Donuts map[string]donutConfig
}

func getDonutConfigDir() string {
	u, err := user.Current()
	if err != nil {
		msg := fmt.Sprintf("Unable to obtain user's home directory. \nError: %s", err)
		log.Fatalln(msg)
	}

	return path.Join(u.HomeDir, donutConfigDir)
}

func getDonutConfigFilename() string {
	return path.Join(getDonutConfigDir(), "donuts.json")
}

// saveDonutConfig writes configuration data in json format to donut config file.
func saveDonutConfig(donutConfigData *mcDonutConfig) error {
	jsonConfig, err := json.MarshalIndent(donutConfigData, "", "\t")
	if err != nil {
		return err
	}

	err = os.MkdirAll(getDonutConfigDir(), 0755)
	if !os.IsExist(err) && err != nil {
		return err
	}

	configFile, err := os.OpenFile(getDonutConfigFilename(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer configFile.Close()

	_, err = configFile.Write(jsonConfig)
	if err != nil {
		return err
	}
	return nil
}

func loadDonutConfig() (donutConfigData *mcDonutConfig, err error) {
	configFile := getDonutConfigFilename()
	_, err = os.Stat(configFile)
	if err != nil {
		return nil, err
	}

	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(configBytes, &donutConfigData)
	if err != nil {
		return nil, err
	}

	return donutConfigData, nil
}
