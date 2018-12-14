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
 *
 */

package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/configuration"
)

func np(kv map[string]string) {
	b, err := json.MarshalIndent(kv, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(string(b))
}

func main() {
	var err error
	var keys []string
	var kv map[string]string
	serverConfig := &configuration.ServerConfig{RWMutex: &sync.RWMutex{}}

	fmt.Println()
	fmt.Println("*** Get full configuration")
	keys = []string{}
	if kv, err = serverConfig.GetHandler(keys); err != nil {
		logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	}
	// fmt.Printf("kv>\n%+v\n\n", kv)
	fmt.Println()
	np(kv)

	// fmt.Println("Set 'version'='31'")
	// if err := serverConfig.SetHandler("version", "31"); err != nil {
	// 	fmt.Printf("ERROR: %v\n\n", err)
	// 	// os.Exit(1)
	// }
	// fmt.Println("Success! Yeay!")

	// fmt.Println("Get 'version' value")
	// keys = []string{"version"}
	// fmt.Println("Calling GetHandler 'version'")
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	fmt.Printf("ERROR: %v\n\n", err)
	// 	// os.Exit(1)
	// }
	// fmt.Printf("'version': %v\n", kv["version"])
	// np(kv)

	// fmt.Println("Set a config parameter, 'region'='minio-region'")
	// if err = serverConfig.SetHandler("region", "minio-region"); err != nil {
	// 	logger.FatalIf(err, "Failed to load configuration data: ")
	// 	fmt.Println("Failed to load configuration data:", err)
	// }
	// fmt.Println()

	// fmt.Println("Get config parameter, 'region'")
	// keys = []string{"region"}
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	// }
	// fmt.Printf("kv>\n%+v\n\n", kv)
	// np(kv)

	// fmt.Println()
	// fmt.Println("*** Get 'region', 'version', 'worm' and 'notify.redis' config info")
	// keys = []string{"region", "version", "worm", "notify.redis"}
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	// }
	// fmt.Printf("kv>\n%+v\n\n", kv)
	// np(kv)

	// fmt.Println()
	// fmt.Println("*** Get full configuration")
	// keys = []string{}
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	// }
	// fmt.Printf("kv>\n%+v\n\n", kv)
	// fmt.Println()
	// np(kv)

}
