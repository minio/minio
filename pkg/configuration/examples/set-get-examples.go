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
	fmt.Println()

	fmt.Println("*******************************************")
	fmt.Println("GET full configuration")
	fmt.Println("*******************************************")
	keys = []string{}
	if kv, err = serverConfig.GetHandler(keys); err != nil {
		logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	}
	// fmt.Printf("kv>\n%+v\n\n", kv)
	fmt.Println()
	np(kv)

	fmt.Println()
	fmt.Println()

	// fmt.Println("*******************************************")
	// fmt.Println("SET 'worm'='12'")
	// fmt.Println("*******************************************")
	// if err := serverConfig.SetHandler("worm", "12"); err != nil {
	// 	fmt.Printf("ERROR setting 'worm' = '12': %v\n\n", err)
	// 	// os.Exit(1)
	// } else {
	// 	fmt.Println("Success! Yeay!")
	// }

	// fmt.Println()
	// fmt.Println()

	fmt.Println("*******************************************")
	fmt.Println("GET 'wox' value")
	fmt.Println("*******************************************")
	keys = []string{"wox"}
	if kv, err = serverConfig.GetHandler(keys); err != nil {
		fmt.Printf("ERROR: %v\n\n", err)
		// os.Exit(1)
	}
	fmt.Printf("'wox': %v\n", kv["wox"])
	np(kv)

	fmt.Println()
	fmt.Println()

	// fmt.Println("*******************************************")
	// fmt.Println("SET a config parameter, 'log.http.TARGET3.anonymous'='false'")
	// fmt.Println("*******************************************")
	// if err = serverConfig.SetHandler("log.http.TARGET3.anonymous", "false"); err != nil {
	// 	logger.FatalIf(err, "Failed to load configuration data: ")
	// 	fmt.Println("Failed to load configuration data:", err)
	// } else {
	// 	fmt.Println("Success! Yeay!")
	// }

	// fmt.Println()
	// fmt.Println()

	// fmt.Println("*******************************************")
	// fmt.Println("SET a config parameter, 'notify.kafka.1123'='true'")
	// fmt.Println("*******************************************")
	// if err = serverConfig.SetHandler("notify.kafka.1123", "true"); err != nil {
	// 	logger.FatalIf(err, "Failed to load configuration data: ")
	// 	fmt.Println("Failed to load configuration data:", err)
	// } else {
	// 	fmt.Println("Success! Yeay!")
	// }

	// fmt.Println()
	// fmt.Println()

	// fmt.Println("*******************************************")
	// fmt.Println("GET config parameter, 'region'")
	// fmt.Println("*******************************************")
	// keys = []string{"region"}
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	// }
	// // fmt.Printf("kv>\n%+v\n\n", kv)
	// fmt.Println()
	// np(kv)

	// fmt.Println()
	// fmt.Println()

	// fmt.Println("*******************************************")
	// fmt.Println("GET 'region', 'version', 'worm' and 'log' config info")
	// fmt.Println("*******************************************")
	// fmt.Println()
	// keys = []string{"region", "version", "worm", "log"}
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	// }
	// // fmt.Printf("kv>\n%+v\n\n", kv)
	// fmt.Println()
	// np(kv)

	// fmt.Println()
	// fmt.Println()

	// fmt.Println("*******************************************")
	// fmt.Println("GET full configuration")
	// fmt.Println("*******************************************")
	// keys = []string{}
	// if kv, err = serverConfig.GetHandler(keys); err != nil {
	// 	logger.FatalIf(err, "Failed to get region and version configuration parameters."+err.Error())
	// }
	// // fmt.Printf("kv>\n%+v\n\n", kv)
	// fmt.Println()
	// np(kv)

	// fmt.Println()
	// fmt.Println()

}
