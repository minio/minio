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
	"fmt"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/configuration"
)

func main() {
	var err error

	serverConfig := &configuration.ServerConfig{}
	// Load configuration data from disk to memory
	fmt.Println("************************")
	fmt.Println("***** Calling Load *****")
	fmt.Println("************************")
	if err = serverConfig.Load(); err != nil {
		logger.FatalIf(err, "Failed to load configuration data: ")
	}

	fmt.Println("******************************")
	fmt.Println("***** Calling Gethandler *****")
	fmt.Println("******************************")
	kv, err := serverConfig.GetHandler()
	fmt.Printf("GetHandler >\n%+v\n\n", kv)

}
