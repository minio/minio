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
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/server"
	"github.com/minio-io/minio/pkg/utils/log"
)

func getStorageType(input string) server.StorageType {
	switch {
	case input == "file":
		return server.File
	case input == "memory":
		return server.Memory
	default:
		{
			log.Println("Unknown storage type:", input)
			log.Println("Choosing default storage type as 'file'..")
			return server.File
		}
	}
}

func runCmd(c *cli.Context) {
	storageTypeStr := c.String("storage-type")
	domain := c.String("domain")
	apiaddress := c.String("api-address")
	webaddress := c.String("web-address")
	certFile := c.String("cert")
	keyFile := c.String("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		log.Fatal("Both certificate and key must be provided to enable https")
	}
	tls := (certFile != "" && keyFile != "")
	storageType := getStorageType(storageTypeStr)
	var serverConfigs []server.Config
	apiServerConfig := server.Config{
		Domain:   domain,
		Address:  apiaddress,
		TLS:      tls,
		CertFile: certFile,
		KeyFile:  keyFile,
		APIType: server.MinioAPI{
			StorageType: storageType,
		},
	}
	webUIServerConfig := server.Config{
		Domain:   domain,
		Address:  webaddress,
		TLS:      false,
		CertFile: "",
		KeyFile:  "",
		APIType: server.Web{
			Websocket: false,
		},
	}
	serverConfigs = append(serverConfigs, apiServerConfig)
	serverConfigs = append(serverConfigs, webUIServerConfig)
	server.Start(serverConfigs)
}

func main() {
	app := cli.NewApp()
	app.Name = "minio"
	app.Usage = ""
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "domain,d",
			Value: "",
			Usage: "domain used for routing incoming API requests",
		},
		cli.StringFlag{
			Name:  "api-address,a",
			Value: ":9000",
			Usage: "address for incoming API requests",
		},
		cli.StringFlag{
			Name:  "web-address,w",
			Value: ":9001",
			Usage: "address for incoming Management UI requests",
		},
		cli.StringFlag{
			Name:  "cert,c",
			Value: "",
			Usage: "cert.pem",
		},
		cli.StringFlag{
			Name:  "key,k",
			Value: "",
			Usage: "key.pem",
		},
		cli.StringFlag{
			Name:  "storage-type,s",
			Value: "file",
			Usage: "valid entries: file,inmemory",
		},
	}
	app.Action = runCmd
	app.Run(os.Args)
}
