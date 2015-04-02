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
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/minio-io/cli"
	"github.com/minio-io/iodine"
	"github.com/minio-io/minio/pkg/server"
	"github.com/minio-io/minio/pkg/utils/log"
)

var globalDebugFlag = false

var flags = []cli.Flag{
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
		Hide:  true,
		Value: "",
		Usage: "cert.pem",
	},
	cli.StringFlag{
		Name:  "key,k",
		Hide:  true,
		Value: "",
		Usage: "key.pem",
	},
	cli.StringFlag{
		Name:  "driver-type,t",
		Value: "donut",
		Usage: "valid entries: file,inmemory,donut",
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "print debug information",
	},
}

func init() {
	// Check for the environment early on and gracefuly report.
	_, err := user.Current()
	if err != nil {
		log.Fatalf("minio: Unable to obtain user's home directory. \nError: %s\n", err)
	}
}

func getDriverType(input string) server.DriverType {
	switch {
	case input == "file":
		return server.File
	case input == "memory":
		return server.Memory
	case input == "donut":
		return server.Donut
	case input == "":
		return server.Donut
	default:
		{
			log.Fatal("Unknown driver type: '", input, "', Please specify a valid driver.")
			return -1 // should never reach here
		}
	}
}

func runCmd(c *cli.Context) {
	driverTypeStr := c.String("driver-type")
	domain := c.String("domain")
	apiaddress := c.String("api-address")
	webaddress := c.String("web-address")
	certFile := c.String("cert")
	keyFile := c.String("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		log.Fatal("Both certificate and key must be provided to enable https")
	}
	tls := (certFile != "" && keyFile != "")
	driverType := getDriverType(driverTypeStr)
	var serverConfigs []server.Config
	apiServerConfig := server.Config{
		Domain:   domain,
		Address:  apiaddress,
		TLS:      tls,
		CertFile: certFile,
		KeyFile:  keyFile,
		APIType: server.MinioAPI{
			DriverType: driverType,
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

// Convert bytes to human readable string. Like a 2 MB, 64.2 KB, 52 B
func formatBytes(i int64) (result string) {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f TB", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f GB", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		result = fmt.Sprintf("%.02f MB", float64(i)/1024/1024)
	case i > 1024:
		result = fmt.Sprintf("%.02f KB", float64(i)/1024)
	default:
		result = fmt.Sprintf("%d B", i)
	}
	result = strings.Trim(result, " ")
	return
}

// Tries to get os/arch/platform specific information
// Returns a map of current os/arch/platform/memstats
func getSystemData() map[string]string {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	mem := fmt.Sprintf("Used: %s | Allocated: %s | Used-Heap: %s | Allocated-Heap: %s",
		formatBytes(int64(memstats.Alloc)),
		formatBytes(int64(memstats.TotalAlloc)),
		formatBytes(int64(memstats.HeapAlloc)),
		formatBytes(int64(memstats.HeapSys)))
	platform := fmt.Sprintf("Host: %s | OS: %s | Arch: %s",
		host,
		runtime.GOOS,
		runtime.GOARCH)
	goruntime := fmt.Sprintf("Version: %s | CPUs: %s", runtime.Version(), strconv.Itoa(runtime.NumCPU()))
	return map[string]string{
		"PLATFORM": platform,
		"RUNTIME":  goruntime,
		"MEM":      mem,
	}
}

func main() {
	// set up iodine
	iodine.SetGlobalState("minio.git", minioGitCommitHash)
	iodine.SetGlobalState("minio.starttime", time.Now().Format(time.RFC3339))

	// set up app
	app := cli.NewApp()
	app.Name = "minio"
	app.Version = minioGitCommitHash
	app.Author = "Minio.io"
	app.Usage = "Minimalist Object Storage"
	app.EnableBashCompletion = true
	app.Flags = flags
	app.Action = runCmd
	app.Before = func(c *cli.Context) error {
		globalDebugFlag = c.GlobalBool("debug")
		if globalDebugFlag {
			app.ExtraInfo = getSystemData()
		}
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Error.Println(err)
	}

}
