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

	"errors"
	"github.com/minio-io/cli"
	"github.com/minio-io/minio/pkg/api"
	"github.com/minio-io/minio/pkg/api/web"
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/server"
	"github.com/minio-io/minio/pkg/server/httpserver"
	"github.com/minio-io/minio/pkg/storage/drivers/memory"
	"github.com/minio-io/minio/pkg/utils/log"
	"reflect"
)

var globalDebugFlag = false

var commands = []cli.Command{
	modeCmd,
}

var modeCommands = []cli.Command{
	memoryCmd,
	donutCmd,
}

var modeCmd = cli.Command{
	Name:        "mode",
	Subcommands: modeCommands,
}

var memoryCmd = cli.Command{
	Name:   "memory",
	Action: runMemory,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "max-memory",
			Value: "100M",
			Usage: "",
		},
	},
}

var donutCmd = cli.Command{
	Name:   "donut",
	Action: runDonut,
	Flags:  []cli.Flag{},
}

type memoryFactory struct {
	server.Config

	maxMemory int64
}

func (f memoryFactory) getStartServerFunc() startServerFunc {
	return func() (chan<- string, <-chan error) {
		httpConfig := httpserver.Config{}
		httpConfig.Address = f.Address
		httpConfig.TLS = f.TLS
		httpConfig.CertFile = f.CertFile
		httpConfig.KeyFile = f.KeyFile
		httpConfig.Websocket = false
		_, _, driver := memory.Start(1024 * 1024 * 1024)
		ctrl, status, _ := httpserver.Start(api.HTTPHandler(f.Domain, driver), httpConfig)
		return ctrl, status
	}
}

type webFactory struct {
	server.Config
}

func (f webFactory) getStartServerFunc() startServerFunc {
	return func() (chan<- string, <-chan error) {
		httpConfig := httpserver.Config{}
		httpConfig.Address = f.Address
		httpConfig.TLS = f.TLS
		httpConfig.CertFile = f.CertFile
		httpConfig.KeyFile = f.KeyFile
		httpConfig.Websocket = false
		ctrl, status, _ := httpserver.Start(web.HTTPHandler(), httpConfig)
		return ctrl, status
	}
}

type donutFactory struct {
	server.Config
}

func (f donutFactory) getStartServerFunc() startServerFunc {
	return func() (chan<- string, <-chan error) {
		return nil, nil
	}
}

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

type startServerFunc func() (chan<- string, <-chan error)

func runCmd(c *cli.Context) {
	// default to memory driver, 1GB
	apiServerConfig := getAPIServerConfig(c)
	memoryDriver := memoryFactory{
		Config:    apiServerConfig,
		maxMemory: 1024 * 1024 * 1024,
	}
	apiServer := memoryDriver.getStartServerFunc()
	webServer := getWebServerConfigFunc(c)
	servers := []startServerFunc{apiServer, webServer}
	startMinio(servers)
}

func runMemory(c *cli.Context) {
	apiServerConfig := getAPIServerConfig(c)
	// TODO max-memory should take human readable values
	maxMemory, err := strconv.ParseInt(c.String("max-memory"), 10, 64)
	if err != nil {
		log.Println("max-memory not a numeric value")
	}
	memoryDriver := memoryFactory{
		Config:    apiServerConfig,
		maxMemory: maxMemory,
	}
	apiServer := memoryDriver.getStartServerFunc()
	webServer := getWebServerConfigFunc(c)
	servers := []startServerFunc{apiServer, webServer}
	startMinio(servers)
}

func runDonut(c *cli.Context) {
	apiServerConfig := getAPIServerConfig(c)
	donutDriver := donutFactory{
		Config: apiServerConfig,
	}
	apiServer := donutDriver.getStartServerFunc()
	webServer := getWebServerConfigFunc(c)
	servers := []startServerFunc{apiServer, webServer}
	startMinio(servers)
}

func getAPIServerConfig(c *cli.Context) server.Config {
	certFile := c.String("cert")
	keyFile := c.String("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		log.Fatal("Both certificate and key must be provided to enable https")
	}
	tls := (certFile != "" && keyFile != "")
	return server.Config{
		Domain:   c.String("domain"),
		Address:  c.String("api-address"),
		TLS:      tls,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
}

func getWebServerConfigFunc(c *cli.Context) startServerFunc {
	config := server.Config{
		Domain:   c.String("domain"),
		Address:  c.String("web-address"),
		TLS:      false,
		CertFile: "",
		KeyFile:  "",
	}
	webDrivers := webFactory{
		Config: config,
	}
	return webDrivers.getStartServerFunc()
}

func startMinio(servers []startServerFunc) {
	var ctrlChannels []chan<- string
	var errChannels []<-chan error
	for _, server := range servers {
		ctrlChannel, errChannel := server()
		ctrlChannels = append(ctrlChannels, ctrlChannel)
		errChannels = append(errChannels, errChannel)
	}
	cases := createSelectCases(errChannels)
	for len(cases) > 0 {
		chosen, value, recvOk := reflect.Select(cases)
		switch recvOk {
		case true:
			// Status Message Received
			switch true {
			case value.Interface() != nil:
				// For any error received cleanup all existing channels and fail
				for _, ch := range ctrlChannels {
					close(ch)
				}
				msg := fmt.Sprintf("%q", value.Interface())
				log.Fatal(iodine.New(errors.New(msg), nil))
			}
		case false:
			// Channel closed, remove from list
			var aliveStatusChans []<-chan error
			for i, ch := range errChannels {
				if i != chosen {
					aliveStatusChans = append(aliveStatusChans, ch)
				}
			}
			// create new select cases without defunct channel
			errChannels = aliveStatusChans
			cases = createSelectCases(errChannels)
		}
	}
}

func createSelectCases(channels []<-chan error) []reflect.SelectCase {
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	return cases
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
	app.Commands = commands
	app.Action = runCmd
	app.Before = func(c *cli.Context) error {
		globalDebugFlag = c.GlobalBool("debug")
		if globalDebugFlag {
			app.ExtraInfo = getSystemData()
		}
		return nil
	}
	app.RunAndExitOnError()
}
