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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio/pkg/iodine"
)

var globalDebugFlag = false

var flags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: "ADDRESS:PORT for object storage access",
	},
	cli.StringFlag{
		Name:  "address-mgmt",
		Hide:  true,
		Value: ":9001",
		Usage: "ADDRESS:PORT for management console access",
	},
	cli.IntFlag{
		Name:  "ratelimit",
		Value: 16,
		Usage: "Limit for total concurrent requests: [DEFAULT: 16]",
	},
	cli.StringFlag{
		Name:  "cert",
		Usage: "Provide your domain certificate",
	},
	cli.StringFlag{
		Name:  "key",
		Usage: "Provide your domain private key",
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
		Fatalf("Unable to obtain user's home directory. \nError: %s\n", err)
	}
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
		humanize.Bytes(memstats.Alloc),
		humanize.Bytes(memstats.TotalAlloc),
		humanize.Bytes(memstats.HeapAlloc),
		humanize.Bytes(memstats.HeapSys))
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
	iodine.SetGlobalState("minio.version", Version)
	iodine.SetGlobalState("minio.starttime", time.Now().Format(time.RFC3339))

	// set up go max processes
	runtime.GOMAXPROCS(runtime.NumCPU())

	// set up app
	app := cli.NewApp()
	app.Name = "minio"
	app.Version = Version
	app.Compiled = getVersion()
	app.Author = "Minio.io"
	app.Usage = "Minimalist Object Storage"
	app.Flags = flags
	app.Commands = commands
	app.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			app.ExtraInfo = getSystemData()
		}
		return nil
	}
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .Flags}}[global flags] {{end}}command{{if .Flags}} [command flags]{{end}} [arguments...]

COMMANDS:
  {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
  {{end}}{{if .Flags}}
GLOBAL FLAGS:
  {{range .Flags}}{{.}}
  {{end}}{{end}}
VERSION:
  {{if .Compiled}}
  {{.Compiled}}{{end}}
  {{range $key, $value := .ExtraInfo}}
{{$key}}:
  {{$value}}
{{end}}
`
	app.RunAndExitOnError()
}
