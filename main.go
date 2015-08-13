/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

	"github.com/minio/minio/internal/github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/github.com/minio/cli"
)

var globalDebugFlag = false

var flags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: "ADDRESS:PORT for cloud storage access",
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

func init() {
	if _, err := user.Current(); err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
}

func main() {
	// set up go max processes
	runtime.GOMAXPROCS(runtime.NumCPU())

	// set up app
	app := cli.NewApp()
	app.Name = "minio"
	app.Version = getVersion()
	app.Compiled = getVersion()
	app.Author = "Minio.io"
	app.Usage = "Minio Cloud Storage"
	app.Flags = flags
	app.Commands = commands
	app.Before = func(c *cli.Context) error {
		globalDebugFlag = c.GlobalBool("debug")
		return nil
	}
	app.ExtraInfo = func() map[string]string {
		if globalDebugFlag {
			return getSystemData()
		}
		return make(map[string]string)
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
  {{range $key, $value := ExtraInfo}}
{{$key}}:
  {{$value}}
{{end}}

`
	app.CommandNotFound = func(ctx *cli.Context, command string) {
		Fatalf("Command not found: ‘%s’\n", command)
	}

	app.RunAndExitOnError()
}
