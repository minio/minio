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
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/iodine"
)

var globalDebugFlag = false

var flags = []cli.Flag{
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

func runMkdonut(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowAppHelp(c)
		os.Exit(1)
	}
	donutName := c.Args().First()
	if c.Args().First() != "" {
		if !donut.IsValidDonut(donutName) {
			Fatalf("Invalid donutname %s\n", donutName)
		}
	}
	var disks []string
	for _, disk := range c.Args().Tail() {
		if _, err := isUsable(disk); err != nil {
			Fatalln(err)
		}
		disks = append(disks, disk)
	}
	for _, disk := range disks {
		if err := os.MkdirAll(filepath.Join(disk, donutName), 0700); err != nil {
			Fatalln(err)
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		Fatalln(err)
	}
	donutConfig := &donut.Config{}
	donutConfig.Version = "0.0.1"
	donutConfig.DonutName = donutName
	donutConfig.NodeDiskMap = make(map[string][]string)
	// keep it in exact order as it was specified, do not try to sort disks
	donutConfig.NodeDiskMap[hostname] = disks
	// default cache is unlimited
	donutConfig.MaxSize = 0

	if err := donut.SaveConfig(donutConfig); err != nil {
		Fatalln(err)
	}

	Infoln("Success!")
}

func main() {
	// set up iodine
	iodine.SetGlobalState("mkdonut.version", Version)
	iodine.SetGlobalState("mkdonut.starttime", time.Now().Format(time.RFC3339))

	// set up go max processes
	runtime.GOMAXPROCS(runtime.NumCPU())

	// set up app
	app := cli.NewApp()
	app.Action = runMkdonut
	app.Name = "mkdonut"
	app.Version = Version
	app.Compiled = getVersion()
	app.Author = "Minio.io"
	app.Usage = "Make a donut"
	app.Flags = flags
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
