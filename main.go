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
	"net/http"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
)

func init() {
	// Check for the environment early on and gracefuly report.
	u, err := user.Current()
	if err != nil {
		Fatalf("Unable to obtain user's home directory. \nError: %s\n", err)
	}
	var uid int
	uid, err = strconv.Atoi(u.Uid)
	if err != nil {
		Fatalf("Unable to convert user id to an integer. \nError: %s\n", err)
	}
	if uid == 0 {
		Fatalln("Please run as a normal user, running as root is disallowed")
	}
	verifyMinioRuntime()
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

// getFormattedVersion -
func getFormattedVersion() string {
	t, _ := time.Parse(time.RFC3339Nano, Version)
	if t.IsZero() {
		return ""
	}
	return t.Format(http.TimeFormat)
}

func registerApp() *cli.App {
	// register all commands
	registerCommand(donutCmd)
	registerCommand(serverCmd)
	registerCommand(controllerCmd)
	registerCommand(versionCmd)

	// register all flags
	registerFlag(addressFlag)
	registerFlag(addressMgmtFlag)
	registerFlag(ratelimitFlag)
	registerFlag(certFlag)
	registerFlag(keyFlag)
	registerFlag(debugFlag)

	// set up app
	app := cli.NewApp()
	app.Name = "minio"
	// hide --version flag, version is a command
	app.HideVersion = true
	app.Author = "Minio.io"
	app.Usage = "Minio Cloud Storage"
	app.Flags = flags
	app.Commands = commands

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
  ` + getFormattedVersion() +
		`{{range $key, $value := ExtraInfo}}
{{$key}}:
  {{$value}}
{{end}}
`
	app.CommandNotFound = func(ctx *cli.Context, command string) {
		Fatalf("Command not found: ‘%s’\n", command)
	}

	return app
}

func main() {
	// set up go max processes
	runtime.GOMAXPROCS(runtime.NumCPU())

	app := registerApp()
	app.Before = func(c *cli.Context) error {
		// get  flag and set global defaults here.
		return nil
	}
	app.ExtraInfo = func() map[string]string {
		return getSystemData()
	}

	app.RunAndExitOnError()
}
