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
	"runtime"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio-xl/pkg/probe"
)

func init() {
	// Check if minio was compiled using a supported version of Golang.
	checkGolangRuntimeVersion()

	// Check for the environment early on and gracefuly report.
	_, err := userCurrent()
	if err != nil {
		Fatalf("Unable to obtain user's home directory. \nError: %s\n", err)
	}

	if os.Getenv("DOCKERIMAGE") == "1" {
		// the further checks are ignored for docker image
		return
	}

	if os.Geteuid() == 0 {
		Fatalln("Please run ‘minio’ as a non-root user.")
	}

}

func migrate() {
	// Migrate config file
	migrateConfig()
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

func findClosestCommands(command string) []string {
	var closestCommands []string
	for _, value := range commandsTree.PrefixMatch(command) {
		closestCommands = append(closestCommands, value.(string))
	}
	return closestCommands
}

func registerApp() *cli.App {
	// register all commands
	registerCommand(serverCmd)
	registerCommand(configCmd)
	registerCommand(versionCmd)
	registerCommand(updateCmd)

	// register all flags
	registerFlag(addressFlag)
	registerFlag(accessLogFlag)
	registerFlag(rateLimitFlag)
	registerFlag(anonymousFlag)
	registerFlag(certFlag)
	registerFlag(keyFlag)
	registerFlag(jsonFlag)

	// set up app
	app := cli.NewApp()
	app.Name = "Minio"
	// hide --version flag, version is a command
	app.HideVersion = true
	app.Author = "Minio.io"
	app.Usage = "Cloud Storage Server for Micro Services."
	app.Description = `Micro services environment provisions one Minio server per application instance. Scalability is achieved through large number of smaller personalized instances. This version of the Minio binary is built using Filesystem storage backend for magnetic and solid state disks.`
	app.Flags = flags
	app.Commands = commands

	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

DESCRIPTION:
  {{.Description}}

USAGE:
  minio {{if .Flags}}[flags] {{end}}command{{if .Flags}}{{end}} [arguments...]

COMMANDS:
  {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
  {{end}}{{if .Flags}}
FLAGS:
  {{range .Flags}}{{.}}
  {{end}}{{end}}
VERSION:
  ` + minioVersion +
		`
{{range $key, $value := ExtraInfo}}
{{$key}}:
  {{$value}}
{{end}}
`
	app.CommandNotFound = func(ctx *cli.Context, command string) {
		msg := fmt.Sprintf("‘%s’ is not a minio sub-command. See ‘minio help’.", command)
		closestCommands := findClosestCommands(command)
		if len(closestCommands) > 0 {
			msg += fmt.Sprintf("\n\nDid you mean one of these?\n")
			for _, cmd := range closestCommands {
				msg += fmt.Sprintf("        ‘%s’\n", cmd)
			}
		}
		Fatalln(msg)
	}

	return app
}

func main() {
	probe.Init() // Set project's root source path.
	probe.SetAppInfo("Release-Tag", minioReleaseTag)
	probe.SetAppInfo("Commit-ID", minioShortCommitID)
	if os.Getenv("DOCKERIMAGE") == "1" {
		probe.SetAppInfo("Docker-Image", "true")
	}

	app := registerApp()
	app.Before = func(c *cli.Context) error {
		globalJSONFlag = c.GlobalBool("json")
		migrate()
		return nil
	}
	app.ExtraInfo = func() map[string]string {
		return getSystemData()
	}

	app.RunAndExitOnError()
}
