// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/trie"
	"github.com/minio/pkg/v3/words"
)

// GlobalFlags - global flags for minio.
var GlobalFlags = []cli.Flag{
	// Deprecated flag, so its hidden now - existing deployments will keep working.
	cli.StringFlag{
		Name:   "config-dir, C",
		Value:  defaultConfigDir.Get(),
		Usage:  "[DEPRECATED] path to legacy configuration directory",
		Hidden: true,
	},
	cli.StringFlag{
		Name:  "certs-dir, S",
		Value: defaultCertsDir.Get(),
		Usage: "path to certs directory",
	},
	cli.BoolFlag{
		Name:  "quiet",
		Usage: "disable startup and info messages",
	},
	cli.BoolFlag{
		Name:  "anonymous",
		Usage: "hide sensitive information from logging",
	},
	cli.BoolFlag{
		Name:  "json",
		Usage: "output logs in JSON format",
	},
	// Deprecated flag, so its hidden now, existing deployments will keep working.
	cli.BoolFlag{
		Name:   "compat",
		Usage:  "enable strict S3 compatibility by turning off certain performance optimizations",
		Hidden: true,
	},
	// This flag is hidden and to be used only during certain performance testing.
	cli.BoolFlag{
		Name:   "no-compat",
		Usage:  "disable strict S3 compatibility by turning on certain performance optimizations",
		Hidden: true,
	},
}

// Help template for minio.
var minioHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

DESCRIPTION:
  {{.Description}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}COMMAND{{if .VisibleFlags}}{{end}} [ARGS...]

COMMANDS:
  {{range .VisibleCommands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
  {{end}}{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
VERSION:
  {{.Version}}
`

func newApp(name string) *cli.App {
	// Collection of minio commands currently supported are.
	commands := []cli.Command{}

	// Collection of minio commands currently supported in a trie tree.
	commandsTree := trie.NewTrie()

	// registerCommand registers a cli command.
	registerCommand := func(command cli.Command) {
		// avoid registering commands which are not being built (via
		// go:build tags)
		if command.Name == "" {
			return
		}
		commands = append(commands, command)
		commandsTree.Insert(command.Name)
	}

	findClosestCommands := func(command string) []string {
		var closestCommands []string
		closestCommands = append(closestCommands, commandsTree.PrefixMatch(command)...)

		sort.Strings(closestCommands)
		// Suggest other close commands - allow missed, wrongly added and
		// even transposed characters
		for _, value := range commandsTree.Walk(commandsTree.Root()) {
			if sort.SearchStrings(closestCommands, value) < len(closestCommands) {
				continue
			}
			// 2 is arbitrary and represents the max
			// allowed number of typed errors
			if words.DamerauLevenshteinDistance(command, value) < 2 {
				closestCommands = append(closestCommands, value)
			}
		}

		return closestCommands
	}

	// Register all commands.
	registerCommand(serverCmd)
	registerCommand(fmtGenCmd)

	// Set up app.
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, h",
		Usage: "show help",
	}
	cli.VersionPrinter = printMinIOVersion

	app := cli.NewApp()
	app.Name = name
	app.Author = "MinIO, Inc."
	app.Version = ReleaseTag
	app.Usage = "High Performance Object Storage"
	app.Description = `Build high performance data infrastructure for machine learning, analytics and application data workloads with MinIO`
	app.Flags = GlobalFlags
	app.HideHelpCommand = true // Hide `help, h` command, we already have `minio --help`.
	app.Commands = commands
	app.CustomAppHelpTemplate = minioHelpTemplate
	app.CommandNotFound = func(ctx *cli.Context, command string) {
		console.Printf("‘%s’ is not a minio sub-command. See ‘minio --help’.\n", command)
		closestCommands := findClosestCommands(command)
		if len(closestCommands) > 0 {
			console.Println()
			console.Println("Did you mean one of these?")
			for _, cmd := range closestCommands {
				console.Printf("\t‘%s’\n", cmd)
			}
		}

		os.Exit(1)
	}

	return app
}

func startupBanner(banner io.Writer) {
	CopyrightYear = strconv.Itoa(time.Now().Year())
	fmt.Fprintln(banner, color.Blue("Copyright:")+color.Bold(" 2015-%s MinIO, Inc.", CopyrightYear))
	fmt.Fprintln(banner, color.Blue("License:")+color.Bold(" "+MinioLicense))
	fmt.Fprintln(banner, color.Blue("Version:")+color.Bold(" %s (%s %s/%s)", ReleaseTag, runtime.Version(), runtime.GOOS, runtime.GOARCH))
}

func versionBanner(c *cli.Context) io.Reader {
	banner := &strings.Builder{}
	fmt.Fprintln(banner, color.Bold("%s version %s (commit-id=%s)", c.App.Name, c.App.Version, CommitID))
	fmt.Fprintln(banner, color.Blue("Runtime:")+color.Bold(" %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH))
	fmt.Fprintln(banner, color.Blue("License:")+color.Bold(" GNU AGPLv3 - https://www.gnu.org/licenses/agpl-3.0.html"))
	fmt.Fprintln(banner, color.Blue("Copyright:")+color.Bold(" 2015-%s MinIO, Inc.", CopyrightYear))
	return strings.NewReader(banner.String())
}

func printMinIOVersion(c *cli.Context) {
	io.Copy(c.App.Writer, versionBanner(c))
}

var debugNoExit = env.Get("_MINIO_DEBUG_NO_EXIT", "") != ""

// Main main for minio server.
func Main(args []string) {
	// Set the minio app name.
	appName := filepath.Base(args[0])

	if debugNoExit {
		freeze := func(_ int) {
			// Infinite blocking op
			<-make(chan struct{})
		}

		// Override the logger os.Exit()
		logger.ExitFunc = freeze

		defer func() {
			if err := recover(); err != nil {
				fmt.Println("panic:", err)
				fmt.Println("")
				fmt.Println(string(debug.Stack()))
			}
			freeze(-1)
		}()
	}

	// Run the app - exit on error.
	if err := newApp(appName).Run(args); err != nil {
		os.Exit(1) //nolint:gocritic
	}
}
