/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package cmd

import (
	"fmt"
	"os"
	"sort"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var (
	// global flags for minio.
	minioFlags = []cli.Flag{
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "Show help.",
		},
	}
)

// Help template for minio.
var minioHelpTemplate = `NAME:
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
  ` + Version +
	`{{ "\n"}}`

// init - check the environment before main starts
func init() {
	// Check if minio was compiled using a supported version of Golang.
	checkGoVersion()

	// Set global trace flag.
	globalTrace = os.Getenv("MINIO_TRACE") == "1"
}

func migrate() {
	// Migrate config file
	migrateConfig()

	// Migrate other configs here.
}

func enableLoggers() {
	// Enable all loggers here.
	enableConsoleLogger()
	enableFileLogger()
	// Add your logger here.
}

func findClosestCommands(command string) []string {
	var closestCommands []string
	for _, value := range commandsTree.PrefixMatch(command) {
		closestCommands = append(closestCommands, value.(string))
	}
	sort.Strings(closestCommands)
	// Suggest other close commands - allow missed, wrongly added and
	// even transposed characters
	for _, value := range commandsTree.walk(commandsTree.root) {
		if sort.SearchStrings(closestCommands, value.(string)) < len(closestCommands) {
			continue
		}
		// 2 is arbitrary and represents the max
		// allowed number of typed errors
		if DamerauLevenshteinDistance(command, value.(string)) < 2 {
			closestCommands = append(closestCommands, value.(string))
		}
	}
	return closestCommands
}

func registerApp() *cli.App {
	// Register all commands.
	registerCommand(serverCmd)
	registerCommand(versionCmd)
	registerCommand(updateCmd)
	registerCommand(controlCmd)

	// Set up app.
	app := cli.NewApp()
	app.Name = "Minio"
	app.Author = "Minio.io"
	app.Usage = "Cloud Storage Server."
	app.Description = `Minio is an Amazon S3 compatible object storage server. Use it to store photos, videos, VMs, containers, log files, or any blob of data as objects.`
	app.Flags = append(minioFlags, globalFlags...)
	app.Commands = commands
	app.CustomAppHelpTemplate = minioHelpTemplate
	app.CommandNotFound = func(ctx *cli.Context, command string) {
		msg := fmt.Sprintf("‘%s’ is not a minio sub-command. See ‘minio --help’.", command)
		closestCommands := findClosestCommands(command)
		if len(closestCommands) > 0 {
			msg += fmt.Sprintf("\n\nDid you mean one of these?\n")
			for _, cmd := range closestCommands {
				msg += fmt.Sprintf("        ‘%s’\n", cmd)
			}
		}
		console.Fatalln(msg)
	}
	return app
}

// Verify main command syntax.
func checkMainSyntax(c *cli.Context) {
	configPath, err := getConfigPath()
	if err != nil {
		console.Fatalf("Unable to obtain user's home directory. \nError: %s\n", err)
	}
	if configPath == "" {
		console.Fatalln("Config folder cannot be empty, please specify --config-dir <foldername>.")
	}
}

// Main main for minio server.
func Main() {
	app := registerApp()
	app.Before = func(c *cli.Context) error {
		// Sets new config folder.
		setGlobalConfigPath(c.GlobalString("config-dir"))

		// Valid input arguments to main.
		checkMainSyntax(c)

		// Migrate any old version of config / state files to newer format.
		migrate()

		// Initialize config.
		err := initConfig()
		fatalIf(err, "Unable to initialize minio config.")

		// Enable all loggers by now.
		enableLoggers()

		// Set global quiet flag.
		globalQuiet = c.Bool("quiet") || c.GlobalBool("quiet")

		// Do not print update messages, if quiet flag is set.
		if !globalQuiet {
			// Do not print any errors in release update function.
			noError := true
			updateMsg := getReleaseUpdate(minioUpdateStableURL, noError)
			if updateMsg.Update {
				console.Println(updateMsg)
			}
		}

		return nil
	}

	// Start profiler if env is set.
	if profiler := os.Getenv("MINIO_PROFILER"); profiler != "" {
		globalProfiler = startProfiler(profiler)
	}

	// Run the app - exit on error.
	app.RunAndExitOnError()
}
