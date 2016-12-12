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
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var (
	// global flags for minio.
	globalFlags = []cli.Flag{
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "Show help.",
		},
		cli.StringFlag{
			Name:  "config-dir, C",
			Value: mustGetConfigPath(),
			Usage: "Path to configuration directory.",
		},
		cli.BoolFlag{
			Name:  "quiet",
			Usage: "Disable startup information.",
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
}

func migrate() {
	// Migrate config file
	err := migrateConfig()
	fatalIf(err, "Config migration failed.")

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

	// Set up app.
	app := cli.NewApp()
	app.Name = "Minio"
	app.Author = "Minio.io"
	app.Usage = "Cloud Storage Server."
	app.Description = `Minio is an Amazon S3 compatible object storage server. Use it to store photos, videos, VMs, containers, log files, or any blob of data as objects.`
	app.Flags = globalFlags
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
		console.Fatalln("Config directory cannot be empty, please specify --config-dir <directoryname>.")
	}
}

// Check for updates and print a notification message
func checkUpdate() {
	// Do not print update messages, if quiet flag is set.
	if !globalQuiet {
		updateMsg, _, err := getReleaseUpdate(minioUpdateStableURL, 1*time.Second)
		if err != nil {
			// Ignore any errors during getReleaseUpdate(), possibly
			// because of network errors.
			return
		}
		if updateMsg.Update {
			console.Println(updateMsg)
		}
	}
}

// Generic Minio initialization to create/load config, prepare loggers, etc..
func minioInit() {
	// Sets new config directory.
	setGlobalConfigPath(globalConfigDir)

	// Migrate any old version of config / state files to newer format.
	migrate()

	// Initialize config.
	configCreated, err := initConfig()
	if err != nil {
		console.Fatalf("Unable to initialize minio config. Err: %s.\n", err)
	}
	if configCreated {
		console.Println("Created minio configuration file at " + mustGetConfigPath())
	}

	// Enable all loggers by now so we can use errorIf() and fatalIf()
	enableLoggers()

	// Fetch access keys from environment variables and update the config.
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey != "" && secretKey != "" {
		// Set new credentials.
		serverConfig.SetCredential(credential{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		})
	}
	if !isValidAccessKey(serverConfig.GetCredential().AccessKeyID) {
		fatalIf(errInvalidArgument, "Invalid access key. Accept only a string starting with a alphabetic and containing from 5 to 20 characters.")
	}
	if !isValidSecretKey(serverConfig.GetCredential().SecretAccessKey) {
		fatalIf(errInvalidArgument, "Invalid secret key. Accept only a string containing from 8 to 40 characters.")
	}

	// Init the error tracing module.
	initError()

}

// Main main for minio server.
func Main() {
	app := registerApp()
	app.Before = func(c *cli.Context) error {
		// Valid input arguments to main.
		checkMainSyntax(c)
		return nil
	}

	// Start profiler if env is set.
	if profiler := os.Getenv("_MINIO_PROFILER"); profiler != "" {
		globalProfiler = startProfiler(profiler)
	}

	// Run the app - exit on error.
	app.RunAndExitOnError()
}
