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
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}COMMAND{{if .VisibleFlags}}{{end}} [ARGS...]

COMMANDS:
  {{range .VisibleCommands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
  {{end}}{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
VERSION:
  ` + Version +
	`{{ "\n"}}`

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
	for _, value := range commandsTree.Walk(commandsTree.Root()) {
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
	app.Version = Version
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
		older, downloadURL, err := getUpdateInfo(1 * time.Second)
		if err != nil {
			// Its OK to ignore any errors during getUpdateInfo() here.
			return
		}
		if older > time.Duration(0) {
			console.Println(colorizeUpdateMessage(downloadURL, older))
		}
	}
}

// Initializes a new config if it doesn't exist, else migrates any old config
// to newer config and finally loads the config to memory.
func initConfig() {
	envCreds := mustGetCredentialFromEnv()

	// Config file does not exist, we create it fresh and return upon success.
	if !isConfigFileExists() {
		if err := newConfig(envCreds); err != nil {
			console.Fatalf("Unable to initialize minio config for the first time. Err: %s.\n", err)
		}
		console.Println("Created minio configuration file successfully at " + mustGetConfigPath())
		return
	}

	// Migrate any old version of config / state files to newer format.
	migrate()

	// Once we have migrated all the old config, now load them.
	if err := loadConfig(envCreds); err != nil {
		console.Fatalf("Unable to initialize minio config. Err: %s.\n", err)
	}
}

// Generic Minio initialization to create/load config, prepare loggers, etc..
func minioInit(ctx *cli.Context) {
	// Set global variables after parsing passed arguments
	setGlobalsFromContext(ctx)

	// Sets new config directory.
	setGlobalConfigPath(globalConfigDir)

	// Is TLS configured?.
	globalIsSSL = isSSL()

	// Initialize minio server config.
	initConfig()

	// Enable all loggers by now so we can use errorIf() and fatalIf()
	enableLoggers()

	// Init the error tracing module.
	initError()

}

// Main main for minio server.
func Main(args []string, exitFn func(int)) {
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
	if err := app.Run(args); err != nil {
		exitFn(1)
	}
}
