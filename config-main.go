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

import "github.com/minio/cli"

//   Configure minio server
//
//   ----
//   NOTE: that the configure command only writes values to the config file.
//   It does not use any configuration values from the environment variables.
//   ----
//
var configCmd = cli.Command{
	Name:   "config",
	Usage:  "Collection of config management commands.",
	Action: mainConfig,
	Subcommands: []cli.Command{
		configLoggerCmd,
		configVersionCmd,
	},
	CustomHelpTemplate: `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .Flags}}[global flags] {{end}}command{{if .Flags}} [command flags]{{end}} [arguments...]

COMMANDS:
  {{range .Commands}}{{ .Name }}{{ "\t" }}{{.Usage}}
  {{end}}
`,
}

// mainConfig is the handle for "minio config" command. provides sub-commands which write configuration data in json format to config file.
func mainConfig(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowAppHelp(ctx)
	}
}
