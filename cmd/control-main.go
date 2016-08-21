/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

import "github.com/minio/cli"

// "minio control" command.
var controlCmd = cli.Command{
	Name:   "control",
	Usage:  "Control and manage minio server.",
	Action: mainControl,
	Subcommands: []cli.Command{
		healCmd,
		shutdownCmd,
	},
	CustomHelpTemplate: `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.Name}} [FLAGS] COMMAND

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}
COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}
`,
}

func mainControl(ctx *cli.Context) {
	if ctx.Args().First() != "" { // command help.
		cli.ShowCommandHelp(ctx, ctx.Args().First())
	} else {
		// command with Subcommands is an App.
		cli.ShowAppHelp(ctx)
	}
}
