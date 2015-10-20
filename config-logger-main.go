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

// Configure logger
var configLoggerCmd = cli.Command{
	Name:   "logger",
	Usage:  "Configure logger.",
	Action: mainConfigLogger,
	CustomHelpTemplate: `NAME:
   minio config {{.Name}} - {{.Usage}}

USAGE:
   minio config {{.Name}}

`,
}

func mainConfigLogger(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "logger", 1) // last argument is exit code
	}
	if ctx.Args().Get(0) == "mongo" {
		enableLog2Mongo(ctx.Args().Tail())
	}
	if ctx.Args().Get(0) == "syslog" {
		enableLog2Syslog(ctx.Args().Tail())
	}
	if ctx.Args().Get(0) == "file" {
		enableLog2File(ctx.Args().Tail())
	}
}

func enableLog2Mongo(args cli.Args) {
	config, err := loadConfigV2()
	fatalIf(err.Trace(), "Unable to load config", nil)

	config.MongoLogger.Addr = args.Get(0)
	config.MongoLogger.DB = args.Get(1)
	config.MongoLogger.Collection = args.Get(2)

	err = saveConfig(config)
	fatalIf(err.Trace(), "Unable to save config.", nil)
}

func enableLog2Syslog(args cli.Args) {
	config, err := loadConfigV2()
	fatalIf(err.Trace(), "Unable to load config.", nil)

	config.SyslogLogger.Addr = args.Get(0)
	config.SyslogLogger.Network = args.Get(1)
	err = saveConfig(config)
	fatalIf(err.Trace(), "Unable to save config.", nil)
}

func enableLog2File(args cli.Args) {
	config, err := loadConfigV2()
	fatalIf(err.Trace(), "Unable to load config.", nil)
	config.FileLogger.Filename = args.Get(0)
	err = saveConfig(config)
	fatalIf(err.Trace(), "Unable to save config.", nil)
}
