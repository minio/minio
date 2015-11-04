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
	"runtime"

	"github.com/minio/cli"
	"github.com/minio/minio-xl/pkg/probe"
)

// Configure logger
var configLoggerCmd = cli.Command{
	Name:   "logger",
	Usage:  "Configure logger.",
	Action: mainConfigLogger,
	CustomHelpTemplate: `NAME:
   minio config {{.Name}} - {{.Usage}}

USAGE:
   minio config {{.Name}} OPERATION [ARGS...]

   OPERATION = add | list | remove

EXAMPLES:
   1. Configure new mongo logger.
      $ minio config {{.Name}} add mongo localhost:28710 mydb mylogger

   2. Configure new syslog logger. NOTE: syslog logger is not supported on windows.
      $ minio config {{.Name}} add syslog localhost:554 udp

   3. Configure new file logger. "/var/log" should be writable by user.
      $ minio config {{.Name}} add file /var/log/minio.log

   4. List currently configured logger.
      $ minio config {{.Name}} list

   5. Remove/Reset a configured logger.
      $ minio config {{.Name}} remove mongo
`,
}

// Inherit at one place
type config struct {
	*configV2
}

func mainConfigLogger(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "logger", 1) // last argument is exit code
	}
	conf, err := loadConfigV2()
	fatalIf(err.Trace(), "Unable to load config", nil)

	if ctx.Args().Get(0) == "add" {
		args := ctx.Args().Tail()
		if args.Get(0) == "mongo" {
			enableLog2Mongo(&config{conf}, args.Tail())
		}
		if args.Get(0) == "syslog" {
			if runtime.GOOS == "windows" {
				fatalIf(probe.NewError(errInvalidArgument), "Syslog is not supported on windows.", nil)
			}
			enableLog2Syslog(&config{conf}, args.Tail())
		}
		if args.Get(0) == "file" {
			enableLog2File(&config{conf}, args.Tail())
		}
	}
	if ctx.Args().Get(0) == "remove" {
		args := ctx.Args().Tail()
		if args.Get(0) == "mongo" {
			conf.MongoLogger.Addr = ""
			conf.MongoLogger.DB = ""
			conf.MongoLogger.Collection = ""
			err := saveConfig(conf)
			fatalIf(err.Trace(), "Unable to save config.", nil)
		}
		if args.Get(0) == "syslog" {
			if runtime.GOOS == "windows" {
				fatalIf(probe.NewError(errInvalidArgument), "Syslog is not supported on windows.", nil)
			}
			conf.SyslogLogger.Network = ""
			conf.SyslogLogger.Addr = ""
			err := saveConfig(conf)
			fatalIf(err.Trace(), "Unable to save config.", nil)
		}
		if args.Get(0) == "file" {
			conf.FileLogger.Filename = ""
			err := saveConfig(conf)
			fatalIf(err.Trace(), "Unable to save config.", nil)
		}
	}
	if ctx.Args().Get(0) == "list" {
		if globalJSONFlag {
			Println(conf.JSON())
			return
		}
		Println(conf)
	}
}

func enableLog2Mongo(conf *config, args cli.Args) {
	if conf.IsFileLoggingEnabled() {
		Infoln("File logging already enabled. Removing automatically by enabling mongo.")
		conf.FileLogger.Filename = ""
	}
	if conf.IsSysloggingEnabled() {
		Infoln("Syslog logging already enabled. Removing automatically by enabling mongo.")
		conf.SyslogLogger.Addr = ""
		conf.SyslogLogger.Network = ""
	}
	conf.MongoLogger.Addr = args.Get(0)
	conf.MongoLogger.DB = args.Get(1)
	conf.MongoLogger.Collection = args.Get(2)

	err := saveConfig(conf.configV2)
	fatalIf(err.Trace(), "Unable to save mongo logging config.", nil)
}

func enableLog2Syslog(conf *config, args cli.Args) {
	if conf.IsFileLoggingEnabled() {
		Infoln("File logging already enabled. Removing automatically by enabling syslog.")
		conf.FileLogger.Filename = ""
	}
	if conf.IsMongoLoggingEnabled() {
		Infoln("Mongo logging already enabled. Removing automatically by enabling syslog.")
		conf.MongoLogger.Addr = ""
		conf.MongoLogger.DB = ""
		conf.MongoLogger.Collection = ""
	}
	conf.SyslogLogger.Addr = args.Get(0)
	conf.SyslogLogger.Network = args.Get(1)
	err := saveConfig(conf.configV2)
	fatalIf(err.Trace(), "Unable to save syslog config.", nil)
}

func enableLog2File(conf *config, args cli.Args) {
	if conf.IsSysloggingEnabled() {
		Infoln("Syslog logging already enabled. Removing automatically by enabling file logging.")
		conf.SyslogLogger.Addr = ""
		conf.SyslogLogger.Network = ""
	}
	if conf.IsMongoLoggingEnabled() {
		Infoln("Mongo logging already enabled. Removing automatically by enabling file logging.")
		conf.MongoLogger.Addr = ""
		conf.MongoLogger.DB = ""
		conf.MongoLogger.Collection = ""
	}
	conf.FileLogger.Filename = args.Get(0)
	err := saveConfig(conf.configV2)
	fatalIf(err.Trace(), "Unable to save file logging config.", nil)
}
