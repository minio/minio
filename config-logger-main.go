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
   minio config {{.Name}}

`,
}

// Inherit at one place
type config struct {
	*configV2
}

func (c *config) IsFileLoggingEnabled() bool {
	if c.FileLogger.Filename != "" {
		return true
	}
	return false
}

func (c *config) IsSysloggingEnabled() bool {
	if c.SyslogLogger.Network != "" && c.SyslogLogger.Addr != "" {
		return true
	}
	return false
}

func (c *config) IsMongoLoggingEnabled() bool {
	if c.MongoLogger.Addr != "" && c.MongoLogger.DB != "" && c.MongoLogger.Collection != "" {
		return true
	}
	return false
}

func (c *config) String() string {
	str := fmt.Sprintf("Mongo -> Addr: %s, DB: %s, Collection: %s\n", c.MongoLogger.Addr, c.MongoLogger.DB, c.MongoLogger.Collection)
	str = str + fmt.Sprintf("Syslog -> Addr: %s, Network: %s\n", c.SyslogLogger.Addr, c.SyslogLogger.Network)
	str = str + fmt.Sprintf("File -> Filename: %s", c.FileLogger.Filename)
	return str
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
		Println(&config{conf})
	}
}

func enableLog2Mongo(conf *config, args cli.Args) {
	if conf.IsFileLoggingEnabled() {
		fatalIf(probe.NewError(errInvalidArgument), "File logging already enabled. Please remove before enabling mongo.", nil)
	}
	if conf.IsSysloggingEnabled() {
		fatalIf(probe.NewError(errInvalidArgument), "Syslog logging already enabled. Please remove before enabling mongo.", nil)
	}
	conf.MongoLogger.Addr = args.Get(0)
	conf.MongoLogger.DB = args.Get(1)
	conf.MongoLogger.Collection = args.Get(2)

	err := saveConfig(conf.configV2)
	fatalIf(err.Trace(), "Unable to save config.", nil)
}

func enableLog2Syslog(conf *config, args cli.Args) {
	if conf.IsFileLoggingEnabled() {
		fatalIf(probe.NewError(errInvalidArgument), "File logging already enabled. Please remove before enabling syslog.", nil)
	}
	if conf.IsMongoLoggingEnabled() {
		fatalIf(probe.NewError(errInvalidArgument), "Mongo logging already enabled. Please remove before enabling syslog.", nil)
	}
	conf.SyslogLogger.Addr = args.Get(0)
	conf.SyslogLogger.Network = args.Get(1)
	err := saveConfig(conf.configV2)
	fatalIf(err.Trace(), "Unable to save config.", nil)
}

func enableLog2File(conf *config, args cli.Args) {
	if conf.IsSysloggingEnabled() {
		fatalIf(probe.NewError(errInvalidArgument), "Syslog logging already enabled. Please remove before enabling file.", nil)
	}
	if conf.IsMongoLoggingEnabled() {
		fatalIf(probe.NewError(errInvalidArgument), "Mongo logging already enabled. Please remove before enabling file.", nil)
	}
	conf.FileLogger.Filename = args.Get(0)
	err := saveConfig(conf.configV2)
	fatalIf(err.Trace(), "Unable to save config.", nil)
}
