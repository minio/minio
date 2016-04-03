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

package main

import (
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

// Global constants for Minio.
const (
	minGoVersion = ">= 1.6" // Minio requires at least Go v1.6
)

// minio configuration related constants.
const (
	globalMinioConfigVersion = "4"
	globalMinioConfigDir     = ".minio"
	globalMinioCertsDir      = ".minio/certs"
	globalMinioCertFile      = "public.crt"
	globalMinioKeyFile       = "private.key"
	globalMinioConfigFile    = "config.json"
)

var (
	globalQuiet = false // Quiet flag set via command line
	globalDebug = false // Debug flag set via command line
	// Add new global flags here.
)

// global colors.
var (
	colorMagenta = color.New(color.FgMagenta, color.Bold).SprintfFunc()
	colorWhite   = color.New(color.FgWhite, color.Bold).SprintfFunc()
	colorGreen   = color.New(color.FgGreen, color.Bold).SprintfFunc()
)

// Set global states. NOTE: It is deliberately kept monolithic to
// ensure we dont miss out any flags.
func setGlobals(quiet, debug bool) {
	globalQuiet = quiet
	globalDebug = debug
	// Enable debug messages if requested.
	if globalDebug {
		console.DebugPrint = true
	}
}

// Set global states. NOTE: It is deliberately kept monolithic to
// ensure we dont miss out any flags.
func setGlobalsFromContext(ctx *cli.Context) {
	quiet := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	debug := ctx.Bool("debug") || ctx.GlobalBool("debug")
	setGlobals(quiet, debug)
}
