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

import "github.com/fatih/color"

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
	globalMinioProfilePath   = "profile"
	// Add new global values here.
)

var (
	globalQuiet = false // Quiet flag set via command line
	globalTrace = false // Trace flag set via environment setting.
	// Add new global flags here.
)

// global colors.
var (
	colorMagenta = color.New(color.FgMagenta, color.Bold).SprintfFunc()
	colorWhite   = color.New(color.FgWhite, color.Bold).SprintfFunc()
	colorGreen   = color.New(color.FgGreen, color.Bold).SprintfFunc()
)
