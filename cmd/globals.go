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
	"github.com/fatih/color"
	"github.com/minio/minio/pkg/objcache"
)

// Global constants for Minio.
const (
	minGoVersion = ">= 1.6" // Minio requires at least Go v1.6
)

// minio configuration related constants.
const (
	globalMinioConfigVersion = "6"
	globalMinioConfigDir     = ".minio"
	globalMinioCertsDir      = "certs"
	globalMinioCertFile      = "public.crt"
	globalMinioKeyFile       = "private.key"
	globalMinioConfigFile    = "config.json"
	// Add new global values here.
)

var (
	globalQuiet = false // Quiet flag set via command line
	globalTrace = false // Trace flag set via environment setting.
	// Add new global flags here.

	// Maximum connections handled per
	// server, defaults to 0 (unlimited).
	globalMaxConn = 0
	// Maximum cache size.
	globalMaxCacheSize = uint64(maxCacheSize)
	// Cache expiry.
	globalCacheExpiry = objcache.DefaultExpiry
	// Add new variable global values here.
)

var (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1024 * 1024)
)

// global colors.
var (
	colorBlue = color.New(color.FgBlue).SprintfFunc()
	colorBold = color.New(color.Bold).SprintFunc()
)
