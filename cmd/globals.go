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
	"crypto/x509"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/objcache"
)

// Global constants for Minio.
const (
	minGoVersion = ">= 1.7" // Minio requires at least Go v1.7
)

// minio configuration related constants.
const (
	globalMinioConfigVersion      = "10"
	globalMinioConfigDir          = ".minio"
	globalMinioCertsDir           = "certs"
	globalMinioCertsCADir         = "CAs"
	globalMinioCertFile           = "public.crt"
	globalMinioKeyFile            = "private.key"
	globalMinioConfigFile         = "config.json"
	globalMinioCertExpireWarnDays = time.Hour * 24 * 30 // 30 days.
	// Add new global values here.
)

const (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1 * humanize.MiByte)

	// The maximum allowed difference between the request generation time and the server processing time
	globalMaxSkewTime = 15 * time.Minute
)

var (
	globalQuiet     = false               // quiet flag set via command line.
	globalConfigDir = mustGetConfigPath() // config-dir flag set via command line
	// Add new global flags here.

	globalIsDistXL = false // "Is Distributed?" flag.

	// Maximum cache size.
	globalMaxCacheSize = uint64(maxCacheSize)
	// Cache expiry.
	globalCacheExpiry = objcache.DefaultExpiry
	// Minio local server address (in `host:port` format)
	globalMinioAddr = ""
	// Minio default port, can be changed through command line.
	globalMinioPort = "9000"
	// Holds the host that was passed using --address
	globalMinioHost = ""
	// Peer communication struct
	globalS3Peers = s3Peers{}

	// CA root certificates, a nil value means system certs pool will be used
	globalRootCAs *x509.CertPool

	// Add new variable global values here.
)

var (
	// Keeps the connection active by waiting for following amount of time.
	// Primarily used in ListenBucketNotification.
	globalSNSConnAlive = 5 * time.Second
)

// global colors.
var (
	colorRed   = color.New(color.FgRed).SprintFunc()
	colorBold  = color.New(color.Bold).SprintFunc()
	colorBlue  = color.New(color.FgBlue).SprintfFunc()
	colorGreen = color.New(color.FgGreen).SprintfFunc()
)

// Parse command arguments and set global variables accordingly
func setGlobalsFromContext(c *cli.Context) {
	// Set config dir
	switch {
	case c.IsSet("config-dir"):
		globalConfigDir = c.String("config-dir")
	case c.GlobalIsSet("config-dir"):
		globalConfigDir = c.GlobalString("config-dir")
	}
	if globalConfigDir == "" {
		console.Fatalf("Unable to get config file. Config directory is empty.")
	}
	// Set global quiet flag.
	globalQuiet = c.Bool("quiet") || c.GlobalBool("quiet")
}
