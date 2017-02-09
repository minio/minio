/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"net/url"
	"os"
	"runtime"
	"strings"
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
	globalMinioConfigVersion      = "13"
	globalMinioConfigDir          = ".minio"
	globalMinioCertsDir           = "certs"
	globalMinioCertsCADir         = "CAs"
	globalMinioCertFile           = "public.crt"
	globalMinioKeyFile            = "private.key"
	globalMinioConfigFile         = "config.json"
	globalMinioCertExpireWarnDays = time.Hour * 24 * 30 // 30 days.

	globalMinioDefaultRegion       = "us-east-1"
	globalMinioDefaultOwnerID      = "minio"
	globalMinioDefaultStorageClass = "STANDARD"
	globalWindowsOSName            = "windows"
	globalNetBSDOSName             = "netbsd"
	globalSolarisOSName            = "solaris"
	// Add new global values here.
)

const (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1 * humanize.MiByte)

	// Limit memory allocation to store multipart data
	maxFormMemory = int64(5 * humanize.MiByte)

	// The maximum allowed difference between the request generation time and the server processing time
	globalMaxSkewTime = 15 * time.Minute
)

var (
	globalQuiet     = false               // quiet flag set via command line.
	globalConfigDir = mustGetConfigPath() // config-dir flag set via command line
	// Add new global flags here.

	// Indicates if the running minio server is distributed setup.
	globalIsDistXL = false

	// Indicates if the running minio server is an erasure-code backend.
	globalIsXL = false

	// This flag is set to 'true' by default, it is set to `false`
	// when MINIO_BROWSER env is set to 'off'.
	globalIsBrowserEnabled = !strings.EqualFold(os.Getenv("MINIO_BROWSER"), "off")

	// Maximum cache size. Defaults to disabled.
	// Caching is enabled only for RAM size > 8GiB.
	globalMaxCacheSize = uint64(0)

	// Maximum size of internal objects parts
	globalPutPartSize = int64(64 * 1024 * 1024)

	// Cache expiry.
	globalCacheExpiry = objcache.DefaultExpiry

	// Minio local server address (in `host:port` format)
	globalMinioAddr = ""
	// Minio default port, can be changed through command line.
	globalMinioPort = "9000"
	// Holds the host that was passed using --address
	globalMinioHost = ""

	// Holds the list of API endpoints for a given server.
	globalAPIEndpoints = []string{}

	// Peer communication struct
	globalS3Peers = s3Peers{}

	// CA root certificates, a nil value means system certs pool will be used
	globalRootCAs *x509.CertPool

	// IsSSL indicates if the server is configured with SSL.
	globalIsSSL bool

	// List of admin peers.
	globalAdminPeers = adminPeers{}

	// Minio server user agent string.
	globalServerUserAgent = "Minio/" + ReleaseTag + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"

	// Set to true if credentials were passed from env, default is false.
	globalIsEnvCreds = false

	// url.URL endpoints of disks that belong to the object storage.
	globalEndpoints = []*url.URL{}

	// Global server's network statistics
	globalConnStats = newConnStats()

	// Global HTTP request statisitics
	globalHTTPStats = newHTTPStats()

	// Time when object layer was initialized on start up.
	globalBootTime time.Time

	// Add new variable global values here.
)

var (
	// Keeps the connection active by waiting for following amount of time.
	// Primarily used in ListenBucketNotification.
	globalSNSConnAlive = 5 * time.Second
)

// global colors.
var (
	colorBold = color.New(color.Bold).SprintFunc()
	colorBlue = color.New(color.FgBlue).SprintfFunc()
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
