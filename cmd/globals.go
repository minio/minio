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
	"runtime"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"
)

// minio configuration related constants.
const (
	globalMinioCertExpireWarnDays = time.Hour * 24 * 30 // 30 days.

	globalMinioDefaultRegion       = "us-east-1"
	globalMinioDefaultOwnerID      = "minio"
	globalMinioDefaultStorageClass = "STANDARD"
	globalWindowsOSName            = "windows"
	globalNetBSDOSName             = "netbsd"
	globalSolarisOSName            = "solaris"
	globalMinioModeFS              = "mode-server-fs"
	globalMinioModeXL              = "mode-server-xl"
	globalMinioModeDistXL          = "mode-server-distributed-xl"
	globalMinioModeGatewayAzure    = "mode-gateway-azure"
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
	// Indicates if the running minio server is distributed setup.
	globalIsDistXL = false

	// Indicates if the running minio server is an erasure-code backend.
	globalIsXL = false

	// This flag is set to 'true' by default
	globalIsBrowserEnabled = true
	// This flag is set to 'true' when MINIO_BROWSER env is set.
	globalIsEnvBrowser = false
	// Set to true if credentials were passed from env, default is false.
	globalIsEnvCreds = false

	// Maximum size of internal objects parts
	globalPutPartSize = int64(64 * 1024 * 1024)

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
