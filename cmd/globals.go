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
	"crypto/tls"
	"crypto/x509"
	"os"
	"runtime"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"
	miniohttp "github.com/minio/minio/pkg/http"
)

// minio configuration related constants.
const (
	globalMinioCertExpireWarnDays = time.Hour * 24 * 30 // 30 days.

	globalMinioDefaultRegion = ""
	// This is a sha256 output of ``arn:aws:iam::minio:user/admin``,
	// this is kept in present form to be compatible with S3 owner ID
	// requirements -
	//
	// ```
	//    The canonical user ID is the Amazon S3–only concept.
	//    It is 64-character obfuscated version of the account ID.
	// ```
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/example-walkthroughs-managing-access-example4.html
	globalMinioDefaultOwnerID      = "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
	globalMinioDefaultStorageClass = "STANDARD"
	globalWindowsOSName            = "windows"
	globalNetBSDOSName             = "netbsd"
	globalSolarisOSName            = "solaris"
	globalMinioModeFS              = "mode-server-fs"
	globalMinioModeXL              = "mode-server-xl"
	globalMinioModeDistXL          = "mode-server-distributed-xl"
	globalMinioModeGatewayAzure    = "mode-gateway-azure"
	globalMinioModeGatewayS3       = "mode-gateway-s3"
	globalMinioModeGatewayGCS      = "mode-gateway-gcs"

	// globalMinioSysTmp prefix is used in Azure/GCS gateway for save metadata sent by Initialize Multipart Upload API.
	globalMinioSysTmp = "minio.sys.tmp/"

	// Add new global values here.
)

const (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1 * humanize.MiByte)

	// Limit memory allocation to store multipart data
	maxFormMemory = int64(5 * humanize.MiByte)

	// The maximum allowed time difference between the incoming request
	// date and server date during signature verification.
	globalMaxSkewTime = 15 * time.Minute // 15 minutes skew allowed.

	// Default Read/Write timeouts for each connection.
	globalConnReadTimeout  = 15 * time.Minute // Timeout after 15 minutes of no data sent by the client.
	globalConnWriteTimeout = 15 * time.Minute // Timeout after 15 minutes if no data received by the client.
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

	// This flag is set to 'true' wen MINIO_REGION env is set.
	globalIsEnvRegion = false

	// This flag is set to 'us-east-1' by default
	globalServerRegion = globalMinioDefaultRegion

	// Maximum size of internal objects parts
	globalPutPartSize = int64(64 * 1024 * 1024)

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

	// IsSSL indicates if the server is configured with SSL.
	globalIsSSL bool

	globalTLSCertificate *tls.Certificate

	globalHTTPServer        *miniohttp.Server
	globalHTTPServerErrorCh = make(chan error)
	globalOSSignalCh        = make(chan os.Signal, 1)

	// List of admin peers.
	globalAdminPeers = adminPeers{}

	// Minio server user agent string.
	globalServerUserAgent = "Minio/" + ReleaseTag + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"

	globalEndpoints EndpointList

	// Global server's network statistics
	globalConnStats = newConnStats()

	// Global HTTP request statisitics
	globalHTTPStats = newHTTPStats()

	// Time when object layer was initialized on start up.
	globalBootTime time.Time

	globalActiveCred         credential
	globalPublicCerts        []*x509.Certificate
	globalXLObjCacheDisabled bool
	// Add new variable global values here.

	globalListingTimeout   = newDynamicTimeout( /*30*/ 600*time.Second /*5*/, 600*time.Second) // timeout for listing related ops
	globalObjectTimeout    = newDynamicTimeout( /*1*/ 10*time.Minute /*10*/, 600*time.Second)  // timeout for Object API related ops
	globalOperationTimeout = newDynamicTimeout(10*time.Minute /*30*/, 600*time.Second)         // default timeout for general ops
	globalHealingTimeout   = newDynamicTimeout(30*time.Minute /*1*/, 30*time.Minute)           // timeout for healing related ops

	// Keep connection active for clients actively using ListenBucketNotification.
	globalSNSConnAlive = 5 * time.Second // Send a whitespace every 5 seconds.
)

// global colors.
var (
	colorBold   = color.New(color.Bold).SprintFunc()
	colorBlue   = color.New(color.FgBlue).SprintfFunc()
	colorYellow = color.New(color.FgYellow).SprintfFunc()
)

// Returns minio global information, as a key value map.
// returned list of global values is not an exhaustive
// list. Feel free to add new relevant fields.
func getGlobalInfo() (globalInfo map[string]interface{}) {
	globalInfo = map[string]interface{}{
		"isDistXL":         globalIsDistXL,
		"isXL":             globalIsXL,
		"isBrowserEnabled": globalIsBrowserEnabled,
		"isEnvBrowser":     globalIsEnvBrowser,
		"isEnvCreds":       globalIsEnvCreds,
		"isEnvRegion":      globalIsEnvRegion,
		"isSSL":            globalIsSSL,
		"serverRegion":     globalServerRegion,
		"serverUserAgent":  globalServerUserAgent,
		// Add more relevant global settings here.
	}

	return globalInfo
}
