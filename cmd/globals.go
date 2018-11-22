/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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
	"fmt"
	"os"
	"runtime"
	"time"

	isatty "github.com/mattn/go-isatty"
	"github.com/minio/minio-go/pkg/set"

	etcd "github.com/coreos/etcd/clientv3"
	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/iam/validator"
)

// minio configuration related constants.
const (
	globalMinioCertExpireWarnDays = time.Hour * 24 * 30 // 30 days.

	globalMinioDefaultPort = "9000"

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
	globalMinioModeGatewayPrefix   = "mode-gateway-"

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

	// Expiry duration after which the multipart uploads are deemed stale.
	globalMultipartExpiry = time.Hour * 24 * 14 // 2 weeks.
	// Cleanup interval when the stale multipart cleanup is initiated.
	globalMultipartCleanupInterval = time.Hour * 24 // 24 hrs.
	// Refresh interval to update in-memory bucket policy cache.
	globalRefreshBucketPolicyInterval = 5 * time.Minute
	// Refresh interval to update in-memory iam config cache.
	globalRefreshIAMInterval = 5 * time.Minute

	// Limit of location constraint XML for unauthenticted PUT bucket operations.
	maxLocationConstraintSize = 3 * humanize.MiByte
)

var globalCLIContext = struct {
	JSON, Quiet bool
	Anonymous   bool
	Addr        string
}{}

var (
	// Indicates the total number of erasure coded sets configured.
	globalXLSetCount int

	// Indicates set drive count.
	globalXLSetDriveCount int

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

	// This flag is set to 'true' when MINIO_REGION env is set.
	globalIsEnvRegion = false

	// This flag is set to 'true' when MINIO_UPDATE env is set to 'off'. Default is false.
	globalInplaceUpdateDisabled = false

	// This flag is set to 'us-east-1' by default
	globalServerRegion = globalMinioDefaultRegion

	// Maximum size of internal objects parts
	globalPutPartSize = int64(64 * 1024 * 1024)

	// Minio local server address (in `host:port` format)
	globalMinioAddr = ""
	// Minio default port, can be changed through command line.
	globalMinioPort = globalMinioDefaultPort
	// Holds the host that was passed using --address
	globalMinioHost = ""

	// globalConfigSys server config system.
	globalConfigSys *ConfigSys

	globalNotificationSys *NotificationSys
	globalPolicySys       *PolicySys
	globalIAMSys          *IAMSys

	// CA root certificates, a nil value means system certs pool will be used
	globalRootCAs *x509.CertPool

	// IsSSL indicates if the server is configured with SSL.
	globalIsSSL bool

	globalTLSCerts *certs.Certs

	globalHTTPServer        *xhttp.Server
	globalHTTPServerErrorCh = make(chan error)
	globalOSSignalCh        = make(chan os.Signal, 1)

	// File to log HTTP request/response headers and body.
	globalHTTPTraceFile *os.File

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

	globalActiveCred  auth.Credentials
	globalPublicCerts []*x509.Certificate

	globalIsEnvDomainName bool
	globalDomainName      string        // Root domain for virtual host style requests
	globalDomainIPs       set.StringSet // Root domain IP address(s) for a distributed Minio deployment

	globalListingTimeout   = newDynamicTimeout( /*30*/ 600*time.Second /*5*/, 600*time.Second) // timeout for listing related ops
	globalObjectTimeout    = newDynamicTimeout( /*1*/ 10*time.Minute /*10*/, 600*time.Second)  // timeout for Object API related ops
	globalOperationTimeout = newDynamicTimeout(10*time.Minute /*30*/, 600*time.Second)         // default timeout for general ops
	globalHealingTimeout   = newDynamicTimeout(30*time.Minute /*1*/, 30*time.Minute)           // timeout for healing related ops

	// Storage classes
	// Set to indicate if storage class is set up
	globalIsStorageClass bool
	// Set to store reduced redundancy storage class
	globalRRStorageClass storageClass
	// Set to store standard storage class
	globalStandardStorageClass storageClass

	globalIsEnvWORM bool
	// Is worm enabled
	globalWORMEnabled bool

	// Is Disk Caching set up
	globalIsDiskCacheEnabled bool

	// Disk cache drives
	globalCacheDrives []string

	// Disk cache excludes
	globalCacheExcludes []string

	// Disk cache expiry
	globalCacheExpiry = 90
	// Max allowed disk cache percentage
	globalCacheMaxUse = 80

	// RPC V1 - Initial version
	// RPC V2 - format.json XL version changed to 2
	// RPC V3 - format.json XL version changed to 3
	// RPC V4 - ReadFile() arguments signature changed
	// Current RPC version
	globalRPCAPIVersion = RPCVersion{4, 0, 0}

	// Allocated etcd endpoint for config and bucket DNS.
	globalEtcdClient *etcd.Client

	// Allocated DNS config wrapper over etcd client.
	globalDNSConfig dns.Config

	// Default usage check interval value.
	globalDefaultUsageCheckInterval = 12 * time.Hour // 12 hours
	// Usage check interval value.
	globalUsageCheckInterval = globalDefaultUsageCheckInterval

	// KMS key id
	globalKMSKeyID string
	// Allocated KMS
	globalKMS crypto.KMS

	// Auto-Encryption, if enabled, turns any non-SSE-C request
	// into an SSE-S3 request. If enabled a valid, non-empty KMS
	// configuration must be present.
	globalAutoEncryption bool

	// Is compression include extensions/content-types set.
	globalIsEnvCompression bool

	// Is compression enabeld.
	globalIsCompressionEnabled = false

	// Include-list for compression.
	globalCompressExtensions = []string{".txt", ".log", ".csv", ".json"}
	globalCompressMimeTypes  = []string{"text/csv", "text/plain", "application/json"}

	// Some standard object extensions which we strictly dis-allow for compression.
	standardExcludeCompressExtensions = []string{".gz", ".bz2", ".rar", ".zip", ".7z"}

	// Some standard content-types which we strictly dis-allow for compression.
	standardExcludeCompressContentTypes = []string{"video/*", "audio/*", "application/zip", "application/x-gzip", "application/x-zip-compressed", " application/x-compress", "application/x-spoon"}

	// Authorization validators list.
	globalIAMValidators *validator.Validators

	// OPA policy system.
	globalPolicyOPA *iampolicy.Opa

	// Deployment ID - unique per deployment
	globalDeploymentID string

	// Add new variable global values here.
)

// global colors.
var (
	// Check if we stderr, stdout are dumb terminals, we do not apply
	// ansi coloring on dumb terminals.
	isTerminal = func() bool {
		return isatty.IsTerminal(os.Stdout.Fd()) && isatty.IsTerminal(os.Stderr.Fd())
	}

	colorBold = func() func(a ...interface{}) string {
		if isTerminal() {
			return color.New(color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()
	colorRed = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgRed).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorBlue = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgBlue).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorYellow = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgYellow).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorCyanBold = func() func(a ...interface{}) string {
		if isTerminal() {
			color.New(color.FgCyan, color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()
	colorYellowBold = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgYellow, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorBgYellow = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.BgYellow).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorBlack = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgBlack).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorGreenBold = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgGreen, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	colorRedBold = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgRed, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()
)

// Returns minio global information, as a key value map.
// returned list of global values is not an exhaustive
// list. Feel free to add new relevant fields.
func getGlobalInfo() (globalInfo map[string]interface{}) {
	globalInfo = map[string]interface{}{
		"isDistXL":         globalIsDistXL,
		"isXL":             globalIsXL,
		"isBrowserEnabled": globalIsBrowserEnabled,
		"isWorm":           globalWORMEnabled,
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
