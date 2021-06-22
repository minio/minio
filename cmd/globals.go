// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/kms"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/cache"
	"github.com/minio/minio/internal/config/compress"
	"github.com/minio/minio/internal/config/dns"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/config/policy/opa"
	"github.com/minio/minio/internal/config/storageclass"
	xhttp "github.com/minio/minio/internal/http"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/pkg/certs"
)

// minio configuration related constants.
const (
	GlobalMinioDefaultPort = "9000"

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
	globalMacOSName                = "darwin"
	globalMinioModeFS              = "mode-server-fs"
	globalMinioModeErasure         = "mode-server-xl"
	globalMinioModeDistErasure     = "mode-server-distributed-xl"
	globalMinioModeGatewayPrefix   = "mode-gateway-"
	globalDirSuffix                = "__XLDIR__"
	globalDirSuffixWithSlash       = globalDirSuffix + slashSeparator

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

	// GlobalStaleUploadsExpiry - Expiry duration after which the uploads in multipart, tmp directory are deemed stale.
	GlobalStaleUploadsExpiry = time.Hour * 24 // 24 hrs.

	// GlobalStaleUploadsCleanupInterval - Cleanup interval when the stale uploads cleanup is initiated.
	GlobalStaleUploadsCleanupInterval = time.Hour * 12 // 12 hrs.

	// GlobalServiceExecutionInterval - Executes the Lifecycle events.
	GlobalServiceExecutionInterval = time.Hour * 24 // 24 hrs.

	// Refresh interval to update in-memory iam config cache.
	globalRefreshIAMInterval = 5 * time.Minute

	// Limit of location constraint XML for unauthenticated PUT bucket operations.
	maxLocationConstraintSize = 3 * humanize.MiByte

	// Maximum size of default bucket encryption configuration allowed
	maxBucketSSEConfigSize = 1 * humanize.MiByte

	// diskFillFraction is the fraction of a disk we allow to be filled.
	diskFillFraction = 0.95

	// diskAssumeUnknownSize is the size to assume when an unknown size upload is requested.
	diskAssumeUnknownSize = 1 << 30

	// diskMinInodes is the minimum number of inodes we want free on a disk to perform writes.
	diskMinInodes = 1000
)

var globalCLIContext = struct {
	JSON, Quiet    bool
	Anonymous      bool
	StrictS3Compat bool
}{}

var (
	// Indicates if the running minio server is distributed setup.
	globalIsDistErasure = false

	// Indicates if the running minio server is an erasure-code backend.
	globalIsErasure = false

	// Indicates if the running minio is in gateway mode.
	globalIsGateway = false

	// Name of gateway server, e.g S3, GCS, Azure, etc
	globalGatewayName = ""

	// This flag is set to 'true' by default
	globalBrowserEnabled = true

	// This flag is set to 'true' by default.
	globalBrowserRedirect = true

	// This flag is set to 'true' when MINIO_UPDATE env is set to 'off'. Default is false.
	globalInplaceUpdateDisabled = false

	// This flag is set to 'us-east-1' by default
	globalServerRegion = globalMinioDefaultRegion

	// MinIO local server address (in `host:port` format)
	globalMinioAddr = ""

	// MinIO default port, can be changed through command line.
	globalMinioPort            = GlobalMinioDefaultPort
	globalMinioConsolePort     = "13333"
	globalMinioConsolePortAuto = false

	// Holds the host that was passed using --address
	globalMinioHost = ""
	// Holds the host that was passed using --console-address
	globalMinioConsoleHost = ""

	// Holds the possible host endpoint.
	globalMinioEndpoint = ""

	// globalConfigSys server config system.
	globalConfigSys *ConfigSys

	globalNotificationSys  *NotificationSys
	globalConfigTargetList *event.TargetList
	// globalEnvTargetList has list of targets configured via env.
	globalEnvTargetList *event.TargetList

	globalBucketMetadataSys *BucketMetadataSys
	globalBucketMonitor     *bandwidth.Monitor
	globalPolicySys         *PolicySys
	globalIAMSys            *IAMSys

	globalLifecycleSys       *LifecycleSys
	globalBucketSSEConfigSys *BucketSSEConfigSys
	globalBucketTargetSys    *BucketTargetSys
	// globalAPIConfig controls S3 API requests throttling,
	// healthcheck readiness deadlines and cors settings.
	globalAPIConfig = apiConfig{listQuorum: 3}

	globalStorageClass storageclass.Config
	globalLDAPConfig   xldap.Config
	globalOpenIDConfig openid.Config

	// CA root certificates, a nil value means system certs pool will be used
	globalRootCAs *x509.CertPool

	// IsSSL indicates if the server is configured with SSL.
	globalIsTLS bool

	globalTLSCerts *certs.Manager

	globalHTTPServer        *xhttp.Server
	globalHTTPServerErrorCh = make(chan error)
	globalOSSignalCh        = make(chan os.Signal, 1)

	// global Trace system to send HTTP request/response
	// and Storage/OS calls info to registered listeners.
	globalTrace = pubsub.New()

	// global Listen system to send S3 API events to registered listeners
	globalHTTPListen = pubsub.New()

	// global console system to send console logs to
	// registered listeners
	globalConsoleSys *HTTPConsoleLoggerSys

	globalEndpoints EndpointServerPools

	// The name of this local node, fetched from arguments
	globalLocalNodeName string

	globalRemoteEndpoints map[string]Endpoint

	// Global server's network statistics
	globalConnStats = newConnStats()

	// Global HTTP request statisitics
	globalHTTPStats = newHTTPStats()

	// Time when the server is started
	globalBootTime = UTCNow()

	globalActiveCred auth.Credentials

	globalPublicCerts []*x509.Certificate

	globalDomainNames []string      // Root domains for virtual host style requests
	globalDomainIPs   set.StringSet // Root domain IP address(s) for a distributed MinIO deployment

	globalOperationTimeout       = newDynamicTimeout(10*time.Minute, 5*time.Minute) // default timeout for general ops
	globalDeleteOperationTimeout = newDynamicTimeout(5*time.Minute, 1*time.Minute)  // default time for delete ops

	globalBucketObjectLockSys *BucketObjectLockSys
	globalBucketQuotaSys      *BucketQuotaSys
	globalBucketVersioningSys *BucketVersioningSys

	// Disk cache drives
	globalCacheConfig cache.Config

	// Initialized KMS configuration for disk cache
	globalCacheKMS kms.KMS

	// Allocated etcd endpoint for config and bucket DNS.
	globalEtcdClient *etcd.Client

	// Is set to true when Bucket federation is requested
	// and is 'true' when etcdConfig.PathPrefix is empty
	globalBucketFederation bool

	// Allocated DNS config wrapper over etcd client.
	globalDNSConfig dns.Store

	// GlobalKMS initialized KMS configuration
	GlobalKMS kms.KMS

	// Auto-Encryption, if enabled, turns any non-SSE-C request
	// into an SSE-S3 request. If enabled a valid, non-empty KMS
	// configuration must be present.
	globalAutoEncryption bool

	// Is compression enabled?
	globalCompressConfigMu sync.Mutex
	globalCompressConfig   compress.Config

	// Some standard object extensions which we strictly dis-allow for compression.
	standardExcludeCompressExtensions = []string{".gz", ".bz2", ".rar", ".zip", ".7z", ".xz", ".mp4", ".mkv", ".mov", ".jpg", ".png", ".gif"}

	// Some standard content-types which we strictly dis-allow for compression.
	standardExcludeCompressContentTypes = []string{"video/*", "audio/*", "application/zip", "application/x-gzip", "application/x-zip-compressed", " application/x-compress", "application/x-spoon"}

	// Authorization validators list.
	globalOpenIDValidators *openid.Validators

	// OPA policy system.
	globalPolicyOPA *opa.Opa

	// Deployment ID - unique per deployment
	globalDeploymentID string

	// GlobalGatewaySSE sse options
	GlobalGatewaySSE gatewaySSE

	globalAllHealState *allHealState

	// The always present healing routine ready to heal objects
	globalBackgroundHealRoutine *healRoutine
	globalBackgroundHealState   *allHealState

	// If writes to FS backend should be O_SYNC.
	globalFSOSync bool

	globalProxyEndpoints []ProxyEndpoint

	globalInternodeTransport http.RoundTripper

	globalProxyTransport http.RoundTripper

	globalDNSCache *xhttp.DNSCache

	globalForwarder *handlers.Forwarder

	globalTierConfigMgr *TierConfigMgr

	globalTierJournal *tierJournal

	globalDebugRemoteTiersImmediately []string
	// Add new variable global values here.
)

var errSelfTestFailure = errors.New("self test failed. unsafe to start server")
