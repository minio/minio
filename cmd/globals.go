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

	consoleapi "github.com/minio/console/api"
	"github.com/minio/dnscache"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/browser"
	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/kms"
	"go.uber.org/atomic"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/callhome"
	"github.com/minio/minio/internal/config/compress"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/config/drive"
	idplugin "github.com/minio/minio/internal/config/identity/plugin"
	polplugin "github.com/minio/minio/internal/config/policy/plugin"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/config/subnet"
	xhttp "github.com/minio/minio/internal/http"
	etcd "go.etcd.io/etcd/client/v3"

	levent "github.com/minio/minio/internal/config/lambda/event"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/pkg/v3/certs"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
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
	globalMinioModeErasureSD       = "mode-server-xl-single"
	globalMinioModeErasure         = "mode-server-xl"
	globalMinioModeDistErasure     = "mode-server-distributed-xl"
	globalDirSuffix                = "__XLDIR__"
	globalDirSuffixWithSlash       = globalDirSuffix + slashSeparator

	// Add new global values here.
)

const (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1 * humanize.MiByte)

	// The maximum allowed time difference between the incoming request
	// date and server date during signature verification.
	globalMaxSkewTime = 15 * time.Minute // 15 minutes skew allowed.

	// GlobalStaleUploadsExpiry - Expiry duration after which the uploads in multipart,
	// tmp directory are deemed stale.
	GlobalStaleUploadsExpiry = time.Hour * 24 // 24 hrs.

	// GlobalStaleUploadsCleanupInterval - Cleanup interval when the stale uploads cleanup is initiated.
	GlobalStaleUploadsCleanupInterval = time.Hour * 6 // 6 hrs.

	// Refresh interval to update in-memory iam config cache.
	globalRefreshIAMInterval = 10 * time.Minute

	// Limit of location constraint XML for unauthenticated PUT bucket operations.
	maxLocationConstraintSize = 3 * humanize.MiByte

	// Maximum size of default bucket encryption configuration allowed
	maxBucketSSEConfigSize = 1 * humanize.MiByte

	// diskFillFraction is the fraction of a disk we allow to be filled.
	diskFillFraction = 0.99

	// diskReserveFraction is the fraction of a disk where we will fill other server pools first.
	// If all pools reach this, we will use all pools with regular placement.
	diskReserveFraction = 0.15

	// diskAssumeUnknownSize is the size to assume when an unknown size upload is requested.
	diskAssumeUnknownSize = 1 << 30

	// diskMinInodes is the minimum number of inodes we want free on a disk to perform writes.
	diskMinInodes = 1000

	// tlsClientSessionCacheSize is the cache size for client sessions.
	tlsClientSessionCacheSize = 100
)

func init() {
	// Injected to prevent circular dependency.
	pubsub.GetByteBuffer = grid.GetByteBuffer
}

type poolDisksLayout struct {
	cmdline string
	layout  [][]string
}

type disksLayout struct {
	legacy bool
	pools  []poolDisksLayout
}

type serverCtxt struct {
	JSON, Quiet               bool
	Anonymous                 bool
	StrictS3Compat            bool
	Addr, ConsoleAddr         string
	ConfigDir, CertsDir       string
	configDirSet, certsDirSet bool
	Interface                 string

	RootUser, RootPwd string

	FTP  []string
	SFTP []string

	MemLimit uint64

	UserTimeout         time.Duration
	IdleTimeout         time.Duration
	ReadHeaderTimeout   time.Duration
	MaxIdleConnsPerHost int

	SendBufSize, RecvBufSize int
	CrossDomainXML           string
	// The layout of disks as interpreted
	Layout disksLayout
}

var (
	// Global user opts context
	globalServerCtxt serverCtxt

	// Indicates if the running minio server is distributed setup.
	globalIsDistErasure = false

	// Indicates if the running minio server is an erasure-code backend.
	globalIsErasure = false

	// Indicates if the running minio server is in single drive XL mode.
	globalIsErasureSD = false

	// Indicates if server code should go through testing path.
	globalIsTesting = false

	// This flag is set to 'true' by default
	globalBrowserEnabled = true

	// Custom browser redirect URL, not set by default
	// and it is automatically deduced.
	globalBrowserRedirectURL *xnet.URL

	// Disable redirect, default is enabled.
	globalBrowserRedirect bool

	// globalBrowserConfig Browser user configurable settings
	globalBrowserConfig browser.Config

	// This flag is set to 'true' when MINIO_UPDATE env is set to 'off'. Default is false.
	globalInplaceUpdateDisabled = false

	// Captures site name and region
	globalSite config.Site

	// MinIO local server address (in `host:port` format)
	globalMinioAddr = ""

	// MinIO default port, can be changed through command line.
	globalMinioPort        = GlobalMinioDefaultPort
	globalMinioConsolePort = "13333"

	// Holds the host that was passed using --address
	globalMinioHost = ""
	// Holds the host that was passed using --console-address
	globalMinioConsoleHost = ""

	// Holds the possible host endpoint.
	globalMinioEndpoint    = ""
	globalMinioEndpointURL *xnet.URL

	// globalConfigSys server config system.
	globalConfigSys *ConfigSys

	globalNotificationSys *NotificationSys

	globalEventNotifier    *EventNotifier
	globalNotifyTargetList *event.TargetList
	globalLambdaTargetList *levent.TargetList

	globalBucketMetadataSys *BucketMetadataSys
	globalBucketMonitor     *bandwidth.Monitor
	globalPolicySys         *PolicySys
	globalIAMSys            *IAMSys
	globalBytePoolCap       atomic.Pointer[bpool.BytePoolCap]

	globalLifecycleSys       *LifecycleSys
	globalBucketSSEConfigSys *BucketSSEConfigSys
	globalBucketTargetSys    *BucketTargetSys
	// globalAPIConfig controls S3 API requests throttling,
	// healthCheck readiness deadlines and cors settings.
	globalAPIConfig = apiConfig{listQuorum: "strict", rootAccess: true}

	globalStorageClass storageclass.Config

	globalAuthNPlugin *idplugin.AuthNPlugin

	// CA root certificates, a nil value means system certs pool will be used
	globalRootCAs *x509.CertPool

	// IsSSL indicates if the server is configured with SSL.
	globalIsTLS bool

	globalTLSCerts *certs.Manager

	globalHTTPServer        *xhttp.Server
	globalTCPOptions        xhttp.TCPOptions
	globalHTTPServerErrorCh = make(chan error)
	globalOSSignalCh        = make(chan os.Signal, 1)

	// global Trace system to send HTTP request/response
	// and Storage/OS calls info to registered listeners.
	globalTrace = pubsub.New[madmin.TraceInfo, madmin.TraceType](8)

	// global Listen system to send S3 API events to registered listeners
	globalHTTPListen = pubsub.New[event.Event, pubsub.Mask](0)

	// global console system to send console logs to
	// registered listeners
	globalConsoleSys *HTTPConsoleLoggerSys

	// All unique drives for this deployment
	globalEndpoints EndpointServerPools
	// All unique nodes for this deployment
	globalNodes []Node

	// The name of this local node, fetched from arguments
	globalLocalNodeName    string
	globalLocalNodeNameHex string
	globalNodeNamesHex     = make(map[string]struct{})

	// The global subnet config
	globalSubnetConfig subnet.Config

	// The global callhome config
	globalCallhomeConfig callhome.Config

	// The global drive config
	globalDriveConfig drive.Config

	// Global server's network statistics
	globalConnStats = newConnStats()

	// Global HTTP request statistics
	globalHTTPStats = newHTTPStats()

	// Global bucket network and API statistics
	globalBucketConnStats = newBucketConnStats()
	globalBucketHTTPStats = newBucketHTTPStats()

	// Time when the server is started
	globalBootTime = UTCNow()

	globalActiveCred         auth.Credentials
	globalNodeAuthToken      string
	globalSiteReplicatorCred siteReplicatorCred

	// Captures if root credentials are set via ENV.
	globalCredViaEnv bool

	globalPublicCerts []*x509.Certificate

	globalDomainNames []string      // Root domains for virtual host style requests
	globalDomainIPs   set.StringSet // Root domain IP address(s) for a distributed MinIO deployment

	globalOperationTimeout       = newDynamicTimeout(10*time.Minute, 5*time.Minute) // default timeout for general ops
	globalDeleteOperationTimeout = newDynamicTimeout(5*time.Minute, 1*time.Minute)  // default time for delete ops

	globalBucketObjectLockSys *BucketObjectLockSys
	globalBucketQuotaSys      *BucketQuotaSys
	globalBucketVersioningSys *BucketVersioningSys

	// Allocated etcd endpoint for config and bucket DNS.
	globalEtcdClient *etcd.Client

	// Cluster replication manager.
	globalSiteReplicationSys SiteReplicationSys

	// Cluster replication resync metrics
	globalSiteResyncMetrics *siteResyncMetrics

	// Is set to true when Bucket federation is requested
	// and is 'true' when etcdConfig.PathPrefix is empty
	globalBucketFederation bool

	// Allocated DNS config wrapper over etcd client.
	globalDNSConfig dns.Store

	// GlobalKMS initialized KMS configuration
	GlobalKMS *kms.KMS

	// Common lock for various subsystems performing the leader tasks
	globalLeaderLock *sharedLock

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

	// AuthZ Plugin system.
	globalAuthZPlugin *polplugin.AuthZPlugin

	// Deployment ID - unique per deployment
	globalDeploymentIDPtr atomic.Pointer[string]
	globalDeploymentID    = func() string {
		ptr := globalDeploymentIDPtr.Load()
		if ptr == nil {
			return ""
		}
		return *ptr
	}

	globalAllHealState = newHealState(GlobalContext, true)

	// The always present healing routine ready to heal objects
	globalBackgroundHealRoutine = newHealRoutine()
	globalBackgroundHealState   = newHealState(GlobalContext, false)

	globalMRFState = newMRFState()

	// If writes to FS backend should be O_SYNC.
	globalFSOSync bool

	globalProxyEndpoints []ProxyEndpoint

	globalInternodeTransport http.RoundTripper

	globalRemoteTargetTransport http.RoundTripper

	globalDNSCache = &dnscache.Resolver{
		Timeout: 5 * time.Second,
	}

	globalForwarder *handlers.Forwarder

	globalTierConfigMgr *TierConfigMgr

	globalConsoleSrv *consoleapi.Server

	// handles service freeze or un-freeze S3 API calls.
	globalServiceFreeze atomic.Value

	// Only needed for tracking
	globalServiceFreezeCnt int32
	globalServiceFreezeMu  sync.Mutex // Updates.

	// Map of local drives to this node, this is set during server startup,
	// disk reconnect and mutated by HealFormat. Hold globalLocalDrivesMu to access.
	globalLocalDrivesMap map[string]StorageAPI
	globalLocalDrivesMu  sync.RWMutex

	globalDriveMonitoring = env.Get("_MINIO_DRIVE_ACTIVE_MONITORING", config.EnableOn) == config.EnableOn

	// Is MINIO_CI_CD set?
	globalIsCICD bool

	globalRootDiskThreshold uint64

	// Used for collecting stats for netperf
	globalNetPerfMinDuration     = time.Second * 10
	globalNetPerfRX              netPerfRX
	globalSiteNetPerfRX          netPerfRX
	globalObjectPerfBucket       = "minio-perf-test-tmp-bucket"
	globalObjectPerfUserMetadata = "X-Amz-Meta-Minio-Object-Perf" // Clients can set this to bypass S3 API service freeze. Used by object pref tests.

	// MinIO version unix timestamp
	globalVersionUnix uint64

	// MinIO client
	globalMinioClient *minio.Client

	// Public key for subnet confidential information
	subnetAdminPublicKey    = []byte("-----BEGIN PUBLIC KEY-----\nMIIBCgKCAQEAyC+ol5v0FP+QcsR6d1KypR/063FInmNEFsFzbEwlHQyEQN3O7kNI\nwVDN1vqp1wDmJYmv4VZGRGzfFw1q+QV7K1TnysrEjrqpVxfxzDQCoUadAp8IxLLc\ns2fjyDNxnZjoC6fTID9C0khKnEa5fPZZc3Ihci9SiCGkPmyUyCGVSxWXIKqL2Lrj\nyDc0pGeEhWeEPqw6q8X2jvTC246tlzqpDeNsPbcv2KblXRcKniQNbBrizT37CKHQ\nM6hc9kugrZbFuo8U5/4RQvZPJnx/DVjLDyoKo2uzuVQs4s+iBrA5sSSLp8rPED/3\n6DgWw3e244Dxtrg972dIT1IOqgn7KUJzVQIDAQAB\n-----END PUBLIC KEY-----")
	subnetAdminPublicKeyDev = []byte("-----BEGIN PUBLIC KEY-----\nMIIBCgKCAQEArhQYXQd6zI4uagtVfthAPOt6i4AYHnEWCoNeAovM4MNl42I9uQFh\n3VHkbWj9Gpx9ghf6PgRgK+8FcFvy+StmGcXpDCiFywXX24uNhcZjscX1C4Esk0BW\nidfI2eXYkOlymD4lcK70SVgJvC693Qa7Z3FE1KU8Nfv2bkxEE4bzOkojX9t6a3+J\nR8X6Z2U8EMlH1qxJPgiPogELhWP0qf2Lq7GwSAflo1Tj/ytxvD12WrnE0Rrj/8yP\nSnp7TbYm91KocKMExlmvx3l2XPLxeU8nf9U0U+KOmorejD3MDMEPF+tlk9LB3JWP\nZqYYe38rfALVTn4RVJriUcNOoEpEyC0WEwIDAQAB\n-----END PUBLIC KEY-----")

	// dynamic sleeper to avoid thundering herd for trash folder expunge routine
	deleteCleanupSleeper = newDynamicSleeper(5, 25*time.Millisecond, false)

	// dynamic sleeper for multipart expiration routine
	deleteMultipartCleanupSleeper = newDynamicSleeper(5, 25*time.Millisecond, false)

	// Is MINIO_SYNC_BOOT set?
	globalEnableSyncBoot bool

	// Contains NIC interface name used for internode communication
	globalInternodeInterface     string
	globalInternodeInterfaceOnce sync.Once

	// Set last client perf extra time (get lock, and validate)
	globalLastClientPerfExtraTime int64

	// Captures all batch jobs metrics globally
	globalBatchJobsMetrics batchJobMetrics

	// Indicates if server was started as `--address ":0"`
	globalDynamicAPIPort bool

	// Add new variable global values here.
)

var globalAuthPluginMutex sync.Mutex

func newGlobalAuthNPluginFn() *idplugin.AuthNPlugin {
	globalAuthPluginMutex.Lock()
	defer globalAuthPluginMutex.Unlock()
	return globalAuthNPlugin
}

func newGlobalAuthZPluginFn() *polplugin.AuthZPlugin {
	globalAuthPluginMutex.Lock()
	defer globalAuthPluginMutex.Unlock()
	return globalAuthZPlugin
}

func setGlobalAuthNPlugin(authn *idplugin.AuthNPlugin) {
	globalAuthPluginMutex.Lock()
	globalAuthNPlugin = authn
	globalAuthPluginMutex.Unlock()
}

func setGlobalAuthZPlugin(authz *polplugin.AuthZPlugin) {
	globalAuthPluginMutex.Lock()
	globalAuthZPlugin = authz
	globalAuthPluginMutex.Unlock()
}

var errSelfTestFailure = errors.New("self test failed. unsafe to start server")
