/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"os"
	"time"

	"github.com/minio/minio-go/v6/pkg/set"

	etcd "github.com/coreos/etcd/clientv3"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/config/etcd/dns"
	xldap "github.com/minio/minio/cmd/config/identity/ldap"
	"github.com/minio/minio/cmd/config/identity/openid"
	"github.com/minio/minio/cmd/config/policy/opa"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/auth"

	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/pubsub"
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
	globalNetBSDOSName             = "netbsd"
	globalMacOSName                = "darwin"
	globalMinioModeFS              = "mode-server-fs"
	globalMinioModeErasure         = "mode-server-xl"
	globalMinioModeDistErasure     = "mode-server-distributed-xl"
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

	// GlobalMultipartExpiry - Expiry duration after which the multipart uploads are deemed stale.
	GlobalMultipartExpiry = time.Hour * 24 * 3 // 3 days.
	// GlobalMultipartCleanupInterval - Cleanup interval when the stale multipart cleanup is initiated.
	GlobalMultipartCleanupInterval = time.Hour * 24 // 24 hrs.

	// GlobalServiceExecutionInterval - Executes the Lifecycle events.
	GlobalServiceExecutionInterval = time.Hour * 24 // 24 hrs.

	// Refresh interval to update in-memory iam config cache.
	globalRefreshIAMInterval = 5 * time.Minute

	// Limit of location constraint XML for unauthenticted PUT bucket operations.
	maxLocationConstraintSize = 3 * humanize.MiByte

	// Maximum size of default bucket encryption configuration allowed
	maxBucketSSEConfigSize = 1 * humanize.MiByte

	// diskFillFraction is the fraction of a disk we allow to be filled.
	diskFillFraction = 0.95
)

var globalCLIContext = struct {
	JSON, Quiet    bool
	Anonymous      bool
	Addr           string
	StrictS3Compat bool
}{}

var (
	// Indicates set drive count.
	globalErasureSetDriveCount int

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

	// This flag is set to 'true' when MINIO_UPDATE env is set to 'off'. Default is false.
	globalInplaceUpdateDisabled = false

	// This flag is set to 'us-east-1' by default
	globalServerRegion = globalMinioDefaultRegion

	// MinIO local server address (in `host:port` format)
	globalMinioAddr = ""
	// MinIO default port, can be changed through command line.
	globalMinioPort = GlobalMinioDefaultPort
	// Holds the host that was passed using --address
	globalMinioHost = ""
	// Holds the possible host endpoint.
	globalMinioEndpoint = ""

	// globalConfigSys server config system.
	globalConfigSys *ConfigSys

	globalNotificationSys  *NotificationSys
	globalConfigTargetList *event.TargetList
	// globalEnvTargetList has list of targets configured via env.
	globalEnvTargetList *event.TargetList

	globalBucketMetadataSys *BucketMetadataSys
	globalPolicySys         *PolicySys
	globalIAMSys            *IAMSys

	globalLifecycleSys       *LifecycleSys
	globalBucketSSEConfigSys *BucketSSEConfigSys

	// globalAPIConfig controls S3 API requests throttling,
	// healthcheck readiness deadlines and cors settings.
	globalAPIConfig apiConfig

	globalStorageClass storageclass.Config
	globalLDAPConfig   xldap.Config
	globalOpenIDConfig openid.Config

	// CA root certificates, a nil value means system certs pool will be used
	globalRootCAs *x509.CertPool

	// IsSSL indicates if the server is configured with SSL.
	globalIsSSL bool

	globalTLSCerts *certs.Certs

	globalHTTPServer        *xhttp.Server
	globalHTTPServerErrorCh = make(chan error)
	globalOSSignalCh        = make(chan os.Signal, 1)

	// global Trace system to send HTTP request/response logs to
	// registered listeners
	globalHTTPTrace = pubsub.New()

	// global Listen system to send S3 API events to registered listeners
	globalHTTPListen = pubsub.New()

	// global console system to send console logs to
	// registered listeners
	globalConsoleSys *HTTPConsoleLoggerSys

	globalEndpoints EndpointZones

	// Global server's network statistics
	globalConnStats = newConnStats()

	// Global HTTP request statisitics
	globalHTTPStats = newHTTPStats()

	// Time when the server is started
	globalBootTime = UTCNow()

	globalActiveCred auth.Credentials

	// Hold the old server credentials passed by the environment
	globalOldCred auth.Credentials

	// Indicates if config is to be encrypted
	globalConfigEncrypted bool

	globalPublicCerts []*x509.Certificate

	globalDomainNames []string      // Root domains for virtual host style requests
	globalDomainIPs   set.StringSet // Root domain IP address(s) for a distributed MinIO deployment

	globalObjectTimeout    = newDynamicTimeout( /*1*/ 10*time.Minute /*10*/, 600*time.Second) // timeout for Object API related ops
	globalOperationTimeout = newDynamicTimeout(10*time.Minute /*30*/, 600*time.Second)        // default timeout for general ops
	globalHealingTimeout   = newDynamicTimeout(30*time.Minute /*1*/, 30*time.Minute)          // timeout for healing related ops

	globalBucketObjectLockSys *BucketObjectLockSys
	globalBucketQuotaSys      *BucketQuotaSys
	globalBucketVersioningSys *BucketVersioningSys

	// Disk cache drives
	globalCacheConfig cache.Config

	// Initialized KMS configuration for disk cache
	globalCacheKMS crypto.KMS

	// Allocated etcd endpoint for config and bucket DNS.
	globalEtcdClient *etcd.Client

	// Is set to true when Bucket federation is requested
	// and is 'true' when etcdConfig.PathPrefix is empty
	globalBucketFederation bool

	// Allocated DNS config wrapper over etcd client.
	globalDNSConfig *dns.CoreDNS

	// GlobalKMS initialized KMS configuration
	GlobalKMS crypto.KMS

	// Auto-Encryption, if enabled, turns any non-SSE-C request
	// into an SSE-S3 request. If enabled a valid, non-empty KMS
	// configuration must be present.
	globalAutoEncryption bool

	// Is compression enabled?
	globalCompressConfig compress.Config

	// Some standard object extensions which we strictly dis-allow for compression.
	standardExcludeCompressExtensions = []string{".gz", ".bz2", ".rar", ".zip", ".7z", ".xz", ".mp4", ".mkv", ".mov"}

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

	// Only enabled when one of the sub-systems fail
	// to initialize, this allows for administrators to
	// fix the system.
	globalSafeMode bool

	// If writes to FS backend should be O_SYNC.
	globalFSOSync bool

	// Add new variable global values here.
)

// Returns minio global information, as a key value map.
// returned list of global values is not an exhaustive
// list. Feel free to add new relevant fields.
func getGlobalInfo() (globalInfo map[string]interface{}) {
	globalInfo = map[string]interface{}{
		"serverRegion": globalServerRegion,
		"domains":      globalDomainNames,
		// Add more relevant global settings here.
	}

	return globalInfo
}
