/*
 * MinIO Cloud Storage, (C) 2017-2020 MinIO, Inc.
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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
)

var (
	gatewayCmd = cli.Command{
		Name:            "gateway",
		Usage:           "start object storage gateway",
		Flags:           append(ServerFlags, GlobalFlags...),
		HideHelpCommand: true,
	}
)

// GatewayLocker implements custom NewNSLock implementation
type GatewayLocker struct {
	ObjectLayer
	nsMutex *nsLockMap
}

// NewNSLock - implements gateway level locker
func (l *GatewayLocker) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	return l.nsMutex.NewNSLock(ctx, nil, bucket, objects...)
}

// Walk - implements common gateway level Walker, to walk on all objects recursively at a prefix
func (l *GatewayLocker) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	walk := func(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
		go func() {
			// Make sure the results channel is ready to be read when we're done.
			defer close(results)

			var marker string

			for {
				// set maxKeys to '0' to list maximum possible objects in single call.
				loi, err := l.ObjectLayer.ListObjects(ctx, bucket, prefix, marker, "", 0)
				if err != nil {
					logger.LogIf(ctx, err)
					return
				}
				marker = loi.NextMarker
				for _, obj := range loi.Objects {
					select {
					case results <- obj:
					case <-ctx.Done():
						return
					}
				}
				if !loi.IsTruncated {
					break
				}
			}
		}()
		return nil
	}

	if err := l.ObjectLayer.Walk(ctx, bucket, prefix, results, opts); err != nil {
		if _, ok := err.(NotImplemented); ok {
			return walk(ctx, bucket, prefix, results)
		}
		return err
	}

	return nil
}

// NewGatewayLayerWithLocker - initialize gateway with locker.
func NewGatewayLayerWithLocker(gwLayer ObjectLayer) ObjectLayer {
	return &GatewayLocker{ObjectLayer: gwLayer, nsMutex: newNSLock(false)}
}

// RegisterGatewayCommand registers a new command for gateway.
func RegisterGatewayCommand(cmd cli.Command) error {
	cmd.Flags = append(append(cmd.Flags, ServerFlags...), GlobalFlags...)
	gatewayCmd.Subcommands = append(gatewayCmd.Subcommands, cmd)
	return nil
}

// ParseGatewayEndpoint - Return endpoint.
func ParseGatewayEndpoint(arg string) (endPoint string, secure bool, err error) {
	schemeSpecified := len(strings.Split(arg, "://")) > 1
	if !schemeSpecified {
		// Default connection will be "secure".
		arg = "https://" + arg
	}

	u, err := url.Parse(arg)
	if err != nil {
		return "", false, err
	}

	switch u.Scheme {
	case "http":
		return u.Host, false, nil
	case "https":
		return u.Host, true, nil
	default:
		return "", false, fmt.Errorf("Unrecognized scheme %s", u.Scheme)
	}
}

// ValidateGatewayArguments - Validate gateway arguments.
func ValidateGatewayArguments(serverAddr, endpointAddr string) error {
	if err := CheckLocalServerAddr(serverAddr); err != nil {
		return err
	}

	if endpointAddr != "" {
		// Reject the endpoint if it points to the gateway handler itself.
		sameTarget, err := sameLocalAddrs(endpointAddr, serverAddr)
		if err != nil {
			return err
		}
		if sameTarget {
			return fmt.Errorf("endpoint points to the local gateway")
		}
	}
	return nil
}

// StartGateway - handler for 'minio gateway <name>'.
func StartGateway(ctx *cli.Context, gw Gateway) {
	// This is only to uniquely identify each gateway deployments.
	globalDeploymentID = env.Get("MINIO_GATEWAY_DEPLOYMENT_ID", mustGetUUID())
	logger.SetDeploymentID(globalDeploymentID)

	if gw == nil {
		logger.FatalIf(errUnexpected, "Gateway implementation not initialized")
	}

	// Validate if we have access, secret set through environment.
	globalGatewayName = gw.Name()
	gatewayName := gw.Name()
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, gatewayName, 1)
	}

	// Initialize globalConsoleSys system
	globalConsoleSys = NewConsoleLogger(GlobalContext)
	logger.AddTarget(globalConsoleSys)

	// Handle common command args.
	handleCommonCmdArgs(ctx)

	// Check and load TLS certificates.
	var err error
	globalPublicCerts, globalTLSCerts, globalIsSSL, err = getTLSConfig()
	logger.FatalIf(err, "Invalid TLS certificate file")

	// Check and load Root CAs.
	globalRootCAs, err = certs.GetRootCAs(globalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Add the global public crts as part of global root CAs
	for _, publicCrt := range globalPublicCerts {
		globalRootCAs.AddCert(publicCrt)
	}

	// Register root CAs for remote ENVs
	env.RegisterGlobalCAs(globalRootCAs)

	// Initialize all help
	initHelp()

	// Get port to listen on from gateway address
	globalMinioHost, globalMinioPort = mustSplitHostPort(globalCLIContext.Addr)

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(checkPortAvailability(globalMinioHost, globalMinioPort), "Unable to start the gateway")

	globalMinioEndpoint = func() string {
		host := globalMinioHost
		if host == "" {
			host = sortIPs(localIP4.ToSlice())[0]
		}
		return fmt.Sprintf("%s://%s", getURLScheme(globalIsSSL), net.JoinHostPort(host, globalMinioPort))
	}()

	// Handle gateway specific env
	gatewayHandleEnvVars()

	// Set system resources to maximum.
	setMaxResources()

	// Set when gateway is enabled
	globalIsGateway = true

	enableConfigOps := false

	// TODO: We need to move this code with globalConfigSys.Init()
	// for now keep it here such that "s3" gateway layer initializes
	// itself properly when KMS is set.

	// Initialize server config.
	srvCfg := newServerConfig()

	// Override any values from ENVs.
	lookupConfigs(srvCfg, 0)

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	// avoid URL path encoding minio/minio#8950
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	if globalEtcdClient != nil {
		// Enable STS router if etcd is enabled.
		registerSTSRouter(router)
	}

	enableIAMOps := globalEtcdClient != nil

	// Enable IAM admin APIs if etcd is enabled, if not just enable basic
	// operations such as profiling, server info etc.
	registerAdminRouter(router, enableConfigOps, enableIAMOps)

	// Add healthcheck router
	registerHealthCheckRouter(router)

	// Add server metrics router
	registerMetricsRouter(router)

	// Register web router when its enabled.
	if globalBrowserEnabled {
		logger.FatalIf(registerWebRouter(router), "Unable to configure web browser")
	}

	// Add API router.
	registerAPIRouter(router)

	// Use all the middlewares
	router.Use(registerMiddlewares)

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	httpServer := xhttp.NewServer([]string{globalCLIContext.Addr},
		criticalErrorHandler{corsHandler(router)}, getCert)
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return GlobalContext
	}
	go func() {
		globalHTTPServerErrorCh <- httpServer.Start()
	}()

	globalObjLayerMutex.Lock()
	globalHTTPServer = httpServer
	globalObjLayerMutex.Unlock()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	newObject, err := gw.NewGatewayLayer(globalActiveCred)
	if err != nil {
		globalHTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize gateway backend")
	}
	newObject = NewGatewayLayerWithLocker(newObject)

	// Once endpoints are finalized, initialize the new object api in safe mode.
	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Calls all New() for all sub-systems.
	newAllSubsystems()

	if gatewayName == NASBackendGateway {
		buckets, err := newObject.ListBuckets(GlobalContext)
		if err != nil {
			logger.Fatal(err, "Unable to list buckets")
		}
		logger.FatalIf(globalNotificationSys.Init(GlobalContext, buckets, newObject), "Unable to initialize notification system")
	}

	if globalEtcdClient != nil {
		// ****  WARNING ****
		// Migrating to encrypted backend on etcd should happen before initialization of
		// IAM sub-systems, make sure that we do not move the above codeblock elsewhere.
		logger.FatalIf(migrateIAMConfigsEtcdToEncrypted(GlobalContext, globalEtcdClient),
			"Unable to handle encrypted backend for iam and policies")
	}

	if enableIAMOps {
		// Initialize users credentials and policies in background.
		go globalIAMSys.Init(GlobalContext, newObject)
	}

	if globalCacheConfig.Enabled {
		// initialize the new disk cache objects.
		var cacheAPI CacheObjectLayer
		cacheAPI, err = newServerCacheObjects(GlobalContext, globalCacheConfig)
		logger.FatalIf(err, "Unable to initialize disk caching")

		globalObjLayerMutex.Lock()
		globalCacheObjectAPI = cacheAPI
		globalObjLayerMutex.Unlock()
	}

	// Populate existing buckets to the etcd backend
	if globalDNSConfig != nil {
		buckets, err := newObject.ListBuckets(GlobalContext)
		if err != nil {
			logger.Fatal(err, "Unable to list buckets")
		}
		initFederatorBackend(buckets, newObject)
	}

	// Verify if object layer supports
	// - encryption
	// - compression
	verifyObjectLayerFeatures("gateway "+gatewayName, newObject)

	// Prints the formatted startup message once object layer is initialized.
	if !globalCLIContext.Quiet {
		mode := globalMinioModeGatewayPrefix + gatewayName
		// Check update mode.
		checkUpdate(mode)

		// Print a warning message if gateway is not ready for production before the startup banner.
		if !gw.Production() {
			logStartupMessage(color.Yellow("               *** Warning: Not Ready for Production ***"))
		}

		// Print gateway startup message.
		printGatewayStartupMessage(getAPIEndpoints(), gatewayName)
	}

	handleSignals()
}
