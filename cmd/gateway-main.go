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
	"github.com/minio/madmin-go"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/certs"
	"github.com/minio/pkg/env"
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
func (l *GatewayLocker) NewNSLock(bucket string, objects ...string) RWLocker {
	return l.nsMutex.NewNSLock(nil, bucket, objects...)
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
	defer globalDNSCache.Stop()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go handleSignals()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
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
	globalPublicCerts, globalTLSCerts, globalIsTLS, err = getTLSConfig()
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
		return fmt.Sprintf("%s://%s", getURLScheme(globalIsTLS), net.JoinHostPort(host, globalMinioPort))
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
	lookupConfigs(srvCfg, nil)

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

	// Add API router.
	registerAPIRouter(router)

	// Use all the middlewares
	router.Use(globalHandlers...)

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	httpServer := xhttp.NewServer([]string{globalMinioAddr},
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

	newObject, err := gw.NewGatewayLayer(madmin.Credentials{
		AccessKey: globalActiveCred.AccessKey,
		SecretKey: globalActiveCred.SecretKey,
	})
	if err != nil {
		globalHTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize gateway backend")
	}
	newObject = NewGatewayLayerWithLocker(newObject)

	// Calls all New() for all sub-systems.
	newAllSubsystems()

	// Once endpoints are finalized, initialize the new object api in safe mode.
	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	if gatewayName == NASBackendGateway {
		buckets, err := newObject.ListBuckets(GlobalContext)
		if err != nil {
			logger.Fatal(err, "Unable to list buckets")
		}
		logger.FatalIf(globalNotificationSys.Init(GlobalContext, buckets, newObject), "Unable to initialize notification system")
	}

	if enableIAMOps {
		// Initialize users credentials and policies in background.
		globalIAMSys.InitStore(newObject)

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
	if !globalCLIContext.Quiet && !globalInplaceUpdateDisabled {
		// Check update mode.
		checkUpdate(globalMinioModeGatewayPrefix + gatewayName)
	}

	if !globalCLIContext.Quiet {
		// Print gateway startup message.
		printGatewayStartupMessage(getAPIEndpoints(), gatewayName)
	}

	if globalBrowserEnabled {
		consoleSrv, err := initConsoleServer()
		if err != nil {
			logger.FatalIf(err, "Unable to initialize console service")
		}

		go func() {
			<-globalOSSignalCh
			consoleSrv.Shutdown()
		}()

		consoleSrv.Serve()
	} else {
		<-globalOSSignalCh
	}
}
