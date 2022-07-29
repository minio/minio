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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/minio/cli"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/fips"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/rest"
	"github.com/minio/pkg/certs"
	"github.com/minio/pkg/env"
)

// ServerFlags - server command specific flags
var ServerFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "address",
		Value:  ":" + GlobalMinioDefaultPort,
		Usage:  "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
		EnvVar: "MINIO_ADDRESS",
	},
	cli.IntFlag{
		Name:   "listeners",
		Value:  1,
		Usage:  "bind N number of listeners per ADDRESS:PORT",
		EnvVar: "MINIO_LISTENERS",
	},
	cli.StringFlag{
		Name:   "console-address",
		Usage:  "bind to a specific ADDRESS:PORT for embedded Console UI, ADDRESS can be an IP or hostname",
		EnvVar: "MINIO_CONSOLE_ADDRESS",
	},
	cli.DurationFlag{
		Name:   "shutdown-timeout",
		Value:  xhttp.DefaultShutdownTimeout,
		Usage:  "shutdown timeout to gracefully shutdown server",
		EnvVar: "MINIO_SHUTDOWN_TIMEOUT",
		Hidden: true,
	},
	cli.DurationFlag{
		Name:   "idle-timeout",
		Value:  xhttp.DefaultIdleTimeout,
		Usage:  "idle timeout is the maximum amount of time to wait for the next request when keep-alives are enabled",
		EnvVar: "MINIO_IDLE_TIMEOUT",
		Hidden: true,
	},
	cli.DurationFlag{
		Name:   "read-header-timeout",
		Value:  xhttp.DefaultReadHeaderTimeout,
		Usage:  "read header timeout is the amount of time allowed to read request headers",
		EnvVar: "MINIO_READ_HEADER_TIMEOUT",
		Hidden: true,
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "start object storage server",
	Flags:  append(ServerFlags, GlobalFlags...),
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
  1. Start minio server on "/home/shared" directory.
     {{.Prompt}} {{.HelpName}} /home/shared

  2. Start single node server with 64 local drives "/mnt/data1" to "/mnt/data64".
     {{.Prompt}} {{.HelpName}} /mnt/data{1...64}

  3. Start distributed minio server on an 32 node setup with 32 drives each, run following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export{1...32}

  4. Start distributed minio server in an expanded setup, run the following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...16}.example.com/mnt/export{1...32} \
            http://node{17...64}.example.com/mnt/export{1...64}
`,
}

func serverCmdArgs(ctx *cli.Context) []string {
	v, _, _, err := env.LookupEnv(config.EnvArgs)
	if err != nil {
		logger.FatalIf(err, "Unable to validate passed arguments in %s:%s",
			config.EnvArgs, os.Getenv(config.EnvArgs))
	}
	if v == "" {
		v, _, _, err = env.LookupEnv(config.EnvVolumes)
		if err != nil {
			logger.FatalIf(err, "Unable to validate passed arguments in %s:%s",
				config.EnvVolumes, os.Getenv(config.EnvVolumes))
		}
	}
	if v == "" {
		// Fall back to older environment value MINIO_ENDPOINTS
		v, _, _, err = env.LookupEnv(config.EnvEndpoints)
		if err != nil {
			logger.FatalIf(err, "Unable to validate passed arguments in %s:%s",
				config.EnvEndpoints, os.Getenv(config.EnvEndpoints))
		}
	}
	if v == "" {
		if !ctx.Args().Present() || ctx.Args().First() == "help" {
			cli.ShowCommandHelpAndExit(ctx, ctx.Command.Name, 1)
		}
		return ctx.Args()
	}
	return strings.Fields(v)
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Handle common command args.
	handleCommonCmdArgs(ctx)

	logger.FatalIf(CheckLocalServerAddr(globalMinioAddr), "Unable to validate passed arguments")

	var err error
	var setupType SetupType

	// Check and load TLS certificates.
	globalPublicCerts, globalTLSCerts, globalIsTLS, err = getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	globalRootCAs, err = certs.GetRootCAs(globalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Add the global public crts as part of global root CAs
	for _, publicCrt := range globalPublicCerts {
		globalRootCAs.AddCert(publicCrt)
	}

	// Register root CAs for remote ENVs
	env.RegisterGlobalCAs(globalRootCAs)

	globalEndpoints, setupType, err = createServerEndpoints(globalMinioAddr, serverCmdArgs(ctx)...)
	logger.FatalIf(err, "Invalid command line arguments")

	globalLocalNodeName = GetLocalPeer(globalEndpoints, globalMinioHost, globalMinioPort)

	globalRemoteEndpoints = make(map[string]Endpoint)
	for _, z := range globalEndpoints {
		for _, ep := range z.Endpoints {
			if ep.IsLocal {
				globalRemoteEndpoints[globalLocalNodeName] = ep
			} else {
				globalRemoteEndpoints[ep.Host] = ep
			}
		}
	}

	// allow transport to be HTTP/1.1 for proxying.
	globalProxyTransport = newCustomHTTPProxyTransport(&tls.Config{
		RootCAs:            globalRootCAs,
		CipherSuites:       fips.TLSCiphers(),
		CurvePreferences:   fips.TLSCurveIDs(),
		ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
	}, rest.DefaultTimeout)()
	globalProxyEndpoints = GetProxyEndpoints(globalEndpoints)
	globalInternodeTransport = newInternodeHTTPTransport(&tls.Config{
		RootCAs:            globalRootCAs,
		CipherSuites:       fips.TLSCiphers(),
		CurvePreferences:   fips.TLSCurveIDs(),
		ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
	}, rest.DefaultTimeout)()
	globalRemoteTargetTransport = NewRemoteTargetHTTPTransport()()

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(checkPortAvailability(globalMinioHost, globalMinioPort), "Unable to start the server")

	globalIsErasure = (setupType == ErasureSetupType)
	globalIsDistErasure = (setupType == DistErasureSetupType)
	if globalIsDistErasure {
		globalIsErasure = true
	}
	globalIsErasureSD = (setupType == ErasureSDSetupType)
}

func serverHandleEnvVars() {
	// Handle common environment variables.
	handleCommonEnvVars()
}

var globalHealStateLK sync.RWMutex

func initAllSubsystems() {
	globalHealStateLK.Lock()
	// New global heal state
	globalAllHealState = newHealState(true)
	globalBackgroundHealState = newHealState(false)
	globalHealStateLK.Unlock()

	// Create new notification system and initialize notification peer targets
	globalNotificationSys = NewNotificationSys(globalEndpoints)

	// Create new bucket metadata system.
	if globalBucketMetadataSys == nil {
		globalBucketMetadataSys = NewBucketMetadataSys()
	} else {
		// Reinitialize safely when testing.
		globalBucketMetadataSys.Reset()
	}

	// Create the bucket bandwidth monitor
	globalBucketMonitor = bandwidth.NewMonitor(GlobalContext, totalNodeCount())

	// Create a new config system.
	globalConfigSys = NewConfigSys()

	// Create new IAM system.
	globalIAMSys = NewIAMSys()

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Create new lifecycle system.
	globalLifecycleSys = NewLifecycleSys()

	// Create new bucket encryption subsystem
	globalBucketSSEConfigSys = NewBucketSSEConfigSys()

	// Create new bucket object lock subsystem
	globalBucketObjectLockSys = NewBucketObjectLockSys()

	// Create new bucket quota subsystem
	globalBucketQuotaSys = NewBucketQuotaSys()

	// Create new bucket versioning subsystem
	if globalBucketVersioningSys == nil {
		globalBucketVersioningSys = NewBucketVersioningSys()
	}

	// Create new bucket replication subsytem
	globalBucketTargetSys = NewBucketTargetSys()

	// Create new ILM tier configuration subsystem
	globalTierConfigMgr = NewTierConfigMgr()
}

func configRetriableErrors(err error) bool {
	// Initializing sub-systems needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	//  - Write quorum not met when upgrading configuration
	//    version is needed, migration is needed etc.
	rquorum := InsufficientReadQuorum{}
	wquorum := InsufficientWriteQuorum{}

	// One of these retriable errors shall be retried.
	return errors.Is(err, errDiskNotFound) ||
		errors.Is(err, errConfigNotFound) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errErasureWriteQuorum) ||
		errors.Is(err, errErasureReadQuorum) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.As(err, &rquorum) ||
		errors.As(err, &wquorum) ||
		isErrObjectNotFound(err) ||
		isErrBucketNotFound(err) ||
		errors.Is(err, os.ErrDeadlineExceeded)
}

func initServer(ctx context.Context, newObject ObjectLayer) error {
	t1 := time.Now()

	// Once the config is fully loaded, initialize the new object layer.
	setObjectLayer(newObject)

	// ****  WARNING ****
	// Migrating to encrypted backend should happen before initialization of any
	// sub-systems, make sure that we do not move the above codeblock elsewhere.

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	lockTimeout := newDynamicTimeout(5*time.Second, 3*time.Second)

	for {
		select {
		case <-ctx.Done():
			// Retry was canceled successfully.
			return fmt.Errorf("Initializing sub-systems stopped gracefully %w", ctx.Err())
		default:
		}

		// Make sure to hold lock for entire migration to avoid
		// such that only one server should migrate the entire config
		// at a given time, this big transaction lock ensures this
		// appropriately. This is also true for rotation of encrypted
		// content.
		txnLk := newObject.NewNSLock(minioMetaBucket, minioConfigPrefix+"/transaction.lock")

		// let one of the server acquire the lock, if not let them timeout.
		// which shall be retried again by this loop.
		lkctx, err := txnLk.GetLock(ctx, lockTimeout)
		if err != nil {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. trying to acquire lock")

			time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
			continue
		}

		// These messages only meant primarily for distributed setup, so only log during distributed setup.
		if globalIsDistErasure {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. lock acquired")
		}

		// Migrate all backend configs to encrypted backend configs, optionally
		// handles rotating keys for encryption, if there is any retriable failure
		// that shall be retried if there is an error.
		if err = handleEncryptedConfigBackend(newObject); err == nil {
			// Upon success migrating the config, initialize all sub-systems
			// if all sub-systems initialized successfully return right away
			if err = initConfigSubsystem(lkctx.Context(), newObject); err == nil {
				txnLk.Unlock(lkctx.Cancel)
				// All successful return.
				if globalIsDistErasure {
					// These messages only meant primarily for distributed setup, so only log during distributed setup.
					logger.Info("All MinIO sub-systems initialized successfully in %s", time.Since(t1))
				}
				return nil
			}
		}

		// Unlock the transaction lock and allow other nodes to acquire the lock if possible.
		txnLk.Unlock(lkctx.Cancel)

		if configRetriableErrors(err) {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. possible cause (%v)", err)
			time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
			continue
		}

		// Any other unhandled return right here.
		return fmt.Errorf("Unable to initialize sub-systems: %w", err)
	}
}

func initConfigSubsystem(ctx context.Context, newObject ObjectLayer) error {
	// %w is used by all error returns here to make sure
	// we wrap the underlying error, make sure when you
	// are modifying this code that you do so, if and when
	// you want to add extra context to your error. This
	// ensures top level retry works accordingly.

	// Initialize config system.
	if err := globalConfigSys.Init(newObject); err != nil {
		if configRetriableErrors(err) {
			return fmt.Errorf("Unable to initialize config system: %w", err)
		}

		// Any other config errors we simply print a message and proceed forward.
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize config, some features may be missing %w", err))
	}

	return nil
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go handleSignals()

	setDefaultProfilerRates()

	// Initialize globalConsoleSys system
	globalConsoleSys = NewConsoleLogger(GlobalContext)
	logger.AddSystemTarget(globalConsoleSys)

	// Perform any self-tests
	bitrotSelfTest()
	erasureSelfTest()
	compressSelfTest()

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Handle all server command args.
	serverHandleCmdArgs(ctx)

	// Initialize KMS configuration
	handleKMSConfig()

	// Set node name, only set for distributed setup.
	globalConsoleSys.SetNodeName(globalLocalNodeName)

	// Initialize all help
	initHelp()

	// Initialize all sub-systems
	initAllSubsystems()

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistErasure {
		if globalEndpoints.HTTPS() && !globalIsTLS {
			logger.Fatal(config.ErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.HTTPS() && globalIsTLS {
			logger.Fatal(config.ErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	// Check for updates in non-blocking manner.
	go func() {
		if !globalCLIContext.Quiet && !globalInplaceUpdateDisabled {
			// Check for new updates from dl.min.io.
			checkUpdate(getMinioMode())
		}
	}()

	if !globalActiveCred.IsValid() && globalIsDistErasure {
		globalActiveCred = auth.DefaultCredentials
	}

	// Set system resources to maximum.
	setMaxResources()

	// Verify kernel release and version.
	if oldLinux() {
		logger.Info(color.RedBold("WARNING: Detected Linux kernel version older than 4.0.0 release, there are some known potential performance problems with this kernel version. MinIO recommends a minimum of 4.x.x linux kernel version for best performance"))
	}

	maxProcs := runtime.GOMAXPROCS(0)
	cpuProcs := runtime.NumCPU()
	if maxProcs < cpuProcs {
		logger.Info(color.RedBold("WARNING: Detected GOMAXPROCS(%d) < NumCPU(%d), please make sure to provide all PROCS to MinIO for optimal performance", maxProcs, cpuProcs))
	}

	// Configure server.
	handler, err := configureServerHandler(globalEndpoints)
	if err != nil {
		logger.Fatal(config.ErrUnexpectedError(err), "Unable to configure one of server's RPC services")
	}

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	listeners := ctx.Int("listeners")
	if listeners == 0 {
		listeners = 1
	}
	addrs := make([]string, 0, listeners)
	for i := 0; i < listeners; i++ {
		addrs = append(addrs, globalMinioAddr)
	}

	httpServer := xhttp.NewServer(addrs).
		UseHandler(setCriticalErrorHandler(corsHandler(handler))).
		UseTLSConfig(newTLSConfig(getCert)).
		UseShutdownTimeout(ctx.Duration("shutdown-timeout")).
		UseIdleTimeout(ctx.Duration("idle-timeout")).
		UseReadHeaderTimeout(ctx.Duration("read-header-timeout")).
		UseBaseContext(GlobalContext).
		UseCustomLogger(log.New(ioutil.Discard, "", 0)) // Turn-off random logging by Go stdlib

	go func() {
		globalHTTPServerErrorCh <- httpServer.Start(GlobalContext)
	}()

	setHTTPServer(httpServer)

	if globalIsDistErasure && globalEndpoints.FirstLocal() {
		// Additionally in distributed setup, validate the setup and configuration.
		if err := verifyServerSystemConfig(GlobalContext, globalEndpoints); err != nil {
			logger.Fatal(err, "Unable to start the server")
		}
	}

	newObject, err := newObjectLayer(GlobalContext, globalEndpoints)
	if err != nil {
		logFatalErrs(err, Endpoint{}, true)
	}

	xhttp.SetDeploymentID(globalDeploymentID)
	xhttp.SetMinIOVersion(Version)

	// Enable background operations for erasure coding
	initAutoHeal(GlobalContext, newObject)
	initHealMRF(GlobalContext, newObject)
	initBackgroundExpiry(GlobalContext, newObject)

	if globalActiveCred.Equal(auth.DefaultCredentials) {
		msg := fmt.Sprintf("WARNING: Detected default credentials '%s', we recommend that you change these values with 'MINIO_ROOT_USER' and 'MINIO_ROOT_PASSWORD' environment variables",
			globalActiveCred)
		logger.Info(color.RedBold(msg))
	}

	if !globalCLIContext.StrictS3Compat {
		logger.Info(color.RedBold("WARNING: Strict AWS S3 compatible incoming PUT, POST content payload validation is turned off, caution is advised do not use in production"))
	}

	if err = initServer(GlobalContext, newObject); err != nil {
		var cerr config.Err
		// For any config error, we don't need to drop into safe-mode
		// instead its a user error and should be fixed by user.
		if errors.As(err, &cerr) {
			logger.FatalIf(err, "Unable to initialize the server")
		}

		// If context was canceled
		if errors.Is(err, context.Canceled) {
			logger.FatalIf(err, "Server startup canceled upon user request")
		}

		logger.LogIf(GlobalContext, err)
	}

	if globalBrowserEnabled {
		srv, err := initConsoleServer()
		if err != nil {
			logger.FatalIf(err, "Unable to initialize console service")
		}

		setConsoleSrv(srv)

		go func() {
			logger.FatalIf(newConsoleServerFn().Serve(), "Unable to initialize console server")
		}()
	}

	// Initialize users credentials and policies in background right after config has initialized.
	go globalIAMSys.Init(GlobalContext, newObject, globalEtcdClient, globalRefreshIAMInterval)

	// Background all other operations such as initializing bucket metadata etc.
	go func() {
		// Initialize transition tier configuration manager
		initBackgroundReplication(GlobalContext, newObject)
		initBackgroundTransition(GlobalContext, newObject)

		go func() {
			err := globalTierConfigMgr.Init(GlobalContext, newObject)
			if err != nil {
				logger.LogIf(GlobalContext, err)
			}

			globalTierJournal, err = initTierDeletionJournal(GlobalContext)
			if err != nil {
				logger.FatalIf(err, "Unable to initialize remote tier pending deletes journal")
			}
		}()

		// Initialize quota manager.
		globalBucketQuotaSys.Init(newObject)

		initDataScanner(GlobalContext, newObject)

		// List buckets to heal, and be re-used for loading configs.
		buckets, err := newObject.ListBuckets(GlobalContext, BucketOptions{})
		if err != nil {
			logger.LogIf(GlobalContext, fmt.Errorf("Unable to list buckets to heal: %w", err))
		}
		// initialize replication resync state.
		go globalReplicationPool.initResync(GlobalContext, buckets, newObject)

		// Populate existing buckets to the etcd backend
		if globalDNSConfig != nil {
			// Background this operation.
			go initFederatorBackend(buckets, newObject)
		}

		// Initialize bucket metadata sub-system.
		globalBucketMetadataSys.Init(GlobalContext, buckets, newObject)

		// Initialize site replication manager.
		globalSiteReplicationSys.Init(GlobalContext, newObject)

		// Initialize bucket notification targets.
		globalNotificationSys.InitBucketTargets(GlobalContext, newObject)

		// initialize the new disk cache objects.
		if globalCacheConfig.Enabled {
			logger.Info(color.Yellow("WARNING: Disk caching is deprecated for single/multi drive MinIO setups. Please migrate to using MinIO S3 gateway instead of disk caching"))
			var cacheAPI CacheObjectLayer
			cacheAPI, err = newServerCacheObjects(GlobalContext, globalCacheConfig)
			logger.FatalIf(err, "Unable to initialize disk caching")

			setCacheObjectLayer(cacheAPI)
		}

		// Prints the formatted startup message, if err is not nil then it prints additional information as well.
		printStartupMessage(getAPIEndpoints(), err)
	}()

	region := globalSite.Region
	if region == "" {
		region = "us-east-1"
	}
	globalMinioClient, err = minio.New(globalLocalNodeName, &minio.Options{
		Creds:     credentials.NewStaticV4(globalActiveCred.AccessKey, globalActiveCred.SecretKey, ""),
		Secure:    globalIsTLS,
		Transport: globalProxyTransport,
		Region:    region,
	})
	logger.FatalIf(err, "Unable to initialize MinIO client")

	if serverDebugLog {
		logger.Info("== DEBUG Mode enabled ==")
		logger.Info("Currently set environment settings:")
		ks := []string{
			config.EnvAccessKey,
			config.EnvSecretKey,
			config.EnvRootUser,
			config.EnvRootPassword,
		}
		for _, v := range os.Environ() {
			// Do not print sensitive creds in debug.
			if contains(ks, strings.Split(v, "=")[0]) {
				continue
			}
			logger.Info(v)
		}
		logger.Info("======")
	}

	<-globalOSSignalCh
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(ctx context.Context, endpointServerPools EndpointServerPools) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.
	if endpointServerPools.NEndpoints() == 1 {
		// Initialize new FS object layer.
		newObject, err = NewFSObjectLayer(endpointServerPools[0].Endpoints[0].Path)
		if err == nil {
			return newObject, nil
		}
		if err != nil && err != errFreshDisk {
			return newObject, err
		}
	}

	return newErasureServerPools(ctx, endpointServerPools)
}
