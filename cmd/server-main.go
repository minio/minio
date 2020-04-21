/*
 * MinIO Cloud Storage, (C) 2015-2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
)

// ServerFlags - server command specific flags
var ServerFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":" + globalMinioDefaultPort,
		Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
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
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export{1...32}

  4. Start distributed minio server in an expanded setup, run the following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...16}.example.com/mnt/export{1...32} \
            http://node{17...64}.example.com/mnt/export{1...64}

`,
}

// Checks if endpoints are either available through environment
// or command line, returns false if both fails.
func endpointsPresent(ctx *cli.Context) bool {
	endpoints := env.Get(config.EnvEndpoints, strings.Join(ctx.Args(), config.ValueSeparator))
	return len(endpoints) != 0
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Handle common command args.
	handleCommonCmdArgs(ctx)

	logger.FatalIf(CheckLocalServerAddr(globalCLIContext.Addr), "Unable to validate passed arguments")

	var setupType SetupType
	var err error

	globalMinioAddr = globalCLIContext.Addr

	globalMinioHost, globalMinioPort = mustSplitHostPort(globalMinioAddr)

	endpoints := strings.Fields(env.Get(config.EnvEndpoints, ""))
	if len(endpoints) > 0 {
		globalEndpoints, globalXLSetDriveCount, setupType, err = createServerEndpoints(globalCLIContext.Addr, endpoints...)
	} else {
		globalEndpoints, globalXLSetDriveCount, setupType, err = createServerEndpoints(globalCLIContext.Addr, ctx.Args()...)
	}
	logger.FatalIf(err, "Invalid command line arguments")

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(checkPortAvailability(globalMinioHost, globalMinioPort), "Unable to start the server")

	globalIsXL = (setupType == XLSetupType)
	globalIsDistXL = (setupType == DistXLSetupType)
	if globalIsDistXL {
		globalIsXL = true
	}
}

func serverHandleEnvVars() {
	// Handle common environment variables.
	handleCommonEnvVars()
}

func newAllSubsystems() {
	// Create new notification system and initialize notification targets
	globalNotificationSys = NewNotificationSys(globalEndpoints)

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
}

func initSafeMode(buckets []BucketInfo) (err error) {
	newObject := newObjectLayerWithoutSafeModeFn()

	// Construct path to config/transaction.lock for locking
	transactionConfigPrefix := minioConfigPrefix + "/transaction.lock"

	// Make sure to hold lock for entire migration to avoid
	// such that only one server should migrate the entire config
	// at a given time, this big transaction lock ensures this
	// appropriately. This is also true for rotation of encrypted
	// content.
	objLock := newObject.NewNSLock(GlobalContext, minioMetaBucket, transactionConfigPrefix)
	if err = objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}

	defer func(objLock RWLocker) {
		objLock.Unlock()

		if err != nil {
			var cerr config.Err
			if errors.As(err, &cerr) {
				return
			}

			// Prints the formatted startup message in safe mode operation.
			printStartupSafeModeMessage(getAPIEndpoints(), err)

			// Initialization returned error reaching safe mode and
			// not proceeding waiting for admin action.
			handleSignals()
		}
	}(objLock)

	// Migrate all backend configs to encrypted backend configs, optionally
	// handles rotating keys for encryption.
	if err = handleEncryptedConfigBackend(newObject, true); err != nil {
		return fmt.Errorf("Unable to handle encrypted backend for config, iam and policies: %w", err)
	}

	// ****  WARNING ****
	// Migrating to encrypted backend should happen before initialization of any
	// sub-systems, make sure that we do not move the above codeblock elsewhere.

	// Validate and initialize all subsystems.
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing sub-systems needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	//  - Write quorum not met when upgrading configuration
	//    version is needed, migration is needed etc.
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		rquorum := InsufficientReadQuorum{}
		wquorum := InsufficientWriteQuorum{}
		bucketNotFound := BucketNotFound{}
		var err error
		select {
		case n := <-retryTimerCh:
			if err = initAllSubsystems(buckets, newObject); err != nil {
				if errors.Is(err, errDiskNotFound) ||
					errors.As(err, &rquorum) ||
					errors.As(err, &wquorum) ||
					errors.As(err, &bucketNotFound) {
					if n < 5 {
						logger.Info("Waiting for all sub-systems to be initialized..")
					} else {
						logger.Info("Waiting for all sub-systems to be initialized.. %v", err)
					}
					continue
				}
				return err
			}
			return nil
		case <-globalOSSignalCh:
			if err == nil {
				return errors.New("Initializing sub-systems stopped gracefully")
			}
			return fmt.Errorf("Unable to initialize sub-systems: %w", err)
		}
	}
}

func initAllSubsystems(buckets []BucketInfo, newObject ObjectLayer) (err error) {
	// Initialize config system.
	if err = globalConfigSys.Init(newObject); err != nil {
		return fmt.Errorf("Unable to initialize config system: %w", err)
	}
	if globalEtcdClient != nil {
		// ****  WARNING ****
		// Migrating to encrypted backend on etcd should happen before initialization of
		// IAM sub-systems, make sure that we do not move the above codeblock elsewhere.
		if err = migrateIAMConfigsEtcdToEncrypted(GlobalContext, globalEtcdClient); err != nil {
			return fmt.Errorf("Unable to handle encrypted backend for iam and policies: %w", err)
		}
	}

	if err = globalIAMSys.Init(GlobalContext, newObject); err != nil {
		return fmt.Errorf("Unable to initialize IAM system: %w", err)
	}

	// Initialize notification system.
	if err = globalNotificationSys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize notification system: %w", err)
	}

	// Initialize policy system.
	if err = globalPolicySys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize policy system: %w", err)
	}

	// Initialize bucket object lock.
	if err = initBucketObjectLockConfig(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize object lock system: %w", err)
	}

	// Initialize lifecycle system.
	if err = globalLifecycleSys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize lifecycle system: %w", err)
	}

	// Initialize bucket encryption subsystem.
	if err = globalBucketSSEConfigSys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize bucket encryption subsystem: %w", err)
	}

	return nil
}

func startBackgroundOps(ctx context.Context, objAPI ObjectLayer) {
	// Make sure only 1 crawler is running on the cluster.
	locker := objAPI.NewNSLock(ctx, minioMetaBucket, "leader")
	for {
		err := locker.GetLock(leaderLockTimeout)
		if err != nil {
			time.Sleep(leaderTick)
			continue
		}
		break
		// No unlock for "leader" lock.
	}

	if globalIsXL {
		initGlobalHeal(ctx, objAPI)
	}

	initDataUsageStats(ctx, objAPI)
	initDailyLifecycle(ctx, objAPI)
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" || !endpointsPresent(ctx) {
		cli.ShowCommandHelpAndExit(ctx, "server", 1)
	}
	setDefaultProfilerRates()

	// Initialize globalConsoleSys system
	globalConsoleSys = NewConsoleLogger(GlobalContext)

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	// Handle all server command args.
	serverHandleCmdArgs(ctx)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Set node name, only set for distributed setup.
	globalConsoleSys.SetNodeName(globalEndpoints)

	// Initialize all help
	initHelp()

	// Check and load TLS certificates.
	var err error
	globalPublicCerts, globalTLSCerts, globalIsSSL, err = getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	globalRootCAs, err = config.GetRootCAs(globalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistXL {
		if globalEndpoints.HTTPS() && !globalIsSSL {
			logger.Fatal(config.ErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.HTTPS() && globalIsSSL {
			logger.Fatal(config.ErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	if !globalCLIContext.Quiet {
		// Check for new updates from dl.min.io.
		checkUpdate(getMinioMode())
	}

	if !globalActiveCred.IsValid() && globalIsDistXL {
		logger.Fatal(config.ErrEnvCredentialsMissingDistributed(nil),
			"Unable to initialize the server in distributed mode")
	}

	// Set system resources to maximum.
	if err = setMaxResources(); err != nil {
		logger.Info("Unable to set system resources to maximum %s", err)
	}

	if globalIsXL {
		// Init global heal state
		globalAllHealState = initHealState()
		globalBackgroundHealState = initHealState()
	}

	// Configure server.
	var handler http.Handler
	handler, err = configureServerHandler(globalEndpoints)
	if err != nil {
		logger.Fatal(config.ErrUnexpectedError(err), "Unable to configure one of server's RPC services")
	}

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	httpServer := xhttp.NewServer([]string{globalMinioAddr}, criticalErrorHandler{handler}, getCert)
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return GlobalContext
	}
	go func() {
		globalHTTPServerErrorCh <- httpServer.Start()
	}()

	globalObjLayerMutex.Lock()
	globalHTTPServer = httpServer
	globalObjLayerMutex.Unlock()

	if globalIsDistXL && globalEndpoints.FirstLocal() {
		for {
			// Additionally in distributed setup, validate the setup and configuration.
			err := verifyServerSystemConfig(globalEndpoints)
			if err == nil {
				break
			}
			logger.LogIf(GlobalContext, err, "Unable to initialize distributed setup")
			select {
			case <-GlobalContext.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}

	newObject, err := newObjectLayer(GlobalContext, globalEndpoints)
	logger.SetDeploymentID(globalDeploymentID)
	if err != nil {
		// Stop watching for any certificate changes.
		globalTLSCerts.Stop()

		globalHTTPServer.Shutdown()
		logger.Fatal(err, "Unable to initialize backend")
	}

	// Once endpoints are finalized, initialize the new object api in safe mode.
	globalObjLayerMutex.Lock()
	globalSafeMode = true
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	newAllSubsystems()

	// Enable healing to heal drives if possible
	if globalIsXL {
		initBackgroundHealing(GlobalContext, newObject)
		initLocalDisksAutoHeal(GlobalContext, newObject)
	}

	go startBackgroundOps(GlobalContext, newObject)

	// Calls New() and initializes all sub-systems.
	buckets, err := newObject.ListBuckets(GlobalContext)
	if err != nil {
		logger.Fatal(err, "Unable to list buckets")
	}

	logger.FatalIf(initSafeMode(buckets), "Unable to initialize server switching into safe-mode")

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
		initFederatorBackend(buckets, newObject)
	}

	// Disable safe mode operation, after all initialization is over.
	globalObjLayerMutex.Lock()
	globalSafeMode = false
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(getAPIEndpoints())

	if globalActiveCred.Equal(auth.DefaultCredentials) {
		msg := fmt.Sprintf("Detected default credentials '%s', please change the credentials immediately using 'MINIO_ACCESS_KEY' and 'MINIO_SECRET_KEY'", globalActiveCred)
		logger.StartupMessage(color.RedBold(msg))
	}

	handleSignals()
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(ctx context.Context, endpointZones EndpointZones) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.

	if endpointZones.NEndpoints() == 1 {
		// Initialize new FS object layer.
		return NewFSObjectLayer(endpointZones[0].Endpoints[0].Path)
	}

	return newXLZones(ctx, endpointZones)
}
