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
	"encoding/gob"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/minio/cli"
	"github.com/minio/dsync/v2"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
)

func init() {
	logger.Init(GOPATH, GOROOT)
	logger.RegisterError(config.FmtError)
	gob.Register(VerifyFileError(""))
	gob.Register(DeleteFileError(""))
}

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
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Custom username or access key of minimum 3 characters in length.
     MINIO_SECRET_KEY: Custom password or secret key of minimum 8 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to MinIO host domain name.

  WORM:
     MINIO_WORM: To turn on Write-Once-Read-Many in server, set this value to "on".

  BUCKET-DNS:
     MINIO_DOMAIN:    To enable bucket DNS requests, set this value to MinIO host domain name.
     MINIO_PUBLIC_IPS: To enable bucket DNS requests, set this value to list of MinIO host public IP(s) delimited by ",".
     MINIO_ETCD_ENDPOINTS: To enable bucket DNS requests, set this value to list of etcd endpoints delimited by ",".

   KMS:
     MINIO_KMS_VAULT_ENDPOINT: To enable Vault as KMS,set this value to Vault endpoint.
     MINIO_KMS_VAULT_APPROLE_ID: To enable Vault as KMS,set this value to Vault AppRole ID.
     MINIO_KMS_VAULT_APPROLE_SECRET: To enable Vault as KMS,set this value to Vault AppRole Secret ID.
     MINIO_KMS_VAULT_KEY_NAME: To enable Vault as KMS,set this value to Vault encryption key-ring name.

EXAMPLES:
  1. Start minio server on "/home/shared" directory.
     {{.Prompt}} {{.HelpName}} /home/shared

  2. Start minio server bound to a specific ADDRESS:PORT.
     {{.Prompt}} {{.HelpName}} --address 192.168.1.101:9000 /home/shared

  3. Start minio server and enable virtual-host-style requests.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_DOMAIN{{.AssignmentOperator}}mydomain.com
     {{.Prompt}} {{.HelpName}} --address mydomain.com:9000 /mnt/export

  4. Start erasure coded minio server on a node with 64 drives.
     {{.Prompt}} {{.HelpName}} /mnt/export{1...64}

  5. Start distributed minio server on an 32 node setup with 32 drives each. Run following command on all the 32 nodes.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export/{1...32}

  6. Start minio server with KMS enabled.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_KMS_VAULT_APPROLE_ID{{.AssignmentOperator}}9b56cc08-8258-45d5-24a3-679876769126
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_KMS_VAULT_APPROLE_SECRET{{.AssignmentOperator}}4e30c52f-13e4-a6f5-0763-d50e8cb4321f
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_KMS_VAULT_ENDPOINT{{.AssignmentOperator}}https://vault-endpoint-ip:8200
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_KMS_VAULT_KEY_NAME{{.AssignmentOperator}}my-minio-key
     {{.Prompt}} {{.HelpName}} /home/shared
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

	if len(ctx.Args()) > serverCommandLineArgsMax {
		uErr := config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("Invalid total number of endpoints (%d) passed, supported upto 32 unique arguments",
			len(ctx.Args())))
		logger.FatalIf(uErr, "Unable to validate passed endpoints")
	}

	endpoints := strings.Fields(env.Get(config.EnvEndpoints, ""))
	if len(endpoints) > 0 {
		globalMinioAddr, globalEndpoints, setupType, globalXLSetCount, globalXLSetDriveCount, err = createServerEndpoints(globalCLIContext.Addr, endpoints...)
	} else {
		globalMinioAddr, globalEndpoints, setupType, globalXLSetCount, globalXLSetDriveCount, err = createServerEndpoints(globalCLIContext.Addr, ctx.Args()...)
	}
	logger.FatalIf(err, "Invalid command line arguments")

	logger.LogIf(context.Background(), checkEndpointsSubOptimal(ctx, setupType, globalEndpoints))

	globalMinioHost, globalMinioPort = mustSplitHostPort(globalMinioAddr)

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error sutiation we check for port availability.
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
}

func initSafeModeInit(buckets []BucketInfo) (err error) {
	defer func() {
		if err != nil {
			switch err.(type) {
			case config.Err:
				return
			}
			// Enable logger
			logger.Disable = false

			// Prints the formatted startup message in safe mode operation.
			printStartupSafeModeMessage(getAPIEndpoints(), err)

			// Initialization returned error reaching safe mode and
			// not proceeding waiting for admin action.
			handleSignals()
		}
	}()

	newObject := newObjectLayerWithoutSafeModeFn()

	// Calls New() for all sub-systems.
	newAllSubsystems()

	// Migrate all backend configs to encrypted backend, also handles rotation as well.
	if err = handleEncryptedConfigBackend(newObject, true); err != nil {
		return fmt.Errorf("Unable to handle encrypted backend for config, iam and policies: %v", err)
	}

	// ****  WARNING ****
	// Migrating to encrypted backend should happen before initialization of any
	// sub-systems, make sure that we do not move the above codeblock elsewhere.

	// Validate and initialize all subsystems.
	if err = initAllSubsystems(buckets, newObject); err != nil {
		return err
	}

	return nil
}

func initAllSubsystems(buckets []BucketInfo, newObject ObjectLayer) (err error) {
	// Initialize config system.
	if err = globalConfigSys.Init(newObject); err != nil {
		return fmt.Errorf("Unable to initialize config system: %w", err)
	}

	if err = globalNotificationSys.AddNotificationTargetsFromConfig(globalServerConfig); err != nil {
		return fmt.Errorf("Unable to initialize notification target(s) from config: %w", err)
	}

	if globalEtcdClient != nil {
		// ****  WARNING ****
		// Migrating to encrypted backend on etcd should happen before initialization of
		// IAM sub-systems, make sure that we do not move the above codeblock elsewhere.
		if err = migrateIAMConfigsEtcdToEncrypted(globalEtcdClient); err != nil {
			return fmt.Errorf("Unable to handle encrypted backend for iam and policies: %v", err)
		}
	}

	if err = globalIAMSys.Init(newObject); err != nil {
		return fmt.Errorf("Unable to initialize IAM system: %w", err)
	}

	// Initialize notification system.
	if err = globalNotificationSys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize notification system: %w", err)
	}

	// Initialize policy system.
	if err = globalPolicySys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize policy system; %w", err)
	}

	// Initialize lifecycle system.
	if err = globalLifecycleSys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize lifecycle system: %v", err)
	}

	return nil
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" || !endpointsPresent(ctx) {
		cli.ShowCommandHelpAndExit(ctx, "server", 1)
	}

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	// Disable logging until server initialization is complete, any
	// error during initialization will be shown as a fatal message
	logger.Disable = true

	// Handle all server command args.
	serverHandleCmdArgs(ctx)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Check and load TLS certificates.
	var err error
	globalPublicCerts, globalTLSCerts, globalIsSSL, err = getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	globalRootCAs, err = config.GetRootCAs(globalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistXL {
		if globalEndpoints.IsHTTPS() && !globalIsSSL {
			logger.Fatal(config.ErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.IsHTTPS() && globalIsSSL {
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
	logger.LogIf(context.Background(), setMaxResources())

	// Set nodes for dsync for distributed setup.
	if globalIsDistXL {
		clnts, myNode, err := newDsyncNodes(globalEndpoints)
		if err != nil {
			logger.Fatal(err, "Unable to initialize distributed locking on %s", globalEndpoints)
		}
		globalDsync, err = dsync.New(clnts, myNode)
		if err != nil {
			logger.Fatal(err, "Unable to initialize distributed locking on %s", globalEndpoints)
		}
	}

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	if globalIsXL {
		// Init global heal state
		globalAllHealState = initHealState()
		globalBackgroundHealState = initHealState()
	}

	// Initialize globalConsoleSys system
	globalConsoleSys = NewConsoleLogger(context.Background(), globalEndpoints)

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

	globalHTTPServer = xhttp.NewServer([]string{globalMinioAddr}, criticalErrorHandler{handler}, getCert)
	go func() {
		globalHTTPServerErrorCh <- globalHTTPServer.Start()
	}()

	newObject, err := newObjectLayer(globalEndpoints)
	logger.SetDeploymentID(globalDeploymentID)
	if err != nil {
		// Stop watching for any certificate changes.
		globalTLSCerts.Stop()

		globalHTTPServer.Shutdown()
		logger.Fatal(err, "Unable to initialize backend")
	}

	// Re-enable logging
	logger.Disable = false

	// Once endpoints are finalized, initialize the new object api in safe mode.
	globalObjLayerMutex.Lock()
	globalSafeMode = true
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	buckets, err := newObject.ListBuckets(context.Background())
	if err != nil {
		logger.Fatal(err, "Unable to list buckets")
	}

	// Populate existing buckets to the etcd backend
	if globalDNSConfig != nil {
		initFederatorBackend(buckets, newObject)
	}

	initSafeModeInit(buckets)

	if globalCacheConfig.Enabled {
		msg := color.RedBold("Disk caching is disabled in 'server' mode, 'caching' is only supported in gateway deployments")
		logger.StartupMessage(msg)
	}

	initDailyLifecycle()

	if globalIsXL {
		initBackgroundHealing()
		initLocalDisksAutoHeal()
		initGlobalHeal()
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

	// Set uptime time after object layer has initialized.
	globalBootTime = UTCNow()

	handleSignals()
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(endpoints EndpointList) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.

	isFS := len(endpoints) == 1
	if isFS {
		// Initialize new FS object layer.
		return NewFSObjectLayer(endpoints[0].Path)
	}

	format, err := waitForFormatXL(endpoints[0].IsLocal, endpoints, globalXLSetCount, globalXLSetDriveCount)
	if err != nil {
		return nil, err
	}

	return newXLSets(endpoints, format, len(format.XL.Sets), len(format.XL.Sets[0]))
}
