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
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/minio/cli"
	"github.com/minio/dsync"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/certs"
)

func init() {
	logger.Init(GOPATH, GOROOT)
	logger.RegisterUIError(fmtError)
}

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":" + globalMinioDefaultPort,
		Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "start object storage server",
	Flags:  append(serverFlags, globalFlags...),
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

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  WORM:
     MINIO_WORM: To turn on Write-Once-Read-Many in server, set this value to "on".

  BUCKET-DNS:
     MINIO_DOMAIN:    To enable bucket DNS requests, set this value to Minio host domain name.
     MINIO_PUBLIC_IPS: To enable bucket DNS requests, set this value to list of Minio host public IP(s) delimited by ",".
     MINIO_ETCD_ENDPOINTS: To enable bucket DNS requests, set this value to list of etcd endpoints delimited by ",".

   KMS:
     MINIO_SSE_VAULT_ENDPOINT: To enable Vault as KMS,set this value to Vault endpoint.
     MINIO_SSE_VAULT_APPROLE_ID: To enable Vault as KMS,set this value to Vault AppRole ID.
     MINIO_SSE_VAULT_APPROLE_SECRET: To enable Vault as KMS,set this value to Vault AppRole Secret ID.
     MINIO_SSE_VAULT_KEY_NAME: To enable Vault as KMS,set this value to Vault encryption key-ring name.

EXAMPLES:
  1. Start minio server on "/home/shared" directory.
     $ {{.HelpName}} /home/shared

  2. Start minio server bound to a specific ADDRESS:PORT.
     $ {{.HelpName}} --address 192.168.1.101:9000 /home/shared

  3. Start minio server and enable virtual-host-style requests.
     $ export MINIO_DOMAIN=mydomain.com
     $ {{.HelpName}} --address mydomain.com:9000 /mnt/export

  4. Start erasure coded minio server on a node with 64 drives.
     $ {{.HelpName}} /mnt/export{1...64}

  5. Start distributed minio server on an 32 node setup with 32 drives each. Run following command on all the 32 nodes.
     $ export MINIO_ACCESS_KEY=minio
     $ export MINIO_SECRET_KEY=miniostorage
     $ {{.HelpName}} http://node{1...32}.example.com/mnt/export/{1...32}

  6. Start minio server with edge caching enabled.
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_MAXUSE=80
     $ {{.HelpName}} /home/shared

  7. Start minio server with KMS enabled.
     $ export MINIO_SSE_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
     $ export MINIO_SSE_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
     $ export MINIO_SSE_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
     $ export MINIO_SSE_VAULT_KEY_NAME=my-minio-key
     $ {{.HelpName}} /home/shared
`,
}

// Checks if endpoints are either available through environment
// or command line, returns false if both fails.
func endpointsPresent(ctx *cli.Context) bool {
	_, ok := os.LookupEnv("MINIO_ENDPOINTS")
	if !ok {
		ok = ctx.Args().Present()
	}
	return ok
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Handle common command args.
	handleCommonCmdArgs(ctx)

	logger.FatalIf(CheckLocalServerAddr(globalCLIContext.Addr), "Unable to validate passed arguments")

	var setupType SetupType
	var err error

	if len(ctx.Args()) > serverCommandLineArgsMax {
		uErr := uiErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("Invalid total number of endpoints (%d) passed, supported upto 32 unique arguments",
			len(ctx.Args())))
		logger.FatalIf(uErr, "Unable to validate passed endpoints")
	}

	endpoints := strings.Fields(os.Getenv("MINIO_ENDPOINTS"))
	if len(endpoints) > 0 {
		globalMinioAddr, globalEndpoints, setupType, globalXLSetCount, globalXLSetDriveCount, err = createServerEndpoints(globalCLIContext.Addr, endpoints...)
	} else {
		globalMinioAddr, globalEndpoints, setupType, globalXLSetCount, globalXLSetDriveCount, err = createServerEndpoints(globalCLIContext.Addr, ctx.Args()...)
	}
	logger.FatalIf(err, "Invalid command line arguments")

	globalMinioHost, globalMinioPort = mustSplitHostPort(globalMinioAddr)

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error sutiation we check for port availability.
	logger.FatalIf(checkPortAvailability(globalMinioPort), "Unable to start the server")

	globalIsXL = (setupType == XLSetupType)
	globalIsDistXL = (setupType == DistXLSetupType)
	if globalIsDistXL {
		globalIsXL = true
	}
}

func serverHandleEnvVars() {
	// Handle common environment variables.
	handleCommonEnvVars()

	if serverRegion := os.Getenv("MINIO_REGION"); serverRegion != "" {
		// region Envs are set globally.
		globalIsEnvRegion = true
		globalServerRegion = serverRegion
	}

}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" || !endpointsPresent(ctx) {
		cli.ShowCommandHelpAndExit(ctx, "server", 1)
	}

	// Disable logging until server initialization is complete, any
	// error during initialization will be shown as a fatal message
	logger.Disable = true

	// Handle all server command args.
	serverHandleCmdArgs(ctx)

	// Create certs path.
	logger.FatalIf(createConfigDir(), "Unable to initialize configuration files")

	// Check and load TLS certificates.
	var err error
	globalPublicCerts, globalTLSCerts, globalIsSSL, err = getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	globalRootCAs, err = getRootCAs(getCADir())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistXL {
		if globalEndpoints.IsHTTPS() && !globalIsSSL {
			logger.Fatal(uiErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.IsHTTPS() && globalIsSSL {
			logger.Fatal(uiErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	if !globalCLIContext.Quiet {
		// Check for new updates from dl.minio.io.
		mode := globalMinioModeFS
		if globalIsDistXL {
			mode = globalMinioModeDistXL
		} else if globalIsXL {
			mode = globalMinioModeXL
		}
		checkUpdate(mode)
	}

	// FIXME: This code should be removed in future releases and we should have mandatory
	// check for ENVs credentials under distributed setup. Until all users migrate we
	// are intentionally providing backward compatibility.
	{
		// Check for backward compatibility and newer style.
		if !globalIsEnvCreds && globalIsDistXL {
			// Try to load old config file if any, for backward compatibility.
			var config = &serverConfig{}
			if _, err = Load(getConfigFile(), config); err == nil {
				globalActiveCred = config.Credential
			}

			if os.IsNotExist(err) {
				if _, err = Load(getConfigFile()+".deprecated", config); err == nil {
					globalActiveCred = config.Credential
				}
			}

			if globalActiveCred.IsValid() {
				// Credential is valid don't throw an error instead print a message regarding deprecation of 'config.json'
				// based model and proceed to use it for now in distributed setup.
				logger.Info(`Supplying credentials from your 'config.json' is **DEPRECATED**, Access key and Secret key in distributed server mode is expected to be specified with environment variables MINIO_ACCESS_KEY and MINIO_SECRET_KEY. This approach will become mandatory in future releases, please migrate to this approach soon.`)
			} else {
				// Credential is not available anywhere by both means, we cannot start distributed setup anymore, fail eagerly.
				logger.Fatal(uiErrEnvCredentialsMissingDistributed(nil), "Unable to initialize the server in distributed mode")
			}
		}
	}

	// Set system resources to maximum.
	logger.LogIf(context.Background(), setMaxResources())

	// Set nodes for dsync for distributed setup.
	if globalIsDistXL {
		globalDsync, err = dsync.New(newDsyncNodes(globalEndpoints))
		if err != nil {
			logger.Fatal(err, "Unable to initialize distributed locking on %s", globalEndpoints)
		}
	}

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	// Init global heal state
	initAllHealState(globalIsXL)

	// Configure server.
	var handler http.Handler
	handler, err = configureServerHandler(globalEndpoints)
	if err != nil {
		logger.Fatal(uiErrUnexpectedError(err), "Unable to configure one of server's RPC services")
	}

	// Initialize Admin Peers inter-node communication only in distributed setup.
	initGlobalAdminPeers(globalEndpoints)

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	globalHTTPServer = xhttp.NewServer([]string{globalMinioAddr}, criticalErrorHandler{handler}, getCert)
	globalHTTPServer.UpdateBytesReadFunc = globalConnStats.incInputBytes
	globalHTTPServer.UpdateBytesWrittenFunc = globalConnStats.incOutputBytes
	go func() {
		globalHTTPServerErrorCh <- globalHTTPServer.Start()
	}()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	newObject, err := newObjectLayer(globalEndpoints)
	if err != nil {
		// Stop watching for any certificate changes.
		globalTLSCerts.Stop()

		globalHTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize backend")
	}

	// Populate existing buckets to the etcd backend
	if globalDNSConfig != nil {
		initFederatorBackend(newObject)
	}

	// Re-enable logging
	logger.Disable = false

	// Create a new config system.
	globalConfigSys = NewConfigSys()

	// Initialize config system.
	if err = globalConfigSys.Init(newObject); err != nil {
		logger.Fatal(err, "Unable to initialize config system")
	}

	// Load logger subsystem
	loadLoggers()

	var cacheConfig = globalServerConfig.GetCacheConfig()
	if len(cacheConfig.Drives) > 0 {
		// initialize the new disk cache objects.
		globalCacheObjectAPI, err = newServerCacheObjects(cacheConfig)
		logger.FatalIf(err, "Unable to initialize disk caching")
	}

	// Create new IAM system.
	globalIAMSys = NewIAMSys()
	if err = globalIAMSys.Init(newObject); err != nil {
		logger.Fatal(err, "Unable to initialize IAM system")
	}

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Initialize policy system.
	if err = globalPolicySys.Init(newObject); err != nil {
		logger.Fatal(err, "Unable to initialize policy system")
	}

	// Create new notification system.
	globalNotificationSys = NewNotificationSys(globalServerConfig, globalEndpoints)

	// Initialize notification system.
	if err = globalNotificationSys.Init(newObject); err != nil {
		logger.LogIf(context.Background(), err)
	}
	if globalAutoEncryption && !newObject.IsEncryptionSupported() {
		logger.Fatal(errors.New("Invalid KMS configuration"), "auto-encryption is enabled but server does not support encryption")
	}

	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(getAPIEndpoints())

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

	format, err := waitForFormatXL(context.Background(), endpoints[0].IsLocal, endpoints, globalXLSetCount, globalXLSetDriveCount)
	if err != nil {
		return nil, err
	}

	return newXLSets(endpoints, format, len(format.XL.Sets), len(format.XL.Sets[0]))
}
