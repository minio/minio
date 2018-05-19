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
)

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":" + globalMinioPort,
		Usage: "Bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname.",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "Start object storage server.",
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

  ENDPOINTS:
     MINIO_ENDPOINTS: List of all endpoints delimited by ' '.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
	
  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  WORM:
     MINIO_WORM: To turn on Write-Once-Read-Many in server, set this value to "on".

EXAMPLES:
  1. Start minio server on "/home/shared" directory.
      $ {{.HelpName}} /home/shared

  2. Start minio server bound to a specific ADDRESS:PORT.
      $ {{.HelpName}} --address 192.168.1.101:9000 /home/shared

  3. Start minio server and enable virtual-host-style requests.
      $ export MINIO_DOMAIN=mydomain.com
      $ {{.HelpName}} --address mydomain.com:9000 /mnt/export

  4. Start minio server on 64 disks server with endpoints through environment variable.
      $ export MINIO_ENDPOINTS=/mnt/export{1...64}
      $ {{.HelpName}}

  5. Start distributed minio server on an 8 node setup with 8 drives each. Run following command on all the 8 nodes.
      $ export MINIO_ACCESS_KEY=minio
      $ export MINIO_SECRET_KEY=miniostorage
      $ {{.HelpName}} http://node{1...8}.example.com/mnt/export/{1...8}

  6. Start minio server with edge caching enabled.
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
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

	// Server address.
	serverAddr := ctx.String("address")
	logger.FatalIf(CheckLocalServerAddr(serverAddr), "Unable to validate passed arguments")

	var setupType SetupType
	var err error

	if len(ctx.Args()) > serverCommandLineArgsMax {
		uErr := uiErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("Invalid total number of endpoints (%d) passed, supported upto 32 unique arguments",
			len(ctx.Args())))
		logger.FatalIf(uErr, "Unable to validate passed endpoints")
	}

	endpoints := strings.Fields(os.Getenv("MINIO_ENDPOINTS"))
	if len(endpoints) > 0 {
		globalMinioAddr, globalEndpoints, setupType, globalXLSetCount, globalXLSetDriveCount, err = createServerEndpoints(serverAddr, endpoints...)
	} else {
		globalMinioAddr, globalEndpoints, setupType, globalXLSetCount, globalXLSetDriveCount, err = createServerEndpoints(serverAddr, ctx.Args()...)
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

func init() {
	logger.Init(GOPATH)
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" || !endpointsPresent(ctx) {
		cli.ShowCommandHelpAndExit(ctx, "server", 1)
	}

	// Disable logging until server initialization is complete, any
	// error during initialization will be shown as a fatal message
	logger.Disable = true

	// Get "json" flag from command line argument and
	// enable json and quite modes if jason flag is turned on.
	jsonFlag := ctx.IsSet("json") || ctx.GlobalIsSet("json")
	if jsonFlag {
		logger.EnableJSON()
	}

	// Get quiet flag from command line argument.
	quietFlag := ctx.IsSet("quiet") || ctx.GlobalIsSet("quiet")
	if quietFlag {
		logger.EnableQuiet()
	}

	logger.RegisterUIError(fmtError)

	// Handle all server command args.
	serverHandleCmdArgs(ctx)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Create certs path.
	logger.FatalIf(createConfigDir(), "Unable to initialize configuration files")

	// Initialize server config.
	initConfig()

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalTLSCertificate, globalIsSSL, err = getSSLConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistXL {
		if globalEndpoints.IsHTTPS() && !globalIsSSL {
			logger.Fatal(uiErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.IsHTTPS() && globalIsSSL {
			logger.Fatal(uiErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	if !quietFlag {
		// Check for new updates from dl.minio.io.
		mode := globalMinioModeFS
		if globalIsDistXL {
			mode = globalMinioModeDistXL
		} else if globalIsXL {
			mode = globalMinioModeXL
		}
		checkUpdate(mode)
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

	// Create new notification system.
	globalNotificationSys, err = NewNotificationSys(globalServerConfig, globalEndpoints)
	if err != nil {
		logger.Fatal(err, "Unable to initialize the notification system")
	}

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Initialize Admin Peers inter-node communication only in distributed setup.
	initGlobalAdminPeers(globalEndpoints)

	globalHTTPServer = xhttp.NewServer([]string{globalMinioAddr}, handler, globalTLSCertificate)
	globalHTTPServer.ReadTimeout = globalConnReadTimeout
	globalHTTPServer.WriteTimeout = globalConnWriteTimeout
	globalHTTPServer.UpdateBytesReadFunc = globalConnStats.incInputBytes
	globalHTTPServer.UpdateBytesWrittenFunc = globalConnStats.incOutputBytes
	go func() {
		globalHTTPServerErrorCh <- globalHTTPServer.Start()
	}()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	newObject, err := newObjectLayer(globalEndpoints)
	if err != nil {
		globalHTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize backend")
	}

	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	apiEndpoints := getAPIEndpoints(globalMinioAddr)
	printStartupMessage(apiEndpoints)

	// Set uptime time after object layer has initialized.
	globalBootTime = UTCNow()

	// Re-enable logging
	logger.Disable = false

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
