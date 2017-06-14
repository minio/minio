/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/dsync"
)

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
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
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}PATH [PATH...]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Custom username or access key of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Custom password or secret key of 8 to 40 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

EXAMPLES:
  1. Start minio server on "/home/shared" directory.
      $ {{.HelpName}} /home/shared

  2. Start minio server bound to a specific ADDRESS:PORT.
      $ {{.HelpName}} --address 192.168.1.101:9000 /home/shared

  3. Start erasure coded minio server on a 12 disks server.
      $ {{.HelpName}} /mnt/export1/ /mnt/export2/ /mnt/export3/ /mnt/export4/ \
          /mnt/export5/ /mnt/export6/ /mnt/export7/ /mnt/export8/ /mnt/export9/ \
          /mnt/export10/ /mnt/export11/ /mnt/export12/

  4. Start erasure coded distributed minio server on a 4 node setup with 1 drive each. Run following commands on all the 4 nodes.
      $ export MINIO_ACCESS_KEY=minio
      $ export MINIO_SECRET_KEY=miniostorage
      $ {{.HelpName}} http://192.168.1.11/mnt/export/ http://192.168.1.12/mnt/export/ \
          http://192.168.1.13/mnt/export/ http://192.168.1.14/mnt/export/
`,
}

// Check for updates and print a notification message
func checkUpdate(mode string) {
	// Its OK to ignore any errors during getUpdateInfo() here.
	if older, downloadURL, err := getUpdateInfo(1*time.Second, mode); err == nil {
		if updateMsg := computeUpdateMessage(downloadURL, older); updateMsg != "" {
			log.Println(updateMsg)
		}
	}
}

func enableLoggers() {
	fileLogTarget := serverConfig.Logger.GetFile()
	if fileLogTarget.Enable {
		err := InitFileLogger(&fileLogTarget)
		fatalIf(err, "Unable to initialize file logger")
		log.AddTarget(fileLogTarget)
	}

	consoleLogTarget := serverConfig.Logger.GetConsole()
	if consoleLogTarget.Enable {
		InitConsoleLogger(&consoleLogTarget)
	}

	log.SetConsoleTarget(consoleLogTarget)
}

func initConfig() {
	// Config file does not exist, we create it fresh and return upon success.
	if isFile(getConfigFile()) {
		fatalIf(migrateConfig(), "Config migration failed.")
		fatalIf(loadConfig(), "Unable to load config version: '%s'.", v18)
	} else {
		fatalIf(newConfig(), "Unable to initialize minio config for the first time.")
		log.Println("Created minio configuration file successfully at " + getConfigDir())
	}
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Set configuration directory.
	{
		// Get configuration directory from command line argument.
		configDir := ctx.String("config-dir")
		if !ctx.IsSet("config-dir") && ctx.GlobalIsSet("config-dir") {
			configDir = ctx.GlobalString("config-dir")
		}
		if configDir == "" {
			fatalIf(errors.New("empty directory"), "Configuration directory cannot be empty.")
		}

		// Disallow relative paths, figure out absolute paths.
		configDirAbs, err := filepath.Abs(configDir)
		fatalIf(err, "Unable to fetch absolute path for config directory %s", configDir)

		setConfigDir(configDirAbs)
	}

	// Server address.
	serverAddr := ctx.String("address")
	fatalIf(CheckLocalServerAddr(serverAddr), "Invalid address ‘%s’ in command line argument.", serverAddr)

	var setupType SetupType
	var err error
	globalMinioAddr, globalEndpoints, setupType, err = CreateEndpoints(serverAddr, ctx.Args()...)
	fatalIf(err, "Invalid command line arguments server=‘%s’, args=%s", serverAddr, ctx.Args())
	globalMinioHost, globalMinioPort = mustSplitHostPort(globalMinioAddr)
	if runtime.GOOS == "darwin" {
		// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
		// to IPv6 address ie minio will start listening on IPv6 address whereas another
		// (non-)minio process is listening on IPv4 of given port.
		// To avoid this error sutiation we check for port availability only for macOS.
		fatalIf(checkPortAvailability(globalMinioPort), "Port %d already in use", globalMinioPort)
	}

	globalIsXL = (setupType == XLSetupType)
	globalIsDistXL = (setupType == DistXLSetupType)
	if globalIsDistXL {
		globalIsXL = true
	}
}

func serverHandleEnvVars() {
	// Start profiler if env is set.
	if profiler := os.Getenv("_MINIO_PROFILER"); profiler != "" {
		globalProfiler = startProfiler(profiler)
	}

	// Check if object cache is disabled.
	globalXLObjCacheDisabled = strings.EqualFold(os.Getenv("_MINIO_CACHE"), "off")

	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey != "" && secretKey != "" {
		cred, err := createCredential(accessKey, secretKey)
		fatalIf(err, "Invalid access/secret Key set in environment.")

		// credential Envs are set globally.
		globalIsEnvCreds = true
		globalActiveCred = cred
	}

	if browser := os.Getenv("MINIO_BROWSER"); browser != "" {
		browserFlag, err := ParseBrowserFlag(browser)
		if err != nil {
			fatalIf(errors.New("invalid value"), "Unknown value ‘%s’ in MINIO_BROWSER environment variable.", browser)
		}

		// browser Envs are set globally, this does not represent
		// if browser is turned off or on.
		globalIsEnvBrowser = true
		globalIsBrowserEnabled = bool(browserFlag)
	}

	if serverRegion := os.Getenv("MINIO_REGION"); serverRegion != "" {
		// region Envs are set globally.
		globalIsEnvRegion = true
		globalServerRegion = serverRegion
	}

}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "server", 1)
	}

	// Get quiet flag from command line argument.
	quietFlag := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	if quietFlag {
		log.EnableQuiet()
	}

	serverHandleCmdArgs(ctx)
	serverHandleEnvVars()

	// Create certs path.
	fatalIf(createConfigDir(), "Unable to create configuration directories.")

	initConfig()

	// Enable loggers as per configuration file.
	enableLoggers()

	// Init the error tracing module.
	initError()

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalIsSSL, err = getSSLConfig()
	fatalIf(err, "Invalid SSL key file")

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
	errorIf(setMaxResources(), "Unable to change resource limit")

	// Set nodes for dsync for distributed setup.
	if globalIsDistXL {
		clnts, myNode := newDsyncNodes(globalEndpoints)
		fatalIf(dsync.Init(clnts, myNode), "Unable to initialize distributed locking clients")
	}

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	// Configure server.
	handler, err := configureServerHandler(globalEndpoints)
	fatalIf(err, "Unable to configure one of server's RPC services.")

	// Initialize a new HTTP server.
	apiServer := NewServerMux(globalMinioAddr, handler)

	// Initialize S3 Peers inter-node communication only in distributed setup.
	initGlobalS3Peers(globalEndpoints)

	// Initialize Admin Peers inter-node communication only in distributed setup.
	initGlobalAdminPeers(globalEndpoints)

	// Start server, automatically configures TLS if certs are available.
	go func() {
		cert, key := "", ""
		if globalIsSSL {
			cert, key = getPublicCertFile(), getPrivateKeyFile()
		}
		fatalIf(apiServer.ListenAndServe(cert, key), "Failed to start minio server.")
	}()

	newObject, err := newObjectLayer(globalEndpoints)
	fatalIf(err, "Initializing object layer failed")

	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	apiEndpoints := getAPIEndpoints(apiServer.Addr)
	printStartupMessage(apiEndpoints)

	// Set uptime time after object layer has initialized.
	globalBootTime = UTCNow()

	// Waits on the server.
	<-globalServiceDoneCh
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(endpoints EndpointList) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.
	isFS := len(endpoints) == 1
	if isFS {
		// Initialize new FS object layer.
		return newFSObjectLayer(endpoints[0].Path)
	}

	// Initialize storage disks.
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		return nil, err
	}

	// Wait for formatting disks for XL backend.
	var formattedDisks []StorageAPI

	// First disk argument check if it is local.
	firstDisk := endpoints[0].IsLocal
	formattedDisks, err = waitForFormatXLDisks(firstDisk, endpoints, storageDisks)
	if err != nil {
		return nil, err
	}

	// Cleanup objects that weren't successfully written into the namespace.
	if err = houseKeeping(storageDisks); err != nil {
		return nil, err
	}

	// Once XL formatted, initialize object layer.
	newObject, err = newXLObjectLayer(formattedDisks)
	if err != nil {
		return nil, err
	}

	// XL initialized, return.
	return newObject, nil
}
