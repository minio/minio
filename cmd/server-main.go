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
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"runtime"

	"github.com/minio/cli"
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
		if older > time.Duration(0) {
			log.Println(colorizeUpdateMessage(downloadURL, older))
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

type serverCmdConfig struct {
	serverAddr string
	endpoints  []*url.URL
}

// Parse an array of end-points (from the command line)
func parseStorageEndpoints(eps []string) (endpoints []*url.URL, err error) {
	for _, ep := range eps {
		if ep == "" {
			return nil, errInvalidArgument
		}
		var u *url.URL
		u, err = url.Parse(ep)
		if err != nil {
			return nil, err
		}
		if u.Host != "" {
			_, port, err := net.SplitHostPort(u.Host)
			// Ignore the missing port error as the default port can be globalMinioPort.
			if err != nil && !strings.Contains(err.Error(), "missing port in address") {
				return nil, err
			}

			if globalMinioHost == "" {
				// For ex.: minio server host1:port1 host2:port2...
				// we return error as port is configurable only
				// using "--address :port"
				if port != "" {
					return nil, fmt.Errorf("Invalid Argument %s, port configurable using --address :<port>", u.Host)
				}
				u.Host = net.JoinHostPort(u.Host, globalMinioPort)
			} else {
				// For ex.: minio server --address host:port host1:port1 host2:port2...
				// i.e if "--address host:port" is specified
				// port info in u.Host is mandatory else return error.
				if port == "" {
					return nil, fmt.Errorf("Invalid Argument %s, port mandatory when --address <host>:<port> is used", u.Host)
				}
			}
		}
		endpoints = append(endpoints, u)
	}
	return endpoints, nil
}

// Validate if input disks are sufficient for initializing XL.
func checkSufficientDisks(eps []*url.URL) error {
	// Verify total number of disks.
	total := len(eps)
	if total > maxErasureBlocks {
		return errXLMaxDisks
	}
	if total < minErasureBlocks {
		return errXLMinDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// Verify if we have even number of disks.
	// only combination of 4, 6, 8, 10, 12, 14, 16 are supported.
	if !isEven(total) {
		return errXLNumDisks
	}

	// Success.
	return nil
}

// Returns if slice of disks is a distributed setup.
func isDistributedSetup(eps []*url.URL) bool {
	// Validate if one the disks is not local.
	for _, ep := range eps {
		if !isLocalStorage(ep) {
			// One or more disks supplied as arguments are
			// not attached to the local node.
			return true
		}
	}
	return false
}

// Returns true if path is empty, or equals to '.', '/', '\' characters.
func isPathSentinel(path string) bool {
	return path == "" || path == "." || path == "/" || path == `\`
}

// Returned when path is empty or root path.
var errEmptyRootPath = errors.New("Empty or root path is not allowed")

// Invalid scheme passed.
var errInvalidScheme = errors.New("Invalid scheme")

// Check if endpoint is in expected syntax by valid scheme/path across all platforms.
func checkEndpointURL(endpointURL *url.URL) (err error) {
	// Applicable to all OS.
	if endpointURL.Scheme == "" || endpointURL.Scheme == httpScheme || endpointURL.Scheme == httpsScheme {
		if isPathSentinel(path.Clean(endpointURL.Path)) {
			err = errEmptyRootPath
		}

		return err
	}

	// Applicable to Windows only.
	if runtime.GOOS == globalWindowsOSName {
		// On Windows, endpoint can be a path with drive eg. C:\Export and its URL.Scheme is 'C'.
		// Check if URL.Scheme is a single letter alphabet to represent a drive.
		// Note: URL.Parse() converts scheme into lower case always.
		if len(endpointURL.Scheme) == 1 && endpointURL.Scheme[0] >= 'a' && endpointURL.Scheme[0] <= 'z' {
			// If endpoint is C:\ or C:\export, URL.Path does not have path information like \ or \export
			// hence we directly work with endpoint.
			if isPathSentinel(strings.SplitN(path.Clean(endpointURL.String()), ":", 2)[1]) {
				err = errEmptyRootPath
			}

			return err
		}
	}

	return errInvalidScheme
}

// Check if endpoints are in expected syntax by valid scheme/path across all platforms.
func checkEndpointsSyntax(eps []*url.URL, disks []string) error {
	for i, u := range eps {
		if err := checkEndpointURL(u); err != nil {
			return fmt.Errorf("%s: %s (%s)", err.Error(), u.Path, disks[i])
		}
	}

	return nil
}

// Make sure all the command line parameters are OK and exit in case of invalid parameters.
func checkServerSyntax(endpoints []*url.URL, disks []string) {
	// Validate if endpoints follow the expected syntax.
	err := checkEndpointsSyntax(endpoints, disks)
	fatalIf(err, "Invalid endpoints found %s", strings.Join(disks, " "))

	// Validate for duplicate endpoints are supplied.
	err = checkDuplicateEndpoints(endpoints)
	fatalIf(err, "Duplicate entries in %s", strings.Join(disks, " "))

	if len(endpoints) > 1 {
		// Validate if we have sufficient disks for XL setup.
		err = checkSufficientDisks(endpoints)
		fatalIf(err, "Insufficient number of disks.")
	} else {
		// Validate if we have invalid disk for FS setup.
		if endpoints[0].Host != "" && endpoints[0].Scheme != "" {
			fatalIf(errInvalidArgument, "%s, FS setup expects a filesystem path", endpoints[0])
		}
	}

	if !isDistributedSetup(endpoints) {
		// for FS and singlenode-XL validation is done, return.
		return
	}

	// Rest of the checks applies only to distributed XL setup.
	if globalMinioHost != "" {
		// We are here implies --address host:port is passed, hence the user is trying
		// to run one minio process per export disk.
		if globalMinioPort == "" {
			fatalIf(errInvalidArgument, "Port missing, Host:Port should be specified for --address")
		}
		foundCnt := 0
		for _, ep := range endpoints {
			if ep.Host == globalMinioAddr {
				foundCnt++
			}
		}
		if foundCnt == 0 {
			// --address host:port should be available in the XL disk list.
			fatalIf(errInvalidArgument, "%s is not available in %s", globalMinioAddr, strings.Join(disks, " "))
		}
		if foundCnt > 1 {
			// --address host:port should match exactly one entry in the XL disk list.
			fatalIf(errInvalidArgument, "%s matches % entries in %s", globalMinioAddr, foundCnt, strings.Join(disks, " "))
		}
	}

	for _, ep := range endpoints {
		if ep.Scheme == httpsScheme && !globalIsSSL {
			// Certificates should be provided for https configuration.
			fatalIf(errInvalidArgument, "Certificates not provided for secure configuration")
		}
	}
}

// Checks if any of the endpoints supplied is local to this server.
func isAnyEndpointLocal(eps []*url.URL) bool {
	anyLocalEp := false
	for _, ep := range eps {
		if isLocalStorage(ep) {
			anyLocalEp = true
			break
		}
	}
	return anyLocalEp
}

// Returned when there are no ports.
var errEmptyPort = errors.New("Port cannot be empty or '0', please use `--address` to pick a specific port")

// Convert an input address of form host:port into, host and port, returns if any.
func getHostPort(address string) (host, port string, err error) {
	// Check if requested port is available.
	host, port, err = net.SplitHostPort(address)
	if err != nil {
		return "", "", err
	}

	// Empty ports.
	if port == "0" || port == "" {
		// Port zero or empty means use requested to choose any freely available
		// port. Avoid this since it won't work with any configured clients,
		// can lead to serious loss of availability.
		return "", "", errEmptyPort
	}

	// Parse port.
	if _, err = strconv.Atoi(port); err != nil {
		return "", "", err
	}

	if runtime.GOOS == "darwin" {
		// On macOS, if a process already listens on 127.0.0.1:PORT, net.Listen() falls back
		// to IPv6 address ie minio will start listening on IPv6 address whereas another
		// (non-)minio process is listening on IPv4 of given port.
		// To avoid this error sutiation we check for port availability only for macOS.
		if err = checkPortAvailability(port); err != nil {
			return "", "", err
		}
	}

	// Success.
	return host, port, nil
}

func initConfig() {
	// Config file does not exist, we create it fresh and return upon success.
	if isFile(getConfigFile()) {
		fatalIf(migrateConfig(), "Config migration failed.")
		fatalIf(validateConfig(), "Unable to validate configuration file")
		fatalIf(loadConfig(), "Unable to initialize minio config")
	} else {
		fatalIf(newConfig(), "Unable to initialize minio config for the first time.")
		log.Println("Created minio configuration file successfully at " + getConfigDir())
	}
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Get configuration directory from command line argument.
	configDir := ctx.String("config-dir")
	if !ctx.IsSet("config-dir") && ctx.GlobalIsSet("config-dir") {
		configDir = ctx.GlobalString("config-dir")
	}
	if configDir == "" {
		fatalIf(errors.New("empty directory"), "Configuration directory cannot be empty.")
	}

	// Disallow relative paths, figure out absolute paths.
	{
		configDirAbs, err := filepath.Abs(configDir)
		fatalIf(err, "Unable to fetch absolute path for config directory %s", configDir)

		configDir = configDirAbs
	}

	// Set configuration directory.
	setConfigDir(configDir)

	// Server address.
	globalMinioAddr = ctx.String("address")

	var err error
	globalMinioHost, globalMinioPort, err = getHostPort(globalMinioAddr)
	fatalIf(err, "Unable to extract host and port %s", globalMinioAddr)

	// Disks to be used in server init.
	endpoints, err := parseStorageEndpoints(ctx.Args())
	fatalIf(err, "Unable to parse storage endpoints %s", ctx.Args())

	// Sort endpoints for consistent ordering across multiple
	// nodes in a distributed setup. This is to avoid format.json
	// corruption if the disks aren't supplied in the same order
	// on all nodes.
	sort.Sort(byHostPath(endpoints))

	checkServerSyntax(endpoints, ctx.Args())

	// Should exit gracefully if none of the endpoints passed
	// as command line args are local to this server.
	if !isAnyEndpointLocal(endpoints) {
		fatalIf(errInvalidArgument, "None of the disks passed as command line args are local to this server.")
	}

	// Check if endpoints are part of distributed setup.
	globalIsDistXL = isDistributedSetup(endpoints)

	// Set globalIsXL if erasure code backend is about to be
	// initialized for the given endpoints.
	if len(endpoints) > 1 {
		globalIsXL = true
	}

	// Set endpoints of []*url.URL type to globalEndpoints.
	globalEndpoints = endpoints
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
		if globalIsXL {
			mode = globalMinioModeXL
		}
		if globalIsDistXL {
			mode = globalMinioModeDistXL
		}
		checkUpdate(mode)
	}

	// Set system resources to maximum.
	errorIf(setMaxResources(), "Unable to change resource limit")

	// Set nodes for dsync for distributed setup.
	if globalIsDistXL {
		fatalIf(initDsyncNodes(), "Unable to initialize distributed locking clients")
	}

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	// Configure server.
	srvConfig := serverCmdConfig{
		serverAddr: globalMinioAddr,
		endpoints:  globalEndpoints,
	}

	// Configure server.
	handler, err := configureServerHandler(srvConfig)
	fatalIf(err, "Unable to configure one of server's RPC services.")

	// Initialize a new HTTP server.
	apiServer := NewServerMux(globalMinioAddr, handler)

	// Set the global minio addr for this server.
	globalMinioAddr = getLocalAddress(srvConfig)

	// Initialize S3 Peers inter-node communication only in distributed setup.
	initGlobalS3Peers(globalEndpoints)

	// Initialize Admin Peers inter-node communication only in distributed setup.
	initGlobalAdminPeers(globalEndpoints)

	// Determine API endpoints where we are going to serve the S3 API from.
	globalAPIEndpoints, err = finalizeAPIEndpoints(apiServer.Addr)
	fatalIf(err, "Unable to finalize API endpoints for %s", apiServer.Addr)

	// Start server, automatically configures TLS if certs are available.
	go func() {
		cert, key := "", ""
		if globalIsSSL {
			cert, key = getPublicCertFile(), getPrivateKeyFile()
		}
		fatalIf(apiServer.ListenAndServe(cert, key), "Failed to start minio server.")
	}()

	newObject, err := newObjectLayer(srvConfig)
	fatalIf(err, "Initializing object layer failed")

	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(globalAPIEndpoints)

	// Set uptime time after object layer has initialized.
	globalBootTime = UTCNow()

	// Waits on the server.
	<-globalServiceDoneCh
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(srvCmdCfg serverCmdConfig) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.
	isFS := len(srvCmdCfg.endpoints) == 1
	if isFS {
		// Unescape is needed for some UNC paths on windows
		// which are of this form \\127.0.0.1\\export\test.
		var fsPath string
		fsPath, err = url.QueryUnescape(srvCmdCfg.endpoints[0].String())
		if err != nil {
			return nil, err
		}

		// Initialize new FS object layer.
		newObject, err = newFSObjectLayer(fsPath)
		if err != nil {
			return nil, err
		}

		// FS initialized, return.
		return newObject, nil
	}

	// First disk argument check if it is local.
	firstDisk := isLocalStorage(srvCmdCfg.endpoints[0])

	// Initialize storage disks.
	storageDisks, err := initStorageDisks(srvCmdCfg.endpoints)
	if err != nil {
		return nil, err
	}

	// Wait for formatting disks for XL backend.
	var formattedDisks []StorageAPI
	formattedDisks, err = waitForFormatXLDisks(firstDisk, srvCmdCfg.endpoints, storageDisks)
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
