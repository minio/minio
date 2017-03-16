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
	"sort"
	"strconv"
	"strings"
	"time"

	"runtime"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
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
func checkUpdate() {
	// Its OK to ignore any errors during getUpdateInfo() here.
	if older, downloadURL, err := getUpdateInfo(1 * time.Second); err == nil {
		if older > time.Duration(0) {
			console.Println(colorizeUpdateMessage(downloadURL, older))
		}
	}
}

// envParams holds all env parameters
type envParams struct {
	creds   credential
	browser string
}

func migrate() {
	// Migrate config file
	err := migrateConfig()
	fatalIf(err, "Config migration failed.")

	// Migrate other configs here.
}

func enableLoggers() {
	// Enable all loggers here.
	enableConsoleLogger()
	enableFileLogger()
	// Add your logger here.
}

// Initializes a new config if it doesn't exist, else migrates any old config
// to newer config and finally loads the config to memory.
func initConfig() {
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	var cred credential
	var err error
	if accessKey != "" && secretKey != "" {
		if cred, err = createCredential(accessKey, secretKey); err != nil {
			console.Fatalf("Invalid access/secret Key set in environment. Err: %s.\n", err)
		}

		// Envs are set globally.
		globalIsEnvCreds = true
	}

	browser := os.Getenv("MINIO_BROWSER")
	if browser != "" {
		if !(strings.EqualFold(browser, "off") || strings.EqualFold(browser, "on")) {
			console.Fatalf("Invalid value ‘%s’ in MINIO_BROWSER environment variable.", browser)
		}

		globalIsEnvBrowser = strings.EqualFold(browser, "off")
	}

	envs := envParams{
		creds:   cred,
		browser: browser,
	}

	// Config file does not exist, we create it fresh and return upon success.
	if !isConfigFileExists() {
		if err := newConfig(envs); err != nil {
			console.Fatalf("Unable to initialize minio config for the first time. Error: %s.\n", err)
		}
		console.Println("Created minio configuration file successfully at " + getConfigDir())
		return
	}

	// Migrate any old version of config / state files to newer format.
	migrate()

	// Validate config file
	if err := validateConfig(); err != nil {
		console.Fatalf("Cannot validate configuration file. Error: %s\n", err)
	}

	// Once we have migrated all the old config, now load them.
	if err := loadConfig(envs); err != nil {
		console.Fatalf("Unable to initialize minio config. Error: %s.\n", err)
	}
}

// Generic Minio initialization to create/load config, prepare loggers, etc..
func minioInit(ctx *cli.Context) {
	// Create certs path.
	fatalIf(createConfigDir(), "Unable to create \"certs\" directory.")

	// Is TLS configured?.
	globalIsSSL = isSSL()

	// Initialize minio server config.
	initConfig()

	// Enable all loggers by now so we can use errorIf() and fatalIf()
	enableLoggers()

	// Init the error tracing module.
	initError()
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

// initServer initialize server config.
func initServerConfig(c *cli.Context) {
	// Initialization such as config generating/loading config, enable logging, ..
	minioInit(c)

	// Load user supplied root CAs
	fatalIf(loadRootCAs(), "Unable to load a CA files")

	// Set system resources to maximum.
	errorIf(setMaxResources(), "Unable to change resource limit")
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
func checkServerSyntax(c *cli.Context) {
	serverAddr := c.String("address")

	host, portStr, err := net.SplitHostPort(serverAddr)
	fatalIf(err, "Unable to parse %s.", serverAddr)

	// Verify syntax for all the XL disks.
	disks := c.Args()

	// Parse disks check if they comply with expected URI style.
	endpoints, err := parseStorageEndpoints(disks)
	fatalIf(err, "Unable to parse storage endpoints %s", strings.Join(disks, " "))

	// Validate if endpoints follow the expected syntax.
	err = checkEndpointsSyntax(endpoints, disks)
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
	if host != "" {
		// We are here implies --address host:port is passed, hence the user is trying
		// to run one minio process per export disk.
		if portStr == "" {
			fatalIf(errInvalidArgument, "Port missing, Host:Port should be specified for --address")
		}
		foundCnt := 0
		for _, ep := range endpoints {
			if ep.Host == serverAddr {
				foundCnt++
			}
		}
		if foundCnt == 0 {
			// --address host:port should be available in the XL disk list.
			fatalIf(errInvalidArgument, "%s is not available in %s", serverAddr, strings.Join(disks, " "))
		}
		if foundCnt > 1 {
			// --address host:port should match exactly one entry in the XL disk list.
			fatalIf(errInvalidArgument, "%s matches % entries in %s", serverAddr, foundCnt, strings.Join(disks, " "))
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

// serverMain handler called for 'minio server' command.
func serverMain(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}

	// Get quiet flag from command line argument.
	quietFlag := c.Bool("quiet") || c.GlobalBool("quiet")

	// Get configuration directory from command line argument.
	configDir := c.String("config-dir")
	if !c.IsSet("config-dir") && c.GlobalIsSet("config-dir") {
		configDir = c.GlobalString("config-dir")
	}
	if configDir == "" {
		console.Fatalln("Configuration directory cannot be empty.")
	}

	// Set configuration directory.
	setConfigDir(configDir)

	// Start profiler if env is set.
	if profiler := os.Getenv("_MINIO_PROFILER"); profiler != "" {
		globalProfiler = startProfiler(profiler)
	}

	// Initializes server config, certs, logging and system settings.
	initServerConfig(c)

	// Check for new updates from dl.minio.io.
	if !quietFlag {
		checkUpdate()
	}

	// Server address.
	serverAddr := c.String("address")

	var err error
	globalMinioHost, globalMinioPort, err = getHostPort(serverAddr)
	fatalIf(err, "Unable to extract host and port %s", serverAddr)

	// Check server syntax and exit in case of errors.
	// Done after globalMinioHost and globalMinioPort is set
	// as parseStorageEndpoints() depends on it.
	checkServerSyntax(c)

	// Disks to be used in server init.
	endpoints, err := parseStorageEndpoints(c.Args())
	fatalIf(err, "Unable to parse storage endpoints %s", c.Args())

	// Should exit gracefully if none of the endpoints passed
	// as command line args are local to this server.
	if !isAnyEndpointLocal(endpoints) {
		fatalIf(errInvalidArgument, "None of the disks passed as command line args are local to this server.")
	}

	// Sort endpoints for consistent ordering across multiple
	// nodes in a distributed setup. This is to avoid format.json
	// corruption if the disks aren't supplied in the same order
	// on all nodes.
	sort.Sort(byHostPath(endpoints))

	// Configure server.
	srvConfig := serverCmdConfig{
		serverAddr: serverAddr,
		endpoints:  endpoints,
	}

	// Check if endpoints are part of distributed setup.
	globalIsDistXL = isDistributedSetup(endpoints)

	// Set nodes for dsync for distributed setup.
	if globalIsDistXL {
		fatalIf(initDsyncNodes(endpoints), "Unable to initialize distributed locking clients")
	}

	// Set globalIsXL if erasure code backend is about to be
	// initialized for the given endpoints.
	if len(endpoints) > 1 {
		globalIsXL = true
	}

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	// Configure server.
	handler, err := configureServerHandler(srvConfig)
	fatalIf(err, "Unable to configure one of server's RPC services.")

	// Initialize a new HTTP server.
	apiServer := NewServerMux(serverAddr, handler)

	// Set the global minio addr for this server.
	globalMinioAddr = getLocalAddress(srvConfig)

	// Initialize S3 Peers inter-node communication only in distributed setup.
	initGlobalS3Peers(endpoints)

	// Initialize Admin Peers inter-node communication only in distributed setup.
	initGlobalAdminPeers(endpoints)

	// Determine API endpoints where we are going to serve the S3 API from.
	apiEndPoints, err := finalizeAPIEndpoints(apiServer.Addr)
	fatalIf(err, "Unable to finalize API endpoints for %s", apiServer.Addr)

	// Set the global API endpoints value.
	globalAPIEndpoints = apiEndPoints

	// Start server, automatically configures TLS if certs are available.
	go func() {
		cert, key := "", ""
		if globalIsSSL {
			cert, key = getPublicCertFile(), getPrivateKeyFile()
		}
		fatalIf(apiServer.ListenAndServe(cert, key), "Failed to start minio server.")
	}()

	// Set endpoints of []*url.URL type to globalEndpoints.
	globalEndpoints = endpoints

	newObject, err := newObjectLayer(srvConfig)
	fatalIf(err, "Initializing object layer failed")

	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	if !quietFlag {
		printStartupMessage(apiEndPoints)
	}

	// Set uptime time after object layer has initialized.
	globalBootTime = time.Now().UTC()

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
