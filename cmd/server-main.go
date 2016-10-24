/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"regexp"

	"github.com/minio/cli"
)

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: "Specify custom server \"ADDRESS:PORT\", defaults to \":9000\".",
	},
	cli.StringFlag{
		Name:  "ignore-disks",
		Usage: "Specify comma separated list of disks that are offline.",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "Start object storage server.",
	Flags:  append(serverFlags, globalFlags...),
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Usage}}

USAGE:
  minio {{.Name}} [FLAGS] PATH [PATH...]

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Access key string of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Secret key string of 8 to 40 characters in length.

  CACHING:
     MINIO_CACHE_SIZE: Set total cache size in NN[GB|MB|KB]. Defaults to 8GB.
     MINIO_CACHE_EXPIRY: Set cache expiration duration in NN[h|m|s]. Defaults to 72 hours.

  SECURITY:
     MINIO_SECURE_CONSOLE: Set secure console to '0' to disable printing secret key. Defaults to '1'.

EXAMPLES:
  1. Start minio server.
      $ minio {{.Name}} /home/shared

  2. Start minio server bound to a specific IP:PORT, when you have multiple network interfaces.
      $ minio {{.Name}} --address 192.168.1.101:9000 /home/shared

  3. Start minio server on Windows.
      $ minio {{.Name}} C:\MyShare

  4. Start minio server on 12 disks to enable erasure coded layer with 6 data and 6 parity.
      $ minio {{.Name}} /mnt/export1/ /mnt/export2/ /mnt/export3/ /mnt/export4/ \
          /mnt/export5/ /mnt/export6/ /mnt/export7/ /mnt/export8/ /mnt/export9/ \
          /mnt/export10/ /mnt/export11/ /mnt/export12/

  5. Start minio server on 12 disks while ignoring two disks for initialization.
      $ minio {{.Name}} --ignore-disks=/mnt/export1/ /mnt/export1/ /mnt/export2/ \
          /mnt/export3/ /mnt/export4/ /mnt/export5/ /mnt/export6/ /mnt/export7/ \
	  /mnt/export8/ /mnt/export9/ /mnt/export10/ /mnt/export11/ /mnt/export12/

  6. Start minio server on a 4 node distributed setup. Type the following command on all the 4 nodes.
      $ export MINIO_ACCESS_KEY=minio
      $ export MINIO_SECRET_KEY=miniostorage
      $ minio {{.Name}} 192.168.1.11:/mnt/export/ 192.168.1.12:/mnt/export/ \
          192.168.1.13:/mnt/export/ 192.168.1.14:/mnt/export/

`,
}

type serverCmdConfig struct {
	serverAddr       string
	endPoints        []storageEndPoint
	ignoredEndPoints []storageEndPoint
	isDistXL         bool // True only if its distributed XL.
	storageDisks     []StorageAPI
}

// End point is specified in the command line as host:port:path or host:path or path
// host:port:path or host:path - for distributed XL. Default port is 9000.
// just path - for single node XL or FS.
type storageEndPoint struct {
	host string // Will be empty for single node XL and FS
	port int    // Will be valid for distributed XL
	path string // Will be valid for all configs
}

// Returns string form.
func (ep storageEndPoint) String() string {
	var str []string
	if ep.host != "" {
		str = append(str, ep.host)
	}
	if ep.port != 0 {
		str = append(str, strconv.Itoa(ep.port))
	}
	if ep.path != "" {
		str = append(str, ep.path)
	}
	return strings.Join(str, ":")
}

// Returns if ep is present in the eps list.
func (ep storageEndPoint) presentIn(eps []storageEndPoint) bool {
	for _, entry := range eps {
		if entry == ep {
			return true
		}
	}
	return false
}

// Parse end-point (of the form host:port:path or host:path or path)
func parseStorageEndPoint(ep string, defaultPort int) (storageEndPoint, error) {
	if runtime.GOOS == "windows" {
		// Try to match path, ex. C:\export or export
		matched, err := regexp.MatchString(`^([a-zA-Z]:\\[^:]+|[^:]+)$`, ep)
		if err != nil {
			return storageEndPoint{}, err
		}
		if matched {
			return storageEndPoint{path: ep}, nil
		}

		// Try to match host:path ex. 127.0.0.1:C:\export
		re, err := regexp.Compile(`^([^:]+):([a-zA-Z]:\\[^:]+)$`)
		if err != nil {
			return storageEndPoint{}, err
		}
		result := re.FindStringSubmatch(ep)
		if len(result) != 0 {
			return storageEndPoint{host: result[1], port: defaultPort, path: result[2]}, nil
		}

		// Try to match host:port:path ex. 127.0.0.1:443:C:\export
		re, err = regexp.Compile(`^([^:]+):([0-9]+):([a-zA-Z]:\\[^:]+)$`)
		if err != nil {
			return storageEndPoint{}, err
		}
		result = re.FindStringSubmatch(ep)
		if len(result) != 0 {
			portInt, err := strconv.Atoi(result[2])
			if err != nil {
				return storageEndPoint{}, err
			}
			return storageEndPoint{host: result[1], port: portInt, path: result[3]}, nil
		}
		return storageEndPoint{}, errors.New("Unable to parse endpoint " + ep)
	}
	// For *nix OSes
	parts := strings.Split(ep, ":")
	var parsedep storageEndPoint
	switch len(parts) {
	case 1:
		parsedep = storageEndPoint{path: parts[0]}
	case 2:
		parsedep = storageEndPoint{host: parts[0], port: defaultPort, path: parts[1]}
	case 3:
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return storageEndPoint{}, err
		}
		parsedep = storageEndPoint{host: parts[0], port: port, path: parts[2]}
	default:
		return storageEndPoint{}, errors.New("Unable to parse " + ep)
	}
	return parsedep, nil
}

// Parse an array of end-points (passed on the command line)
func parseStorageEndPoints(eps []string, defaultPort int) (endpoints []storageEndPoint, err error) {
	for _, ep := range eps {
		if ep == "" {
			continue
		}
		var endpoint storageEndPoint
		endpoint, err = parseStorageEndPoint(ep, defaultPort)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, nil
}

// getListenIPs - gets all the ips to listen on.
func getListenIPs(httpServerConf *http.Server) (hosts []string, port string, err error) {
	var host string
	host, port, err = net.SplitHostPort(httpServerConf.Addr)
	if err != nil {
		return nil, port, fmt.Errorf("Unable to parse host address %s", err)
	}
	if host == "" {
		var ipv4s []net.IP
		ipv4s, err = getInterfaceIPv4s()
		if err != nil {
			return nil, port, fmt.Errorf("Unable reverse sorted ips from hosts %s", err)
		}
		for _, ip := range ipv4s {
			hosts = append(hosts, ip.String())
		}
		return hosts, port, nil
	} // if host != "" {
	// Proceed to append itself, since user requested a specific endpoint.
	hosts = append(hosts, host)
	return hosts, port, nil
}

// Finalizes the endpoints based on the host list and port.
func finalizeEndpoints(tls bool, apiServer *http.Server) (endPoints []string) {
	// Verify current scheme.
	scheme := "http"
	if tls {
		scheme = "https"
	}

	// Get list of listen ips and port.
	hosts, port, err := getListenIPs(apiServer)
	fatalIf(err, "Unable to get list of ips to listen on")

	// Construct proper endpoints.
	for _, host := range hosts {
		endPoints = append(endPoints, fmt.Sprintf("%s://%s:%s", scheme, host, port))
	}

	// Success.
	return endPoints
}

// initServerConfig initialize server config.
func initServerConfig(c *cli.Context) {
	// Create certs path.
	err := createCertsPath()
	fatalIf(err, "Unable to create \"certs\" directory.")

	// Fetch max conn limit from environment variable.
	if maxConnStr := os.Getenv("MINIO_MAXCONN"); maxConnStr != "" {
		// We need to parse to its integer value.
		globalMaxConn, err = strconv.Atoi(maxConnStr)
		fatalIf(err, "Unable to convert MINIO_MAXCONN=%s environment variable into its integer value.", maxConnStr)
	}

	// Fetch max cache size from environment variable.
	if maxCacheSizeStr := os.Getenv("MINIO_CACHE_SIZE"); maxCacheSizeStr != "" {
		// We need to parse cache size to its integer value.
		globalMaxCacheSize, err = strconvBytes(maxCacheSizeStr)
		fatalIf(err, "Unable to convert MINIO_CACHE_SIZE=%s environment variable into its integer value.", maxCacheSizeStr)
	}

	// Fetch cache expiry from environment variable.
	if cacheExpiryStr := os.Getenv("MINIO_CACHE_EXPIRY"); cacheExpiryStr != "" {
		// We need to parse cache expiry to its time.Duration value.
		globalCacheExpiry, err = time.ParseDuration(cacheExpiryStr)
		fatalIf(err, "Unable to convert MINIO_CACHE_EXPIRY=%s environment variable into its time.Duration value.", cacheExpiryStr)
	}

	// When credentials inherited from the env, server cmd has to save them in the disk
	if os.Getenv("MINIO_ACCESS_KEY") != "" && os.Getenv("MINIO_SECRET_KEY") != "" {
		// Env credentials are already loaded in serverConfig, just save in the disk
		err = serverConfig.Save()
		fatalIf(err, "Unable to save credentials in the disk.")
	}

	// Set maxOpenFiles, This is necessary since default operating
	// system limits of 1024, 2048 are not enough for Minio server.
	setMaxOpenFiles()

	// Set maxMemory, This is necessary since default operating
	// system limits might be changed and we need to make sure we
	// do not crash the server so the set the maxCacheSize appropriately.
	setMaxMemory()

	// Do not fail if this is not allowed, lower limits are fine as well.
}

// Validate if input disks are sufficient for initializing XL.
func checkSufficientDisks(eps []storageEndPoint) error {
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

// Validate input disks.
func validateDisks(endPoints []storageEndPoint, ignoredEndPoints []storageEndPoint) []StorageAPI {
	isXL := len(endPoints) > 1
	if isXL {
		// Validate if input disks have duplicates in them.
		err := checkDuplicateEndPoints(endPoints)
		fatalIf(err, "Invalid disk arguments for server.")

		// Validate if input disks are sufficient for erasure coded setup.
		err = checkSufficientDisks(endPoints)
		fatalIf(err, "Invalid disk arguments for server.")
	}
	storageDisks, err := initStorageDisks(endPoints, ignoredEndPoints)
	fatalIf(err, "Unable to initialize storage disks.")
	return storageDisks
}

// Returns if slice of disks is a distributed setup.
func isDistributedSetup(eps []storageEndPoint) (isDist bool) {
	// Port to connect to for the lock servers in a distributed setup.
	for _, ep := range eps {
		if !isLocalStorage(ep) {
			// One or more disks supplied as arguments are not
			// attached to the local node.
			isDist = true
		}
	}
	return isDist
}

// serverMain handler called for 'minio server' command.
func serverMain(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}

	// Server address.
	serverAddr := c.String("address")

	// Check if requested port is available.
	host, portStr, err := net.SplitHostPort(serverAddr)
	fatalIf(err, "Unable to parse %s.", serverAddr)

	portInt, err := strconv.Atoi(portStr)
	fatalIf(err, "Invalid port number.")

	fatalIf(checkPortAvailability(portInt), "Port unavailable %d", portInt)

	// Saves host and port in a globally accessible value.
	globalMinioPort = portInt
	globalMinioHost = host

	// Disks to be ignored in server init, to skip format healing.
	ignoredDisks, err := parseStorageEndPoints(strings.Split(c.String("ignore-disks"), ","), portInt)
	fatalIf(err, "Unable to parse storage endpoints %s", strings.Split(c.String("ignore-disks"), ","))

	// Disks to be used in server init.
	disks, err := parseStorageEndPoints(c.Args(), portInt)
	fatalIf(err, "Unable to parse storage endpoints %s", c.Args())

	// Initialize server config.
	initServerConfig(c)

	// Check 'server' cli arguments.
	storageDisks := validateDisks(disks, ignoredDisks)

	// Cleanup objects that weren't successfully written into the namespace.
	fatalIf(houseKeeping(storageDisks), "Unable to purge temporary files.")

	// If https.
	tls := isSSL()

	// First disk argument check if it is local.
	firstDisk := isLocalStorage(disks[0])

	// Configure server.
	srvConfig := serverCmdConfig{
		serverAddr:       serverAddr,
		endPoints:        disks,
		ignoredEndPoints: ignoredDisks,
		storageDisks:     storageDisks,
		isDistXL:         isDistributedSetup(disks),
	}

	// Configure server.
	handler, err := configureServerHandler(srvConfig)
	fatalIf(err, "Unable to configure one of server's RPC services.")

	// Set nodes for dsync for distributed setup.
	if srvConfig.isDistXL {
		fatalIf(initDsyncNodes(disks), "Unable to initialize distributed locking")
	}

	// Initialize name space lock.
	initNSLock(srvConfig.isDistXL)

	// Initialize a new HTTP server.
	apiServer := NewServerMux(serverAddr, handler)

	// Fetch endpoints which we are going to serve from.
	endPoints := finalizeEndpoints(tls, &apiServer.Server)

	// Initialize local server address
	globalMinioAddr = getLocalAddress(srvConfig)

	// Initialize S3 Peers inter-node communication
	initGlobalS3Peers(disks)

	// Start server, automatically configures TLS if certs are available.
	go func(tls bool) {
		var lerr error
		if tls {
			lerr = apiServer.ListenAndServeTLS(mustGetCertFile(), mustGetKeyFile())
		} else {
			// Fallback to http.
			lerr = apiServer.ListenAndServe()
		}
		fatalIf(lerr, "Failed to start minio server.")
	}(tls)

	// Wait for formatting of disks.
	err = waitForFormatDisks(firstDisk, endPoints[0], storageDisks)
	fatalIf(err, "formatting storage disks failed")

	// Once formatted, initialize object layer.
	newObject, err := newObjectLayer(storageDisks)
	fatalIf(err, "intializing object layer failed")

	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(endPoints)

	// Waits on the server.
	<-globalServiceDoneCh
}
