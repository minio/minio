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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"regexp"
	"runtime"

	"github.com/minio/cli"
)

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: `Bind to a specific IP:PORT. Defaults to ":9000".`,
	},
	cli.StringFlag{
		Name:  "ignore-disks",
		Usage: `Comma separated list of faulty drives to ignore at startup.`,
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
     MINIO_ACCESS_KEY: Username or access key of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Password or secret key of 8 to 40 characters in length.

  CACHING:
     MINIO_CACHE_SIZE:   Limit maximum cache size. Allowed units are [GB|MB|KB]. Defaults to 8GB.
     MINIO_CACHE_EXPIRY: Automatically expire cached objects. Allowed units are [h|m|s]. Defaults to 72h.

  SECURITY:
     MINIO_SECURE_CONSOLE: Set secure console to 'no' to disable printing secret key. Defaults to 'yes'.

EXAMPLES:
  1. Start minio server on "/home/shared" directory.
      $ minio {{.Name}} /home/shared

  2. Start minio server bound to a specific IP:PORT.
      $ minio {{.Name}} --address 192.168.1.101:9000 /home/shared

  3. Start erasure coded minio server on a 12 disks server.
      $ minio {{.Name}} /mnt/export1/ /mnt/export2/ /mnt/export3/ /mnt/export4/ \
          /mnt/export5/ /mnt/export6/ /mnt/export7/ /mnt/export8/ /mnt/export9/ \
          /mnt/export10/ /mnt/export11/ /mnt/export12/

  4. Start erasure coded distributed minio server on a 4 node setup with 1 drive each. Run following commands on all the 4 nodes.
      $ export MINIO_ACCESS_KEY=minio
      $ export MINIO_SECRET_KEY=miniostorage
      $ minio {{.Name}} http://192.168.1.11/mnt/export/ http://192.168.1.12/mnt/export/ \
          http://192.168.1.13/mnt/export/ http://192.168.1.14/mnt/export/

`,
}

type serverCmdConfig struct {
	serverAddr       string
	endpoints        []*url.URL
	ignoredEndpoints []*url.URL
	storageDisks     []StorageAPI
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
					errorIf(fmt.Errorf("Invalid argument %s, port configurable using --address :<port>", u.Host), "")
					return nil, errInvalidArgument
				}
				u.Host = net.JoinHostPort(u.Host, globalMinioPort)
			} else {
				// For ex.: minio server --address host:port host1:port1 host2:port2...
				// i.e if "--address host:port" is specified
				// port info in u.Host is mandatory else return error.
				if port == "" {
					errorIf(fmt.Errorf("Invalid argument %s, port mandatory when --address <host>:<port> is used", u.Host), "")
					return nil, errInvalidArgument
				}
			}
		}
		endpoints = append(endpoints, u)
	}
	return endpoints, nil
}

// getListenIPs - gets all the ips to listen on.
func getListenIPs(serverAddr string) (hosts []string, port string, err error) {
	var host string
	host, port, err = net.SplitHostPort(serverAddr)
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
	hosts, port, err := getListenIPs(apiServer.Addr)
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

// We just exit for invalid endpoints.
func checkEndpointsSyntax(eps []*url.URL, disks []string) {
	for i, u := range eps {
		switch u.Scheme {
		case "", "http", "https":
			// Scheme is "" for FS and singlenode-XL, hence pass.
		default:
			if runtime.GOOS == "windows" {
				// On windows for "C:\export" scheme will be "C"
				matched, err := regexp.MatchString("^[a-zA-Z]$", u.Scheme)
				fatalIf(err, "Parsing scheme : %s (%s)", u.Scheme, disks[i])
				if matched {
					break
				}
			}
			fatalIf(errInvalidArgument, "Invalid scheme : %s (%s)", u.Scheme, disks[i])
		}
		if runtime.GOOS == "windows" {
			if u.Scheme == "http" || u.Scheme == "https" {
				// "http://server1/" is not allowed
				if u.Path == "" || u.Path == "/" || u.Path == "\\" {
					fatalIf(errInvalidArgument, "Empty path for %s", disks[i])
				}
			}
		} else {
			if u.Scheme == "http" || u.Scheme == "https" {
				// "http://server1/" is not allowed.
				if u.Path == "" || u.Path == "/" {
					fatalIf(errInvalidArgument, "Empty path for %s", disks[i])
				}
			} else {
				// "/" is not allowed.
				if u.Path == "" || u.Path == "/" {
					fatalIf(errInvalidArgument, "Empty path for %s", disks[i])
				}
			}
		}
	}

	if err := checkDuplicateEndpoints(eps); err != nil {
		fatalIf(errInvalidArgument, "Duplicate entries in %s", strings.Join(disks, " "))
	}
}

// Make sure all the command line parameters are OK and exit in case of invalid parameters.
func checkServerSyntax(c *cli.Context) {
	serverAddr := c.String("address")

	host, portStr, err := net.SplitHostPort(serverAddr)
	fatalIf(err, "Unable to parse %s.", serverAddr)

	// Verify syntax for all the XL disks.
	disks := c.Args()
	endpoints, err := parseStorageEndpoints(disks)
	fatalIf(err, "Unable to parse storage endpoints %s", disks)
	checkEndpointsSyntax(endpoints, disks)

	if len(endpoints) > 1 {
		// For XL setup.
		err = checkSufficientDisks(endpoints)
		fatalIf(err, "Storage endpoint error.")
	}

	// Verify syntax for all the ignored disks.
	var ignoredEndpoints []*url.URL
	ignoredDisksStr := c.String("ignore-disks")
	if ignoredDisksStr != "" {
		ignoredDisks := strings.Split(ignoredDisksStr, ",")
		if len(endpoints) == 1 {
			fatalIf(errInvalidArgument, "--ignore-disks is valid only for XL setup.")
		}
		ignoredEndpoints, err = parseStorageEndpoints(ignoredDisks)
		fatalIf(err, "Unable to parse ignored storage endpoints %s", ignoredDisks)
		checkEndpointsSyntax(ignoredEndpoints, ignoredDisks)

		for i, ep := range ignoredEndpoints {
			// An ignored disk should be present in the XL disks list.
			if !containsEndpoint(endpoints, ep) {
				fatalIf(errInvalidArgument, "Ignored storage %s not available in the list of erasure storages list.", disks[i])
			}
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

	tls := isSSL()
	for _, ep := range endpoints {
		if ep.Scheme == "https" && !tls {
			// Certificates should be provided for https configuration.
			fatalIf(errInvalidArgument, "Certificates not provided for https")
		}
	}
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
	globalMinioHost = host

	// Check if requested port is available.
	fatalIf(checkPortAvailability(portStr), "Port unavailable %s", portStr)
	globalMinioPort = portStr

	// Check server syntax and exit in case of errors.
	// Done after globalMinioHost and globalMinioPort is set as parseStorageEndpoints()
	// depends on it.
	checkServerSyntax(c)

	// Disks to be ignored in server init, to skip format healing.
	var ignoredEndpoints []*url.URL
	if len(c.String("ignore-disks")) > 0 {
		ignoredEndpoints, err = parseStorageEndpoints(strings.Split(c.String("ignore-disks"), ","))
		fatalIf(err, "Unable to parse storage endpoints %s", strings.Split(c.String("ignore-disks"), ","))
	}

	// Disks to be used in server init.
	endpoints, err := parseStorageEndpoints(c.Args())
	fatalIf(err, "Unable to parse storage endpoints %s", c.Args())

	storageDisks, err := initStorageDisks(endpoints, ignoredEndpoints)
	fatalIf(err, "Unable to initialize storage disks.")

	// Cleanup objects that weren't successfully written into the namespace.
	fatalIf(houseKeeping(storageDisks), "Unable to purge temporary files.")

	// Initialize server config.
	initServerConfig(c)

	// First disk argument check if it is local.
	firstDisk := isLocalStorage(endpoints[0])

	// Check if endpoints are part of distributed setup.
	globalIsDistXL = isDistributedSetup(endpoints)

	// Configure server.
	srvConfig := serverCmdConfig{
		serverAddr:       serverAddr,
		endpoints:        endpoints,
		ignoredEndpoints: ignoredEndpoints,
		storageDisks:     storageDisks,
	}

	// Configure server.
	handler, err := configureServerHandler(srvConfig)
	fatalIf(err, "Unable to configure one of server's RPC services.")

	// Set nodes for dsync for distributed setup.
	if globalIsDistXL {
		fatalIf(initDsyncNodes(endpoints), "Unable to initialize distributed locking")
	}

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	// Initialize a new HTTP server.
	apiServer := NewServerMux(serverAddr, handler)

	// If https.
	tls := isSSL()

	// Fetch endpoints which we are going to serve from.
	endPoints := finalizeEndpoints(tls, apiServer.Server)

	// Initialize local server address
	globalMinioAddr = getLocalAddress(srvConfig)

	// Initialize S3 Peers inter-node communication
	initGlobalS3Peers(endpoints)

	// Start server, automatically configures TLS if certs are available.
	go func(tls bool) {
		var lerr error
		cert, key := "", ""
		if tls {
			cert, key = mustGetCertFile(), mustGetKeyFile()
		}
		lerr = apiServer.ListenAndServe(cert, key)
		fatalIf(lerr, "Failed to start minio server.")
	}(tls)

	// Wait for formatting of disks.
	err = waitForFormatDisks(firstDisk, endpoints, storageDisks)
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
