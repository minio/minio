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
	"sort"
	"strings"
	"time"

	"runtime"

	"github.com/minio/cli"
)

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: `Bind to a specific IP:PORT. Defaults to ":9000".`,
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
     MINIO_ACCESS_KEY: Custom username or access key of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Custom password or secret key of 8 to 40 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

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

	// Create certs path.
	fatalIf(createCertsPath(), "Unable to create \"certs\" directory.")

	// Load user supplied root CAs
	loadRootCAs()

	// Set maxOpenFiles, This is necessary since default operating
	// system limits of 1024, 2048 are not enough for Minio server.
	setMaxOpenFiles()

	// Set maxMemory, This is necessary since default operating
	// system limits might be changed and we need to make sure we
	// do not crash the server so the set the maxCacheSize appropriately.
	setMaxMemory()

	// Do not fail if this is not allowed, lower limits are fine as well.
}

// Returns true if path is empty, or equals to '.', '/', '\' characters.
func isPathSentinel(path string) bool {
	return path == "" || path == "." || path == "/" || path == `\`
}

// Returned when path is empty or root path.
var errEmptyRootPath = errors.New("Empty or root path is not allowed")

// Invalid scheme passed.
var errInvalidScheme = errors.New("Invalid scheme")

// Returned when there are no ports.
var errEmptyPort = errors.New("Port cannot be empty or '0', please use `--address` to pick a specific port")

// serverMain handler called for 'minio server' command.
func serverMain(c *cli.Context) {
	args := c.Args()
	if !args.Present() || args.First() == "help" {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}

	serverAddr := c.String("address")

	// Verify syntax of serverAddr and args.
	setup, err := NewSetup(serverAddr, args...)
	fatalIf(err, "Command line argument error")

	// Initializes server config, certs, logging and system settings.
	initServerConfig(c)

	// Check for new updates from dl.minio.io.
	checkUpdate()

	globalSetup = setup
	globalMinioHost, globalMinioPort = mustSplitHostPort(setup.ServerAddr())
	if runtime.GOOS == "darwin" {
		// On macOS, if a process already listens on 127.0.0.1:PORT, net.Listen() falls back
		// to IPv6 address ie minio will start listening on IPv6 address whereas another
		// (non-)minio process is listening on IPv4 of given port.
		// To avoid this error sutiation we check for port availability only for macOS.
		err = checkPortAvailability(globalMinioPort)
		fatalIf(err, "Port already in use", globalMinioPort)
	}

	// Disks to be used in server init.
	endpoints, err := parseStorageEndpoints(args)
	fatalIf(err, "Unable to parse storage endpoints %s", args)

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

	// Check if endpoints are part of distributed XL setup.
	if globalSetup.Type() == DistXLSetupType {
		err = initDsyncNodes(endpoints)
		fatalIf(err, "Unable to initialize distributed locking clients")
		// TODO: remove globalIsDistXL
		globalIsDistXL = true
	} else if globalSetup.Type() == XLSetupType {
		// TODO: remove globalIsXL
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
			cert, key = mustGetCertFile(), mustGetKeyFile()
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

	// Set startup time
	globalBootTime = time.Now().UTC()

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(apiEndPoints)

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
