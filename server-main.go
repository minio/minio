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

package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var serverCmd = cli.Command{
	Name:  "server",
	Usage: "Start object storage server.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "address",
			Value: ":9000",
		},
	},
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Usage}}

USAGE:
  minio {{.Name}} [OPTIONS] PATH [PATH...]

OPTIONS:
  {{range .Flags}}{{.}}
  {{end}}
ENVIRONMENT VARIABLES:
  MINIO_ACCESS_KEY: Access key string of 5 to 20 characters in length.
  MINIO_SECRET_KEY: Secret key string of 8 to 40 characters in length.

EXAMPLES:
  1. Start minio server.
      $ minio {{.Name}} /home/shared

  2. Start minio server bound to a specific IP:PORT, when you have multiple network interfaces.
      $ minio {{.Name}} --address 192.168.1.101:9000 /home/shared

  3. Start minio server on Windows.
      $ minio {{.Name}} C:\MyShare

  4. Start minio server 12 disks to enable erasure coded layer with 6 data and 6 parity.
      $ minio {{.Name}} /mnt/export1/backend /mnt/export2/backend /mnt/export3/backend /mnt/export4/backend \
          /mnt/export5/backend /mnt/export6/backend /mnt/export7/backend /mnt/export8/backend /mnt/export9/backend \
          /mnt/export10/backend /mnt/export11/backend /mnt/export12/backend
`,
}

type serverCmdConfig struct {
	serverAddr  string
	exportPaths []string
}

// configureServer configure a new server instance
func configureServer(srvCmdConfig serverCmdConfig) *http.Server {
	// Minio server config
	apiServer := &http.Server{
		Addr: srvCmdConfig.serverAddr,
		// Adding timeout of 10 minutes for unresponsive client connections.
		ReadTimeout:    10 * time.Minute,
		WriteTimeout:   10 * time.Minute,
		Handler:        configureServerHandler(srvCmdConfig),
		MaxHeaderBytes: 1 << 20,
	}

	// Returns configured HTTP server.
	return apiServer
}

// getListenIPs - gets all the ips to listen on.
func getListenIPs(httpServerConf *http.Server) (hosts []string, port string) {
	host, port, err := net.SplitHostPort(httpServerConf.Addr)
	fatalIf(err, "Unable to parse host port.")

	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		fatalIf(err, "Unable to determine network interface address.")
		for _, addr := range addrs {
			if addr.Network() == "ip+net" {
				host := strings.Split(addr.String(), "/")[0]
				if ip := net.ParseIP(host); ip.To4() != nil {
					hosts = append(hosts, host)
				}
			}
		}
	}
	return hosts, port
}

// Print listen ips.
func printListenIPs(tls bool, hosts []string, port string) {
	for _, host := range hosts {
		if tls {
			console.Printf("    https://%s:%s\n", host, port)
		} else {
			console.Printf("    http://%s:%s\n", host, port)
		}
	}
}

// initServerConfig initialize server config.
func initServerConfig(c *cli.Context) {
	// Save new config.
	err := serverConfig.Save()
	fatalIf(err, "Unable to save config.")

	// Fetch max conn limit from environment variable.
	if maxConnStr := os.Getenv("MINIO_MAXCONN"); maxConnStr != "" {
		// We need to parse to its integer value.
		var err error
		globalMaxConn, err = strconv.Atoi(maxConnStr)
		fatalIf(err, "Unable to convert MINIO_MAXCONN=%s environment variable into its integer value.", maxConnStr)
	}

	// Fetch max cache size from environment variable.
	if maxCacheSizeStr := os.Getenv("MINIO_CACHE_SIZE"); maxCacheSizeStr != "" {
		// We need to parse cache size to its integer value.
		var err error
		globalMaxCacheSize, err = strconvBytes(maxCacheSizeStr)
		fatalIf(err, "Unable to convert MINIO_CACHE_SIZE=%s environment variable into its integer value.", maxCacheSizeStr)
	}

	// Fetch cache expiry from environment variable.
	if cacheExpiryStr := os.Getenv("MINIO_CACHE_EXPIRY"); cacheExpiryStr != "" {
		// We need to parse cache expiry to its time.Duration value.
		var err error
		globalCacheExpiry, err = time.ParseDuration(cacheExpiryStr)
		fatalIf(err, "Unable to convert MINIO_CACHE_EXPIRY=%s environment variable into its time.Duration value.", cacheExpiryStr)
	}

	// Fetch access keys from environment variables if any and update the config.
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	// Validate if both keys are specified and they are valid save them.
	if accessKey != "" && secretKey != "" {
		if !isValidAccessKey.MatchString(accessKey) {
			fatalIf(errInvalidArgument, "Invalid access key.")
		}
		if !isValidSecretKey.MatchString(secretKey) {
			fatalIf(errInvalidArgument, "Invalid secret key.")
		}
		serverConfig.SetCredential(credential{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		})
	}

	// Set maxOpenFiles, This is necessary since default operating
	// system limits of 1024, 2048 are not enough for Minio server.
	setMaxOpenFiles()
	// Do not fail if this is not allowed, lower limits are fine as well.
}

// Check server arguments.
func checkServerSyntax(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}
}

// Extract port number from address address should be of the form host:port.
func getPort(address string) int {
	_, portStr, err := net.SplitHostPort(address)
	fatalIf(err, "Unable to parse host port.")
	portInt, err := strconv.Atoi(portStr)
	fatalIf(err, "Invalid port number.")
	return portInt
}

// Make sure that none of the other processes are listening on the
// specified port on any of the interfaces.
//
// On linux if a process is listening on 127.0.0.1:9000 then Listen()
// on ":9000" fails with the error "port already in use".
// However on Mac OSX Listen() on ":9000" falls back to the IPv6 address.
// This causes confusion on Mac OSX that minio server is not reachable
// on 127.0.0.1 even though minio server is running. So before we start
// the minio server we make sure that the port is free on all the IPs.
func checkPortAvailability(port int) {
	isAddrInUse := func(err error) bool {
		// Check if the syscall error is EADDRINUSE.
		// EADDRINUSE is the system call error if another process is
		// already listening at the specified port.
		neterr, ok := err.(*net.OpError)
		if !ok {
			return false
		}
		osErr, ok := neterr.Err.(*os.SyscallError)
		if !ok {
			return false
		}
		sysErr, ok := osErr.Err.(syscall.Errno)
		if !ok {
			return false
		}
		if sysErr != syscall.EADDRINUSE {
			return false
		}
		return true
	}
	ifcs, err := net.Interfaces()
	if err != nil {
		fatalIf(err, "Unable to list interfaces.")
	}
	for _, ifc := range ifcs {
		addrs, err := ifc.Addrs()
		if err != nil {
			fatalIf(err, "Unable to list addresses on interface %s.", ifc.Name)
		}
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				errorIf(errors.New(""), "Failed to assert type on (*net.IPNet) interface.")
				continue
			}
			ip := ipnet.IP
			network := "tcp4"
			if ip.To4() == nil {
				network = "tcp6"
			}
			tcpAddr := net.TCPAddr{IP: ip, Port: port, Zone: ifc.Name}
			l, err := net.ListenTCP(network, &tcpAddr)
			if err != nil {
				if isAddrInUse(err) {
					// Fail if port is already in use.
					fatalIf(err, "Unable to listen on %s:%.d.", tcpAddr.IP, tcpAddr.Port)
				} else {
					// Ignore other errors.
					continue
				}
			}
			if err = l.Close(); err != nil {
				fatalIf(err, "Unable to close listener on %s:%.d.", tcpAddr.IP, tcpAddr.Port)
			}
		}
	}
}

func serverMain(c *cli.Context) {
	// check 'server' cli arguments.
	checkServerSyntax(c)

	// Initialize server config.
	initServerConfig(c)

	// If https.
	tls := isSSL()

	// Server address.
	serverAddress := c.String("address")

	host, port, _ := net.SplitHostPort(serverAddress)
	// If port empty, default to port '80'
	if port == "" {
		port = "80"
		// if SSL is enabled, choose port as "443" instead.
		if tls {
			port = "443"
		}
	}

	// Check if requested port is available.
	checkPortAvailability(getPort(net.JoinHostPort(host, port)))

	// Save all command line args as export paths.
	exportPaths := c.Args()

	// Configure server.
	apiServer := configureServer(serverCmdConfig{
		serverAddr:  serverAddress,
		exportPaths: exportPaths,
	})

	// Credential.
	cred := serverConfig.GetCredential()

	// Region.
	region := serverConfig.GetRegion()

	// Print credentials and region.
	console.Println("\n" + cred.String() + "  " + colorMagenta("Region: ") + colorWhite(region))

	hosts, port := getListenIPs(apiServer) // get listen ips and port.

	console.Println("\nMinio Object Storage:")
	// Print api listen ips.
	printListenIPs(tls, hosts, port)

	console.Println("\nMinio Browser:")
	// Print browser listen ips.
	printListenIPs(tls, hosts, port)

	console.Println("\nTo configure Minio Client:")

	// Figure out right endpoint for 'mc'.
	endpoint := fmt.Sprintf("http://%s:%s", hosts[0], port)
	if tls {
		endpoint = fmt.Sprintf("https://%s:%s", hosts[0], port)
	}

	// Download 'mc' info.
	if runtime.GOOS == "windows" {
		console.Printf("    Download 'mc' from https://dl.minio.io/client/mc/release/%s-%s/mc.exe\n", runtime.GOOS, runtime.GOARCH)
		console.Printf("    $ mc.exe config host add myminio %s %s %s\n", endpoint, cred.AccessKeyID, cred.SecretAccessKey)
	} else {
		console.Printf("    $ wget https://dl.minio.io/client/mc/release/%s-%s/mc\n", runtime.GOOS, runtime.GOARCH)
		console.Printf("    $ chmod 755 mc\n")
		console.Printf("    $ ./mc config host add myminio %s %s %s\n", endpoint, cred.AccessKeyID, cred.SecretAccessKey)
	}

	// Start server.
	var err error
	// Configure TLS if certs are available.
	if isSSL() {
		err = apiServer.ListenAndServeTLS(mustGetCertFile(), mustGetKeyFile())
	} else {
		// Fallback to http.
		err = apiServer.ListenAndServe()
	}
	fatalIf(err, "Failed to start minio server.")
}
