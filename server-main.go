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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/minhttp"
)

var serverCmd = cli.Command{
	Name:  "server",
	Usage: "Start Minio cloud storage server.",
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
  minio {{.Name}} [OPTIONS] PATH

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

  4. Start minio server 8 disks to enable erasure coded layer with 4 data and 4 parity.
      $ minio {{.Name}} /mnt/export1/backend /mnt/export2/backend /mnt/export3/backend /mnt/export4/backend \
          /mnt/export5/backend /mnt/export6/backend /mnt/export7/backend /mnt/export8/backend
`,
}

type serverCmdConfig struct {
	serverAddr  string
	exportPaths []string
}

// configureHTTPServer configure a new HTTP server instance.
func configureHTTPServer(srvCmdConfig serverCmdConfig) *http.Server {
	// Minio server config
	apiServer := &http.Server{
		Addr:           srvCmdConfig.serverAddr,
		Handler:        configureServerHandler(srvCmdConfig),
		MaxHeaderBytes: 1 << 20,
	}

	// Returns configured HTTP server.
	return apiServer
}

// configureHTTPSServer configure a new HTTPS server instance.
func configureHTTPSServer(srvCmdConfig serverCmdConfig) *http.Server {
	// Minio server config
	apiServer := &http.Server{
		Addr:           srvCmdConfig.serverAddr,
		Handler:        configureServerHandler(srvCmdConfig),
		MaxHeaderBytes: 1 << 20,
	}

	// Configure TLS if certs are available.
	if isSSL() {
		var err error
		apiServer.TLSConfig = &tls.Config{}
		apiServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		apiServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(mustGetCertFile(), mustGetKeyFile())
		fatalIf(err, "Unable to load certificates.", nil)
	}

	// Returns configured HTTPS server.
	return apiServer
}

// getListenIPs - gets all the ips to listen on.
func getListenIPs(httpServerConf *http.Server) (hosts []string, port string) {
	host, port, err := net.SplitHostPort(httpServerConf.Addr)
	fatalIf(err, "Unable to split host port.", nil)

	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		fatalIf(err, "Unable to get interface address.", nil)
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
	fatalIf(err, "Unable to save config.", nil)

	// Fetch access keys from environment variables if any and update the config.
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	// Validate if both keys are specified and they are valid save them.
	if accessKey != "" && secretKey != "" {
		if !isValidAccessKey.MatchString(accessKey) {
			fatalIf(errInvalidArgument, "Access key does not have required length", nil)
		}
		if !isValidSecretKey.MatchString(secretKey) {
			fatalIf(errInvalidArgument, "Secret key does not have required length", nil)
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
		fatalIf(err, "Unable to list interfaces.", nil)
	}
	for _, ifc := range ifcs {
		addrs, err := ifc.Addrs()
		if err != nil {
			fatalIf(err, fmt.Sprintf("Unable to list addresses on interface %s.", ifc.Name), nil)
		}
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				errorIf(errors.New(""), "Interface type assertion to (*net.IPNet) failed.", nil)
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
					errMsg := fmt.Sprintf("Unable to listen on IP %s, port %.d", tcpAddr.IP, tcpAddr.Port)
					fatalIf(err, errMsg, nil)
				} else {
					// Ignore other errors.
					continue
				}
			}
			if err = l.Close(); err != nil {
				errMsg := fmt.Sprintf("Unable to close listener on IP %s, port %.d", tcpAddr.IP, tcpAddr.Port)
				fatalIf(err, errMsg, nil)
			}
			// If SSL is enabled validate additional ports.
			if isSSL() {
				// For port inet HTTP port, use default inet HTTPS port.
				if port == 80 {
					port = 443
				} else {
					// For all other ports just verify the next
					// available one.
					port++
				}
				tcpAddr := net.TCPAddr{IP: ip, Port: port, Zone: ifc.Name}
				l, err := net.ListenTCP(network, &tcpAddr)
				if err != nil {
					if isAddrInUse(err) {
						// Fail if port is already in use.
						errMsg := fmt.Sprintf("Unable to listen on IP %s, port %.d", tcpAddr.IP, tcpAddr.Port)
						fatalIf(err, errMsg, nil)
					} else {
						// Ignore other errors.
						continue
					}
				}
				if err = l.Close(); err != nil {
					errMsg := fmt.Sprintf("Unable to close listener on IP %s, port %.d", tcpAddr.IP, tcpAddr.Port)
					fatalIf(err, errMsg, nil)
				}
			}
		}
	}
}

func serverMain(c *cli.Context) {
	// Check 'server' cli arguments.
	checkServerSyntax(c)

	// Initialize server config.
	initServerConfig(c)

	// Server address.
	serverAddress := c.String("address")

	_, port, _ := net.SplitHostPort(serverAddress)
	// If port empty, default to port '80'
	if port == "" {
		port = "80"
	}

	portInt, err := strconv.Atoi(port)
	fatalIf(err, "Invalid port number.", nil)

	// Check if requested port is available, if not fail.
	checkPortAvailability(portInt)

	// Save all command line args as export paths.
	exportPaths := c.Args()

	// Configure HTTP server.
	apiHTTPServer := configureHTTPServer(serverCmdConfig{
		serverAddr:  serverAddress,
		exportPaths: exportPaths,
	})
	var apiHTTPSServer *http.Server
	// Configure HTTPS server.
	if isSSL() {
		apiHTTPSServer = configureHTTPSServer(serverCmdConfig{
			serverAddr:  serverAddress,
			exportPaths: exportPaths,
		})
	}
	// Credential.
	cred := serverConfig.GetCredential()

	// Region.
	region := serverConfig.GetRegion()

	// Print credentials and region.
	console.Println("\n" + cred.String() + "  " + colorMagenta("Region: ") + colorWhite(region))

	// Get http listen ips and port.
	httpHosts, httpPort := getListenIPs(apiHTTPServer)

	console.Println("\nMinio Object Storage:")
	// Print HTTP listen ips.
	printListenIPs(false, httpHosts, httpPort)
	var httpsHosts []string
	var httpsPort string
	if isSSL() {
		httpsHosts, httpsPort = getListenIPs(apiHTTPSServer)
		printListenIPs(true, httpsHosts, httpsPort)
	}

	console.Println("\nMinio Browser:")
	// Print HTTP listen ips.
	printListenIPs(false, httpHosts, httpPort)
	if isSSL() {
		httpsHosts, httpsPort = getListenIPs(apiHTTPSServer)
		printListenIPs(true, httpsHosts, httpsPort)
	}

	console.Println("\nTo configure Minio Client:")
	// Figure out right endpoint for 'mc'.
	endpoint := fmt.Sprintf("http://%s:%s", httpHosts[0], httpPort)
	if isSSL() {
		endpoint = fmt.Sprintf("https://%s:%s", httpsHosts[0], httpsPort)
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
	if apiHTTPSServer != nil {
		err = minhttp.ListenAndServe(apiHTTPServer, apiHTTPSServer)
	} else {
		err = minhttp.ListenAndServe(apiHTTPServer)
	}
	errorIf(err, "Failed to start the minio server.", nil)
}
