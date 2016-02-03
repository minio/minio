/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/minio-xl/pkg/minhttp"
	"github.com/minio/minio-xl/pkg/probe"
)

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "Start Minio cloud storage server.",
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Usage}}

USAGE:
  minio {{.Name}} [OPTION VALUE] PATH

  OPTION = min-free-disk VALUE = NN% [DEFAULT: 10%]

EXAMPLES:
  1. Start minio server on Linux.
      $ minio {{.Name}} /home/shared

  2. Start minio server on Windows.
      $ minio {{.Name}} C:\MyShare

  3. Start minio server bound to a specific IP:PORT, when you have multiple network interfaces.
      $ minio --address 192.168.1.101:9000 {{.Name}} /home/shared

  4. Start minio server with minimum free disk threshold to 5%
      $ minio {{.Name}} min-free-disk 5% /home/shared/Pictures

`,
}

// cloudServerConfig - http server config
type cloudServerConfig struct {
	/// HTTP server options
	Address   string // Address:Port listening
	AccessLog bool   // Enable access log handler

	// Credentials.
	AccessKeyID     string // Access key id.
	SecretAccessKey string // Secret access key.

	/// FS options
	Path        string // Path to export for cloud storage
	MinFreeDisk int64  // Minimum free disk space for filesystem

	/// TLS service
	TLS      bool   // TLS on when certs are specified
	CertFile string // Domain certificate
	KeyFile  string // Domain key
}

func configureWebServer(conf cloudServerConfig) (*http.Server, *probe.Error) {
	// Split the api address into host and port.
	host, port, e := net.SplitHostPort(conf.Address)
	if e != nil {
		return nil, probe.NewError(e)
	}
	webPort, e := strconv.Atoi(port)
	if e != nil {
		return nil, probe.NewError(e)
	}
	// Always choose the next port, based on the API address port.
	webPort = webPort + 1
	webAddress := net.JoinHostPort(host, strconv.Itoa(webPort))

	// Minio server config
	webServer := &http.Server{
		Addr:           webAddress,
		Handler:        getWebAPIHandler(getNewWebAPI(conf)),
		MaxHeaderBytes: 1 << 20,
	}

	if conf.TLS {
		var err error
		webServer.TLSConfig = &tls.Config{}
		webServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		webServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, probe.NewError(err)
		}
	}
	return webServer, nil
}

// configureAPIServer configure a new server instance
func configureAPIServer(conf cloudServerConfig) (*http.Server, *probe.Error) {
	// Minio server config
	apiServer := &http.Server{
		Addr:           conf.Address,
		Handler:        getCloudStorageAPIHandler(getNewCloudStorageAPI(conf)),
		MaxHeaderBytes: 1 << 20,
	}

	if conf.TLS {
		var err error
		apiServer.TLSConfig = &tls.Config{}
		apiServer.TLSConfig.Certificates = make([]tls.Certificate, 1)
		apiServer.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, probe.NewError(err)
		}
	}
	return apiServer, nil
}

func printServerMsg(serverConf *http.Server) {
	host, port, e := net.SplitHostPort(serverConf.Addr)
	fatalIf(probe.NewError(e), "Unable to split host port.", nil)

	var hosts []string
	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, e := net.InterfaceAddrs()
		fatalIf(probe.NewError(e), "Unable to get interface address.", nil)

		for _, addr := range addrs {
			if addr.Network() == "ip+net" {
				host := strings.Split(addr.String(), "/")[0]
				if ip := net.ParseIP(host); ip.To4() != nil {
					hosts = append(hosts, host)
				}
			}
		}
	}
	for _, host := range hosts {
		if serverConf.TLSConfig != nil {
			Printf("    https://%s:%s\n", host, port)
		} else {
			Printf("    http://%s:%s\n", host, port)
		}
	}
}

// parse input string with percent to int64
func parsePercentToInt(s string, bitSize int) (int64, *probe.Error) {
	i := strings.Index(s, "%")
	if i < 0 {
		// no percentage string found try to parse the whole string anyways
		p, err := strconv.ParseInt(s, 10, bitSize)
		if err != nil {
			return 0, probe.NewError(err)
		}
		return p, nil
	}
	p, err := strconv.ParseInt(s[:i], 10, bitSize)
	if err != nil {
		return 0, probe.NewError(err)
	}
	return p, nil
}
func setLogger(conf *configV2) *probe.Error {
	if conf.IsMongoLoggingEnabled() {
		err := log2Mongo(conf.MongoLogger.Addr, conf.MongoLogger.DB, conf.MongoLogger.Collection)
		if err != nil {
			return err.Trace()
		}
	}
	if conf.IsSysloggingEnabled() {
		err := log2Syslog(conf.SyslogLogger.Network, conf.SyslogLogger.Addr)
		if err != nil {
			return err.Trace()
		}
	}
	if conf.IsFileLoggingEnabled() {
		err := log2File(conf.FileLogger.Filename)
		if err != nil {
			return err.Trace()
		}
	}
	return nil
}

// Generates config if it doesn't exist, otherwise returns back the saved ones.
func getConfig() (*configV2, *probe.Error) {
	if err := createConfigPath(); err != nil {
		return nil, err.Trace()
	}
	config, err := loadConfigV2()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			// Initialize new config, since config file doesn't exist yet
			config = &configV2{}
			config.Version = "2"
			config.Credentials.AccessKeyID = string(mustGenerateAccessKeyID())
			config.Credentials.SecretAccessKey = string(mustGenerateSecretAccessKey())
			config.Credentials.Region = "us-east-1"
			if err = saveConfig(config); err != nil {
				return nil, err.Trace()
			}
			return config, nil
		}
		return nil, err.Trace()
	}
	return config, nil
}

type accessKeys struct {
	*configV2
}

func (a accessKeys) String() string {
	magenta := color.New(color.FgMagenta, color.Bold).SprintFunc()
	white := color.New(color.FgWhite, color.Bold).SprintfFunc()
	return fmt.Sprint(magenta("AccessKey: ") + white(a.Credentials.AccessKeyID) + "  " + magenta("SecretKey: ") + white(a.Credentials.SecretAccessKey) + "  " + magenta("Region: ") + white(a.Credentials.Region))
}

// JSON - json formatted output
func (a accessKeys) JSON() string {
	b, err := json.Marshal(a)
	errorIf(probe.NewError(err), "Unable to marshal json", nil)
	return string(b)
}

// initServer initialize server
func initServer() (*configV2, *probe.Error) {
	conf, err := getConfig()
	if err != nil {
		return nil, err.Trace()
	}
	if err := setLogger(conf); err != nil {
		return nil, err.Trace()
	}
	if conf != nil {
		Println()
		Println(accessKeys{conf})
	}
	return conf, nil
}

func checkServerSyntax(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}
	if len(c.Args()) > 5 {
		fatalIf(probe.NewError(errInvalidArgument), "Unnecessary arguments passed. Please refer ‘mc server help’", nil)
	}
	path := strings.TrimSpace(c.Args().Last())
	if path == "" {
		fatalIf(probe.NewError(errInvalidArgument), "Path argument cannot be empty.", nil)
	}
}

func serverMain(c *cli.Context) {
	checkServerSyntax(c)

	conf, perr := initServer()
	fatalIf(perr.Trace(), "Failed to read config for minio.", nil)

	certFile := c.GlobalString("cert")
	keyFile := c.GlobalString("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		fatalIf(probe.NewError(errInvalidArgument), "Both certificate and key are required to enable https.", nil)
	}

	var minFreeDisk int64
	minFreeDiskSet := false
	// Default
	minFreeDisk = 10

	args := c.Args()
	for len(args) >= 2 {
		switch args.First() {
		case "min-free-disk":
			if minFreeDiskSet {
				fatalIf(probe.NewError(errInvalidArgument), "Minimum free disk should be set only once.", nil)
			}
			args = args.Tail()
			var err *probe.Error
			minFreeDisk, err = parsePercentToInt(args.First(), 64)
			fatalIf(err.Trace(args.First()), "Invalid minium free disk size "+args.First()+" passed.", nil)
			args = args.Tail()
			minFreeDiskSet = true
		default:
			cli.ShowCommandHelpAndExit(c, "server", 1) // last argument is exit code
		}
	}

	path := strings.TrimSpace(c.Args().Last())
	// Last argument is always path
	if _, err := os.Stat(path); err != nil {
		fatalIf(probe.NewError(err), "Unable to validate the path", nil)
	}
	tls := (certFile != "" && keyFile != "")
	serverConfig := cloudServerConfig{
		Address:         c.GlobalString("address"),
		AccessLog:       c.GlobalBool("enable-accesslog"),
		AccessKeyID:     conf.Credentials.AccessKeyID,
		SecretAccessKey: conf.Credentials.SecretAccessKey,
		Path:            path,
		MinFreeDisk:     minFreeDisk,
		TLS:             tls,
		CertFile:        certFile,
		KeyFile:         keyFile,
	}

	// configure API server.
	apiServer, err := configureAPIServer(serverConfig)
	errorIf(err.Trace(), "Failed to configure API server.", nil)

	Println("\nMinio Object Storage:")
	printServerMsg(apiServer)

	// configure Web server.
	webServer, err := configureWebServer(serverConfig)
	errorIf(err.Trace(), "Failed to configure Web server.", nil)

	Println("\nMinio Browser:")
	printServerMsg(webServer)

	Println("\nTo configure Minio Client:")
	if runtime.GOOS == "windows" {
		Println("    Download \"mc\" from https://dl.minio.io/client/mc/release/" + runtime.GOOS + "-" + runtime.GOARCH + "/mc.exe")
		Println("    $ mc.exe config host add myminio http://localhost:9000 " + conf.Credentials.AccessKeyID + " " + conf.Credentials.SecretAccessKey)
	} else {
		Println("    $ wget https://dl.minio.io/client/mc/release/" + runtime.GOOS + "-" + runtime.GOARCH + "/mc")
		Println("    $ chmod 755 mc")
		Println("    $ ./mc config host add myminio http://localhost:9000 " + conf.Credentials.AccessKeyID + " " + conf.Credentials.SecretAccessKey)
	}

	// Start server.
	err = minhttp.ListenAndServe(apiServer, webServer)
	errorIf(err.Trace(), "Failed to start the minio server.", nil)
}
