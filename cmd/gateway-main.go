/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	miniohttp "github.com/minio/minio/pkg/http"
	xnet "github.com/minio/minio/pkg/net"
)

var (
	gatewayCmd = cli.Command{
		Name:            "gateway",
		Usage:           "Start object storage gateway.",
		Flags:           append(serverFlags, globalFlags...),
		HideHelpCommand: true,
	}
)

// Gateway represents a gateway backend.
type Gateway interface {
	// Name returns the unique name of the gateway.
	Name() string
	// NewGatewayLayer returns a new gateway layer.
	NewGatewayLayer() (GatewayLayer, error)
}

// RegisterGatewayCommand registers a new command for gateway.
func RegisterGatewayCommand(cmd cli.Command) error {
	// We should not have multiple subcommands with same name.
	for _, c := range gatewayCmd.Subcommands {
		if c.Name == cmd.Name {
			return fmt.Errorf("duplicate gateway: %s", cmd.Name)
		}
	}

	gatewayCmd.Subcommands = append(gatewayCmd.Subcommands, cmd)
	return nil
}

// MustRegisterGatewayCommand is like RegisterGatewayCommand but panics instead of returning error.
func MustRegisterGatewayCommand(cmd cli.Command) {
	if err := RegisterGatewayCommand(cmd); err != nil {
		panic(err)
	}
}

// parseGatewayEndpoint - parses given argument and returns endpoint and flag to use TLS for the endpoint.
// arg must be in the form of [{http|https}://]SERVER[:PORT][/]
func parseGatewayEndpoint(arg string) (endpoint string, secure bool, err error) {
	endpoint = strings.TrimSpace(arg)
	if endpoint == "" {
		return endpoint, secure, errors.New("empty endpoint given")
	}

	if tokens := strings.SplitN(endpoint, "://", 2); len(tokens) > 1 {
		if tokens[0] != "http" && tokens[0] != "https" {
			return endpoint, secure, fmt.Errorf("invalid scheme in endpoint %v", arg)
		}
	} else {
		if !strings.HasPrefix(tokens[0], "http:") && !strings.HasPrefix(tokens[0], "https:") {
			// Add secure http scheme
			endpoint = "https://" + endpoint
		}
	}

	u, err := xnet.ParseURL(endpoint)
	if err != nil {
		return endpoint, secure, err
	}

	urlPath := u.Path
	if urlPath != "" {
		urlPath = path.Clean(u.Path)
	}
	if !((urlPath == "" || urlPath == "/") &&
		u.User == nil && u.Opaque == "" && u.ForceQuery == false && u.RawQuery == "" && u.Fragment == "") {
		return endpoint, secure, fmt.Errorf("invalid endpoint format %v", arg)
	}

	endpoint = u.Host
	if u.Scheme == "https" {
		secure = true
	}

	host := xnet.MustParseHost(u.Host)
	if !host.IsPortSet {
		if secure {
			host.PortNumber = 443
		} else {
			host.PortNumber = 80
		}
	}

	isLocal, err := isLocalHost(host.Host)
	if err != nil {
		return endpoint, secure, err
	}
	if isLocal && globalServerHost.PortNumber == host.PortNumber {
		return endpoint, secure, fmt.Errorf("endpoint %v points to this gateway itself", arg)
	}

	return endpoint, secure, nil
}

// Handler for 'minio gateway <name>'.
func startGateway(gw Gateway, quietFlag bool) {
	if !globalIsEnvCreds {
		fatalIf(errors.New("missing keys"), "Access and secret keys must be set through environment variables for gateway %v", gw.Name())
	}

	// Create certs path.
	fatalIf(createConfigDir(), "Unable to create configuration directories.")

	// Initialize server config.
	initConfig()

	// Enable loggers as per configuration file.
	enableLoggers()

	// Init the error tracing module.
	initError()

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalTLSCertificate, globalIsSSL, err = getSSLConfig()
	fatalIf(err, "Invalid SSL certificate file")

	if !quietFlag {
		// Check for new updates from dl.minio.io.
		mode := globalMinioModeGatewayPrefix + gw.Name()
		checkUpdate(mode)
	}

	// Set system resources to maximum.
	errorIf(setMaxResources(), "Unable to change resource limit")

	initNSLock(false) // Enable local namespace lock.

	newObject, err := gw.NewGatewayLayer()
	fatalIf(err, "Unable to initialize gateway layer")

	router := mux.NewRouter().SkipClean(true)

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		fatalIf(registerWebRouter(router), "Unable to configure web browser")
	}
	registerGatewayAPIRouter(router, newObject)

	var handlerFns = []HandlerFunc{
		// Validate all the incoming paths.
		setPathValidityHandler,
		// Limits all requests size to a maximum fixed limit
		setRequestSizeLimitHandler,
		// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
		setCrossDomainPolicy,
		// Validates all incoming requests to have a valid date header.
		// Redirect some pre-defined browser request paths to a static location prefix.
		setBrowserRedirectHandler,
		// Validates if incoming request is for restricted buckets.
		setReservedBucketHandler,
		// Adds cache control for all browser requests.
		setBrowserCacheControlHandler,
		// Validates all incoming requests to have a valid date header.
		setTimeValidityHandler,
		// CORS setting for all browser API requests.
		setCorsHandler,
		// Validates all incoming URL resources, for invalid/unsupported
		// resources client receives a HTTP error.
		setIgnoreResourcesHandler,
		// Auth handler verifies incoming authorization headers and
		// routes them accordingly. Client receives a HTTP error for
		// invalid/unsupported signatures.
		setAuthHandler,
		// Add new handlers here.
	}

	handler := registerHandlers(router, handlerFns...)
	globalHTTPServer = miniohttp.NewServer([]string{globalServerHost.String()}, handler, globalTLSCertificate)
	globalHTTPServer.ReadTimeout = globalConnReadTimeout
	globalHTTPServer.WriteTimeout = globalConnWriteTimeout
	globalHTTPServer.UpdateBytesReadFunc = globalConnStats.incInputBytes
	globalHTTPServer.UpdateBytesWrittenFunc = globalConnStats.incOutputBytes
	globalHTTPServer.ErrorLogFunc = errorIf
	go func() {
		globalHTTPServerErrorCh <- globalHTTPServer.Start()
	}()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	// Once endpoints are finalized, initialize the new object api.
	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Print gateway startup message.
	// Prints the formatted startup message once object layer is initialized.
	printGatewayStartupMessage(getAPIEndpoints(globalServerHost.String()), gw.Name())

	// Set uptime time after object layer has initialized.
	globalBootTime = UTCNow()

	handleSignals()
}
