/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/certs"
)

func init() {
	logger.Init(GOPATH, GOROOT)
	logger.RegisterUIError(fmtError)
}

var (
	gatewayCmd = cli.Command{
		Name:            "gateway",
		Usage:           "Start object storage gateway.",
		Flags:           append(serverFlags, globalFlags...),
		HideHelpCommand: true,
	}
)

// RegisterGatewayCommand registers a new command for gateway.
func RegisterGatewayCommand(cmd cli.Command) error {
	cmd.Flags = append(append(cmd.Flags, append(cmd.Flags, serverFlags...)...), globalFlags...)
	gatewayCmd.Subcommands = append(gatewayCmd.Subcommands, cmd)
	return nil
}

// ParseGatewayEndpoint - Return endpoint.
func ParseGatewayEndpoint(arg string) (endPoint string, secure bool, err error) {
	schemeSpecified := len(strings.Split(arg, "://")) > 1
	if !schemeSpecified {
		// Default connection will be "secure".
		arg = "https://" + arg
	}

	u, err := url.Parse(arg)
	if err != nil {
		return "", false, err
	}

	switch u.Scheme {
	case "http":
		return u.Host, false, nil
	case "https":
		return u.Host, true, nil
	default:
		return "", false, fmt.Errorf("Unrecognized scheme %s", u.Scheme)
	}
}

// ValidateGatewayArguments - Validate gateway arguments.
func ValidateGatewayArguments(serverAddr, endpointAddr string) error {
	if err := CheckLocalServerAddr(serverAddr); err != nil {
		return err
	}

	if endpointAddr != "" {
		// Reject the endpoint if it points to the gateway handler itself.
		sameTarget, err := sameLocalAddrs(endpointAddr, serverAddr)
		if err != nil {
			return err
		}
		if sameTarget {
			return fmt.Errorf("endpoint points to the local gateway")
		}
	}
	return nil
}

// StartGateway - handler for 'minio gateway <name>'.
func StartGateway(ctx *cli.Context, gw Gateway) {
	if gw == nil {
		logger.FatalIf(errUnexpected, "Gateway implementation not initialized")
	}

	// Disable logging until gateway initialization is complete, any
	// error during initialization will be shown as a fatal message
	logger.Disable = true

	// Validate if we have access, secret set through environment.
	gatewayName := gw.Name()
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, gatewayName, 1)
	}

	// Get "json" flag from command line argument and
	// enable json and quite modes if jason flag is turned on.
	jsonFlag := ctx.IsSet("json") || ctx.GlobalIsSet("json")
	if jsonFlag {
		logger.EnableJSON()
	}

	// Get quiet flag from command line argument.
	quietFlag := ctx.IsSet("quiet") || ctx.GlobalIsSet("quiet")
	if quietFlag {
		logger.EnableQuiet()
	}

	// Fetch address option
	gatewayAddr := ctx.GlobalString("address")
	if gatewayAddr == ":"+globalMinioPort {
		gatewayAddr = ctx.String("address")
	}

	// Handle common command args.
	handleCommonCmdArgs(ctx)

	// Handle common env vars.
	handleCommonEnvVars()

	// Get port to listen on from gateway address
	_, gatewayPort, pErr := net.SplitHostPort(gatewayAddr)
	if pErr != nil {
		logger.FatalIf(pErr, "Unable to start gateway")
	}

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(checkPortAvailability(gatewayPort), "Unable to start the gateway")

	// Validate if we have access, secret set through environment.
	if !globalIsEnvCreds {
		logger.Fatal(uiErrEnvCredentialsMissingGateway(nil), "Unable to start gateway")
	}

	// Create certs path.
	logger.FatalIf(createConfigDir(), "Unable to create configuration directories")

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalTLSCerts, globalIsSSL, err = getSSLConfig()
	logger.FatalIf(err, "Invalid SSL certificate file")

	// Set system resources to maximum.
	logger.LogIf(context.Background(), setMaxResources())

	initNSLock(false) // Enable local namespace lock.

	router := mux.NewRouter().SkipClean(true)

	// Add healthcheck router
	registerHealthCheckRouter(router)

	// Add server metrics router
	registerMetricsRouter(router)

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		logger.FatalIf(registerWebRouter(router), "Unable to configure web browser")
	}

	// Enable STS router if etcd is enabled.
	if globalEtcdClient != nil {
		registerSTSRouter(router)
	}

	// Add API router.
	registerAPIRouter(router)

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	globalHTTPServer = xhttp.NewServer([]string{gatewayAddr}, criticalErrorHandler{registerHandlers(router, globalHandlers...)}, getCert)
	globalHTTPServer.UpdateBytesReadFunc = globalConnStats.incInputBytes
	globalHTTPServer.UpdateBytesWrittenFunc = globalConnStats.incOutputBytes
	go func() {
		globalHTTPServerErrorCh <- globalHTTPServer.Start()
	}()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	newObject, err := gw.NewGatewayLayer(globalServerConfig.GetCredential())
	if err != nil {
		// Stop watching for any certificate changes.
		globalTLSCerts.Stop()

		globalHTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize gateway backend")
	}

	// Create a new config system.
	globalConfigSys = NewConfigSys()

	// Initialize server config.
	srvCfg := newServerConfig()

	// Override any values from ENVs.
	srvCfg.loadFromEnvs()

	// Load values to cached global values.
	srvCfg.loadToCachedConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Load logger subsystem
	loadLoggers()

	// This is only to uniquely identify each gateway deployments.
	logger.SetDeploymentID(os.Getenv("MINIO_GATEWAY_DEPLOYMENT_ID"))

	var cacheConfig = globalServerConfig.GetCacheConfig()
	if len(cacheConfig.Drives) > 0 {
		var err error
		// initialize the new disk cache objects.
		globalCacheObjectAPI, err = newServerCacheObjects(cacheConfig)
		logger.FatalIf(err, "Unable to initialize disk caching")
	}

	// Load logger subsystem
	loadLoggers()

	// Re-enable logging
	logger.Disable = false

	// Create new IAM system.
	globalIAMSys = NewIAMSys()
	if globalEtcdClient != nil {
		// Initialize IAM sys.
		go globalIAMSys.Init(newObject)
	}

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Initialize policy system.
	go globalPolicySys.Init(newObject)

	// Create new notification system.
	globalNotificationSys = NewNotificationSys(globalServerConfig, globalEndpoints)

	// Once endpoints are finalized, initialize the new object api.
	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	if !quietFlag {
		mode := globalMinioModeGatewayPrefix + gatewayName
		// Check update mode.
		checkUpdate(mode)

		// Print a warning message if gateway is not ready for production before the startup banner.
		if !gw.Production() {
			logger.StartupMessage(colorYellow("               *** Warning: Not Ready for Production ***"))
		}

		// Print gateway startup message.
		printGatewayStartupMessage(getAPIEndpoints(gatewayAddr), gatewayName)
	}

	handleSignals()
}
