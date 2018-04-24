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
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
)

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

	if runtime.GOOS == "darwin" {
		_, port := mustSplitHostPort(serverAddr)
		// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
		// to IPv6 address i.e minio will start listening on IPv6 address whereas another
		// (non-)minio process is listening on IPv4 of given port.
		// To avoid this error situation we check for port availability only for macOS.
		if err := checkPortAvailability(port); err != nil {
			return err
		}
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

func init() {
	logger.Init(GOPATH)
}

// StartGateway - handler for 'minio gateway <name>'.
func StartGateway(ctx *cli.Context, gw Gateway) {
	if gw == nil {
		logger.FatalIf(errUnexpected, "Gateway implementation not initialized, exiting.")
	}

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

	// Validate if we have access, secret set through environment.
	if !globalIsEnvCreds {
		reqInfo := (&logger.ReqInfo{}).AppendTags("gatewayName", gatewayName)
		contxt := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(contxt, errors.New("Access and Secret keys should be set through ENVs for backend"))
		cli.ShowCommandHelpAndExit(ctx, gatewayName, 1)
	}

	// Create certs path.
	logger.FatalIf(createConfigDir(), "Unable to create configuration directories.")

	// Initialize gateway config.
	initConfig()

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalTLSCertificate, globalIsSSL, err = getSSLConfig()
	logger.FatalIf(err, "Invalid SSL certificate file")

	// Set system resources to maximum.
	logger.LogIf(context.Background(), setMaxResources())

	initNSLock(false) // Enable local namespace lock.

	// Create new notification system.
	globalNotificationSys, err = NewNotificationSys(globalServerConfig, EndpointList{})
	logger.FatalIf(err, "Unable to create new notification system.")

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	newObject, err := gw.NewGatewayLayer(globalServerConfig.GetCredential())
	logger.FatalIf(err, "Unable to initialize gateway layer")

	router := mux.NewRouter().SkipClean(true)

	// Add healthcheck router
	registerHealthCheckRouter(router)

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		logger.FatalIf(registerWebRouter(router), "Unable to configure web browser")
	}

	// Add API router.
	registerAPIRouter(router)

	globalHTTPServer = xhttp.NewServer([]string{gatewayAddr}, registerHandlers(router, globalHandlers...), globalTLSCertificate)

	// Start server, automatically configures TLS if certs are available.
	go func() {
		globalHTTPServerErrorCh <- globalHTTPServer.Start()
	}()

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

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
