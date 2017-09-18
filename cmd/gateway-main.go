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
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	miniohttp "github.com/minio/minio/pkg/http"
)

const azureGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  Azure server endpoint. Default ENDPOINT is https://core.windows.net

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of Azure storage.
     MINIO_SECRET_KEY: Password or secret key of Azure storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ {{.HelpName}}

  2. Start minio gateway server for Azure Blob Storage backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ {{.HelpName}} https://azure.example.com
`

const s3GatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  S3 server endpoint. Default ENDPOINT is https://s3.amazonaws.com

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of S3 storage.
     MINIO_SECRET_KEY: Password or secret key of S3 storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

EXAMPLES:
  1. Start minio gateway server for AWS S3 backend.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}}

  2. Start minio gateway server for S3 backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
      $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
      $ {{.HelpName}} https://play.minio.io:9000
`

const gcsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PROJECTID
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PROJECTID:
  GCS project id, there are no defaults this is mandatory.

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of GCS.
     MINIO_SECRET_KEY: Password or secret key of GCS.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

EXAMPLES:
  1. Start minio gateway server for GCS backend.
      $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
      (Instructions to generate credentials : https://developers.google.com/identity/protocols/application-default-credentials)
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}} mygcsprojectid

`

var (
	azureBackendCmd = cli.Command{
		Name:               "azure",
		Usage:              "Microsoft Azure Blob Storage.",
		Action:             azureGatewayMain,
		CustomHelpTemplate: azureGatewayTemplate,
		Flags:              append(serverFlags, globalFlags...),
		HideHelpCommand:    true,
	}

	s3BackendCmd = cli.Command{
		Name:               "s3",
		Usage:              "Amazon Simple Storage Service (S3).",
		Action:             s3GatewayMain,
		CustomHelpTemplate: s3GatewayTemplate,
		Flags:              append(serverFlags, globalFlags...),
		HideHelpCommand:    true,
	}
	gcsBackendCmd = cli.Command{
		Name:               "gcs",
		Usage:              "Google Cloud Storage.",
		Action:             gcsGatewayMain,
		CustomHelpTemplate: gcsGatewayTemplate,
		Flags:              append(serverFlags, globalFlags...),
		HideHelpCommand:    true,
	}

	gatewayCmd = cli.Command{
		Name:            "gateway",
		Usage:           "Start object storage gateway.",
		Flags:           append(serverFlags, globalFlags...),
		HideHelpCommand: true,
		Subcommands:     []cli.Command{azureBackendCmd, s3BackendCmd, gcsBackendCmd},
	}
)

// Represents the type of the gateway backend.
type gatewayBackend string

const (
	azureBackend gatewayBackend = "azure"
	s3Backend    gatewayBackend = "s3"
	gcsBackend   gatewayBackend = "gcs"
	// Add more backends here.
)

// Initialize gateway layer depending on the backend type.
// Supported backend types are
//
// - Azure Blob Storage.
// - AWS S3.
// - Google Cloud Storage.
// - Add your favorite backend here.
func newGatewayLayer(backendType gatewayBackend, arg string) (GatewayLayer, error) {
	switch backendType {
	case azureBackend:
		return newAzureLayer(arg)
	case s3Backend:
		return newS3Gateway(arg)
	case gcsBackend:
		// FIXME: The following print command is temporary and
		// will be removed when gcs is ready for production use.
		log.Println(colorYellow("\n               *** Warning: Not Ready for Production ***"))
		return newGCSGateway(arg)
	}

	return nil, fmt.Errorf("Unrecognized backend type %s", backendType)
}

// Return endpoint.
func parseGatewayEndpoint(arg string) (endPoint string, secure bool, err error) {
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

// Validate gateway arguments.
func validateGatewayArguments(serverAddr, endpointAddr string) error {
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
			return errors.New("endpoint points to the local gateway")
		}
	}
	return nil
}

// Handler for 'minio gateway azure' command line.
func azureGatewayMain(ctx *cli.Context) {
	if ctx.Args().Present() && ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "azure", 1)
	}

	// Validate gateway arguments.
	fatalIf(validateGatewayArguments(ctx.GlobalString("address"), ctx.Args().First()), "Invalid argument")

	gatewayMain(ctx, azureBackend)
}

// Handler for 'minio gateway s3' command line.
func s3GatewayMain(ctx *cli.Context) {
	if ctx.Args().Present() && ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "s3", 1)
	}

	// Validate gateway arguments.
	fatalIf(validateGatewayArguments(ctx.GlobalString("address"), ctx.Args().First()), "Invalid argument")

	gatewayMain(ctx, s3Backend)
}

// Handler for 'minio gateway gcs' command line
func gcsGatewayMain(ctx *cli.Context) {
	if ctx.Args().Present() && ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "gcs", 1)
	}

	if !isValidGCSProjectIDFormat(ctx.Args().First()) {
		errorIf(errGCSInvalidProjectID, "Unable to start GCS gateway with %s", ctx.Args().First())
		cli.ShowCommandHelpAndExit(ctx, "gcs", 1)
	}

	gatewayMain(ctx, gcsBackend)
}

// Handler for 'minio gateway'.
func gatewayMain(ctx *cli.Context, backendType gatewayBackend) {
	// Get quiet flag from command line argument.
	quietFlag := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	if quietFlag {
		log.EnableQuiet()
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
		fatalIf(fmt.Errorf("Access and Secret keys should be set through ENVs for backend [%s]", backendType), "")
	}

	// Create certs path.
	fatalIf(createConfigDir(), "Unable to create configuration directories.")

	// Initialize gateway config.
	initConfig()

	// Enable loggers as per configuration file.
	enableLoggers()

	// Init the error tracing module.
	initError()

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalTLSCertificate, globalIsSSL, err = getSSLConfig()
	fatalIf(err, "Invalid SSL certificate file")

	initNSLock(false) // Enable local namespace lock.

	newObject, err := newGatewayLayer(backendType, ctx.Args().First())
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

	globalHTTPServer = miniohttp.NewServer([]string{gatewayAddr}, registerHandlers(router, handlerFns...), globalTLSCertificate)

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
		mode := ""
		switch gatewayBackend(backendType) {
		case azureBackend:
			mode = globalMinioModeGatewayAzure
		case gcsBackend:
			mode = globalMinioModeGatewayGCS
		case s3Backend:
			mode = globalMinioModeGatewayS3
		}

		// Check update mode.
		checkUpdate(mode)

		// Print gateway startup message.
		printGatewayStartupMessage(getAPIEndpoints(gatewayAddr), backendType)
	}

	handleSignals()
}
