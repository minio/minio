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
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
)

var gatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} BACKEND [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
BACKEND:
  azure: Microsoft Azure Blob Storage. Default ENDPOINT is https://core.windows.net
  s3: Amazon Simple Storage Service (S3). Default ENDPOINT is https://s3.amazonaws.com
  gcs: Google Cloud Storage (GCS). Default ENDPOINT is https://storage.googleapis.com

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of your storage backend.
     MINIO_SECRET_KEY: Password or secret key of your storage backend.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ {{.HelpName}} azure

  2. Start minio gateway server for AWS S3 backend.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}} s3

  3. Start minio gateway server for S3 backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
      $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
      $ {{.HelpName}} s3 https://play.minio.io:9000

  4. Start minio gateway server for GCS backend.
      $ gcloud init --console-only # this will configure your google account
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}} gcs PROJECTID
`

var gatewayCmd = cli.Command{
	Name:               "gateway",
	Usage:              "Start object storage gateway.",
	Action:             gatewayMain,
	CustomHelpTemplate: gatewayTemplate,
	Flags:              append(serverFlags, globalFlags...),
	HideHelpCommand:    true,
}

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
func newGatewayLayer(backendType string, args cli.Args) (GatewayLayer, error) {
	switch gatewayBackend(backendType) {
	case azureBackend:
		return newAzureLayer(args)
	case s3Backend:
		return newS3Gateway(args)
	case gcsBackend:
		return newGCSGateway(args)
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

// Handler for 'minio gateway'.
func gatewayMain(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "gateway", 1)
	}

	// Get quiet flag from command line argument.
	quietFlag := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	if quietFlag {
		log.EnableQuiet()
	}

	// Handle common command args.
	handleCommonCmdArgs(ctx)

	// Handle common env vars.
	handleCommonEnvVars()

	// Create certs path.
	fatalIf(createConfigDir(), "Unable to create configuration directories.")

	// Initialize config.
	initConfig()

	// Enable loggers as per configuration file.
	enableLoggers()

	// Init the error tracing module.
	initError()

	// Check and load SSL certificates.
	var err error
	globalPublicCerts, globalRootCAs, globalIsSSL, err = getSSLConfig()
	fatalIf(err, "Invalid SSL key file")

	// First argument is selected backend type.
	backendType := ctx.Args().Get(0)
	var endpointAddr string
	if gatewayBackend(backendType) != gcsBackend {
		// Second argument is the endpoint address (optional)
		// Only for gcs.
		endpointAddr = ctx.Args().Get(1)
	}

	// Third argument is the address flag
	serverAddr := ctx.String("address")
	if endpointAddr != "" {
		// Reject the endpoint if it points to the gateway handler itself.
		var sameTarget bool
		sameTarget, err = sameLocalAddrs(endpointAddr, serverAddr)
		fatalIf(err, "Unable to compare server and endpoint addresses.")
		if sameTarget {
			fatalIf(errors.New("endpoint points to the local gateway"), "Endpoint url is not allowed")
		}
	}

	newObject, err := newGatewayLayer(backendType, ctx.Args()[1:])
	fatalIf(err, "Unable to initialize gateway layer")

	initNSLock(false) // Enable local namespace lock.

	router := mux.NewRouter().SkipClean(true)

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		aerr := registerWebRouter(router)
		fatalIf(aerr, "Unable to configure web browser")
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
		setPrivateBucketHandler,
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

	apiServer := NewServerMux(serverAddr, registerHandlers(router, handlerFns...))

	// Start server, automatically configures TLS if certs are available.
	go func() {
		cert, key := "", ""
		if globalIsSSL {
			cert, key = getPublicCertFile(), getPrivateKeyFile()
		}

		aerr := apiServer.ListenAndServe(cert, key)
		fatalIf(aerr, "Failed to start minio server")
	}()

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
		checkUpdate(mode)
		apiEndpoints := getAPIEndpoints(apiServer.Addr)
		printGatewayStartupMessage(apiEndpoints, backendType)
	}

	<-globalServiceDoneCh
}
