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
      $ {{.HelpName}} gcs [PROFILE:]PROJECTID
`

var gatewayCmd = cli.Command{
	Name:               "gateway",
	Usage:              "Start object storage gateway.",
	Action:             gatewayMain,
	CustomHelpTemplate: gatewayTemplate,
	Flags: append(serverFlags,
		cli.BoolFlag{
			Name:  "quiet",
			Usage: "Disable startup banner.",
		},
	),
	HideHelpCommand: true,
}

// Represents the type of the gateway backend.
type gatewayBackend string

const (
	azureBackend gatewayBackend = "azure"
	s3Backend    gatewayBackend = "s3"
	gcsBackend   gatewayBackend = "gcs"
	// Add more backends here.
)

// GatewayFn returns the GatewayLayer for the backend
type GatewayFn func(cli.Args) (GatewayLayer, error)

var (
	backends = map[gatewayBackend]GatewayFn{
		azureBackend: newAzureLayer,
		s3Backend:    newS3Gateway,
		gcsBackend:   newGCSGateway,
	}
)

// Returns access and secretkey set from environment variables.
func mustGetGatewayConfigFromEnv() (string, string, string) {
	// Fetch access keys from environment variables.
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		fatalIf(errors.New("Missing credentials"), "Access and secret keys are mandatory to run Minio gateway server.")
	}

	region := globalMinioDefaultRegion
	if v := os.Getenv("MINIO_REGION"); v != "" {
		region = v
	}

	return accessKey, secretKey, region
}

// Initialize gateway layer depending on the backend type.
func newGatewayLayer(backendType string, args []string) (GatewayLayer, error) {
	if fn, ok := backends[gatewayBackend(backendType)]; ok {
		// every backend is responsible for parsing its own arguments
		return fn(args)
	}

	return nil, fmt.Errorf("Unrecognized backend type %s", backendType)
}

// Initialize a new gateway config.
//
// DO NOT save this config, this is meant to be
// only used in memory.
func newGatewayConfig(accessKey, secretKey, region string) error {
	// Initialize server config.
	srvCfg := newServerConfigV18()

	// If env is set for a fresh start, save them to config file.
	srvCfg.SetCredential(credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
	})

	// Set custom region.
	srvCfg.SetRegion(region)

	// hold the mutex lock before a new config is assigned.
	// Save the new config globally.
	// unlock the mutex.
	serverConfigMu.Lock()
	serverConfig = srvCfg
	serverConfigMu.Unlock()

	return nil
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

	// Fetch access and secret key from env.
	accessKey, secretKey, region := mustGetGatewayConfigFromEnv()

	// Initialize new gateway config.
	newGatewayConfig(accessKey, secretKey, region)

	// Get quiet flag from command line argument.
	quietFlag := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	if quietFlag {
		log.EnableQuiet()
	}

	// First argument is selected backend type.
	backendType := ctx.Args().First()

	// Create certs path for SSL configuration.
	fatalIf(createConfigDir(), "Unable to create configuration directory")

	newObject, err := newGatewayLayer(backendType, ctx.Args()[1:])
	fatalIf(err, "Unable to initialize gateway layer")

	initNSLock(false) // Enable local namespace lock.

	router := mux.NewRouter().SkipClean(true)
	registerGatewayAPIRouter(router, newObject)

	var handlerFns = []HandlerFunc{
		// Validate all the incoming paths.
		setPathValidityHandler,
		// Limits all requests size to a maximum fixed limit
		setRequestSizeLimitHandler,
		// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
		setCrossDomainPolicy,
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
	}

	apiServer := NewServerMux(ctx.String("address"), registerHandlers(router, handlerFns...))

	_, _, globalIsSSL, err = getSSLConfig()
	fatalIf(err, "Invalid SSL key file")

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
		if gatewayBackend(backendType) == azureBackend {
			mode = globalMinioModeGatewayAzure
		} else if gatewayBackend(backendType) == s3Backend {
			mode = globalMinioModeGatewayS3
		}
		checkUpdate(mode)
		apiEndpoints := getAPIEndpoints(apiServer.Addr)
		printGatewayStartupMessage(apiEndpoints, accessKey, secretKey, backendType)
	}

	<-globalServiceDoneCh
}
