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
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
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

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of your storage backend.
     MINIO_SECRET_KEY: Password or secret key of your storage backend.

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ {{.HelpName}} azure
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
	// Add more backends here.
)

// Returns access and secretkey set from environment variables.
func mustGetGatewayCredsFromEnv() (accessKey, secretKey string) {
	// Fetch access keys from environment variables.
	accessKey = os.Getenv("MINIO_ACCESS_KEY")
	secretKey = os.Getenv("MINIO_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		console.Fatalln("Access and secret keys are mandatory to run Minio gateway server.")
	}
	return accessKey, secretKey
}

// Initialize gateway layer depending on the backend type.
// Supported backend types are
//
// - Azure Blob Storage.
// - Add your favorite backend here.
func newGatewayLayer(backendType, endPoint, accessKey, secretKey string, secure bool) (GatewayLayer, error) {
	if gatewayBackend(backendType) != azureBackend {
		return nil, fmt.Errorf("Unrecognized backend type %s", backendType)
	}
	return newAzureLayer(endPoint, accessKey, secretKey, secure)
}

// Initialize a new gateway config.
//
// DO NOT save this config, this is meant to be
// only used in memory.
func newGatewayConfig(accessKey, secretKey, region string) error {
	// Initialize server config.
	srvCfg := newServerConfigV14()

	// If env is set for a fresh start, save them to config file.
	srvCfg.SetCredential(credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
	})

	// Set default printing to console.
	srvCfg.Logger.SetConsole(consoleLogger{true, "error"})

	// Set custom region.
	srvCfg.SetRegion(region)

	// Create certs path for SSL configuration.
	if err := createConfigDir(); err != nil {
		return err
	}

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
	accessKey, secretKey := mustGetGatewayCredsFromEnv()

	// Initialize new gateway config.
	//
	// TODO: add support for custom region when we add
	// support for S3 backend storage, currently this can
	// default to "us-east-1"
	err := newGatewayConfig(accessKey, secretKey, "us-east-1")
	if err != nil {
		console.Fatalf("Unable to initialize gateway config. Error: %s", err)
	}

	// Enable console logging.
	enableConsoleLogger()

	// Get quiet flag from command line argument.
	quietFlag := ctx.Bool("quiet") || ctx.GlobalBool("quiet")

	// First argument is selected backend type.
	backendType := ctx.Args().First()

	// Second argument is endpoint.	If no endpoint is specified then the
	// gateway implementation should use a default setting.
	endPoint, secure, err := parseGatewayEndpoint(ctx.Args().Get(1))
	if err != nil {
		console.Fatalf("Unable to parse endpoint. Error: %s", err)
	}

	// Create certs path for SSL configuration.
	err = createConfigDir()
	if err != nil {
		console.Fatalf("Unable to create configuration directory. Error: %s", err)
	}

	newObject, err := newGatewayLayer(backendType, endPoint, accessKey, secretKey, secure)
	if err != nil {
		console.Fatalf("Unable to initialize gateway layer. Error: %s", err)
	}

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

	// Set if we are SSL enabled S3 gateway.
	globalIsSSL = isSSL()

	// Start server, automatically configures TLS if certs are available.
	go func() {
		cert, key := "", ""
		if globalIsSSL {
			cert, key = getPublicCertFile(), getPrivateKeyFile()
		}
		if aerr := apiServer.ListenAndServe(cert, key); aerr != nil {
			console.Fatalf("Failed to start minio server. Error: %s\n", aerr)
		}
	}()

	apiEndPoints, err := finalizeAPIEndpoints(apiServer.Addr)
	fatalIf(err, "Unable to finalize API endpoints for %s", apiServer.Addr)

	// Once endpoints are finalized, initialize the new object api.
	globalObjLayerMutex.Lock()
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	if !quietFlag {
		mode := ""
		if gatewayBackend(backendType) == azureBackend {
			mode = globalMinioModeGatewayAzure
		}
		checkUpdate(mode)
		printGatewayStartupMessage(apiEndPoints, accessKey, secretKey, backendType)
	}

	<-globalServiceDoneCh
}
