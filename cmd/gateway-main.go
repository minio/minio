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
	"github.com/gorilla/mux"
	"github.com/minio/cli"
)

var gatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}} BACKEND
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Custom username or access key of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Custom password or secret key of 8 to 100 characters in length.

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend.
      $ {{.HelpName}} azure

  2. Start minio gateway server bound to a specific ADDRESS:PORT.
      $ {{.HelpName}} --address 192.168.1.101:9000 azure

`

var gatewayCmd = cli.Command{
	Name:               "gateway",
	Usage:              "Start in gateway mode.",
	Action:             gatewayMain,
	CustomHelpTemplate: gatewayTemplate,
	Flags:              serverFlags,
	HideHelpCommand:    true,
}

// Handler for 'minio gateway azure'.
func gatewayMain(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "gateway", 1)
	}
	if ctx.Args().First() != "azure" {
		cli.ShowCommandHelpAndExit(ctx, "gateway", 1)
	}

	// Azure account key length is higher than the default limit.
	secretKeyMaxLen = secretKeyMaxLenAzure

	initServerConfig(ctx)

	creds := serverConfig.GetCredential()
	newObject, err := newAzureLayer(creds.AccessKey, creds.SecretKey)
	fatalIf(err, "Unable to init the azure module.")

	initNSLock(globalIsDistXL)

	router := mux.NewRouter().SkipClean(true)
	registerGatewayAPIRouter(router, newObject)

	var handlerFns = []HandlerFunc{
		// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
		setCrossDomainPolicy,
		// Validates all incoming requests to have a valid date header.
		setTimeValidityHandler,
		// CORS setting for all browser API requests.
		setCorsHandler,
		// Auth handler verifies incoming authorization headers and
		// routes them accordingly. Client receives a HTTP error for
		// invalid/unsupported signatures.
		setAuthHandler,
	}

	handler := registerHandlers(router, handlerFns...)

	serverAddr := ctx.String("address")
	apiServer := NewServerMux(serverAddr, handler)

	// Start server, automatically configures TLS if certs are available.
	go func() {
		cert, key := "", ""
		if globalIsSSL {
			cert, key = getPublicCertFile(), getPrivateKeyFile()
		}
		fatalIf(apiServer.ListenAndServe(cert, key), "Failed to start minio server.")
	}()

	apiEndPoints, err := finalizeAPIEndpoints(apiServer.Addr)
	fatalIf(err, "Unable to finalize API endpoints for %s", apiServer.Addr)

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(apiEndPoints)

	globalObjectAPI = newObject

	<-globalServiceDoneCh
}
