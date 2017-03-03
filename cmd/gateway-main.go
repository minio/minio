/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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

var gatewayCmd = cli.Command{
	Name:   "gateway",
	Usage:  "Start in gateway mode.",
	Action: gatewayMain,
	Subcommands: []cli.Command{
		{
			Name:  "azure",
			Usage: "Azure cloud",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "address",
					Value: ":9000",
					Usage: "Bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname.",
				},
			},
			Action: azureMain,
		},
	},
}

// Handler for 'minio gateway' - show help.
func gatewayMain(ctx *cli.Context) {
	cli.ShowSubcommandHelp(ctx)
}

// Handler for 'minio gateway azure'.
func azureMain(ctx *cli.Context) {
	// Azure account key length is higher than the default limit.
	secretKeyMaxLen = secretKeyMaxLenAzure

	initServerConfig(ctx)

	creds := serverConfig.GetCredential()
	newObject, err := newAzureLayer(creds.AccessKey, creds.SecretKey)
	fatalIf(err, "Unable to init the azure module.")

	initNSLock(globalIsDistXL)

	router := mux.NewRouter().SkipClean(true)
	registerAPIRouter(router)

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
			cert, key = mustGetCertFile(), mustGetKeyFile()
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
