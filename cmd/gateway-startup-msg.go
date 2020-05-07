/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"strings"

	"github.com/minio/minio/pkg/color"
)

// Prints the formatted startup message.
func printGatewayStartupMessage(apiEndPoints []string, backendType string) {
	strippedAPIEndpoints := stripStandardPorts(apiEndPoints)
	// If cache layer is enabled, print cache capacity.
	cacheAPI := newCachedObjectLayerFn()
	if cacheAPI != nil {
		printCacheStorageInfo(cacheAPI.StorageInfo(GlobalContext))
	}
	// Prints credential.
	printGatewayCommonMsg(strippedAPIEndpoints)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	printCLIAccessMsg(strippedAPIEndpoints[0], fmt.Sprintf("my%s", backendType))

	// Prints documentation message.
	printObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		if globalIsSSL {
			printCertificateMsg(globalPublicCerts)
		}
	}
}

// Prints common server startup message. Prints credential, region and browser access.
func printGatewayCommonMsg(apiEndpoints []string) {
	// Get saved credentials.
	cred := globalActiveCred

	apiEndpointStr := strings.Join(apiEndpoints, "  ")

	// Colorize the message and print.
	logStartupMessage(color.Blue("Endpoint: ") + color.Bold(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 1), apiEndpointStr)))
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		logStartupMessage(color.Blue("AccessKey: ") + color.Bold(fmt.Sprintf("%s ", cred.AccessKey)))
		logStartupMessage(color.Blue("SecretKey: ") + color.Bold(fmt.Sprintf("%s ", cred.SecretKey)))
	}
	printEventNotifiers()

	if globalBrowserEnabled {
		logStartupMessage(color.Blue("\nBrowser Access:"))
		logStartupMessage(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 3), apiEndpointStr))
	}
}
