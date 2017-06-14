/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"runtime"
	"strings"
)

// Prints the formatted startup message.
func printGatewayStartupMessage(apiEndPoints []string, accessKey, secretKey string, backendType gatewayBackend) {
	// Prints credential.
	printGatewayCommonMsg(apiEndPoints, accessKey, secretKey)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	endPoint := apiEndPoints[0]

	// Configure 'mc', following block prints platform specific information for minio client.
	log.Println(colorBlue("\nCommand-line Access: ") + mcQuickStartGuide)
	if runtime.GOOS == globalWindowsOSName {
		mcMessage := fmt.Sprintf("$ mc.exe config host add my%s %s %s %s", backendType, endPoint, accessKey, secretKey)
		log.Println(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	} else {
		mcMessage := fmt.Sprintf("$ mc config host add my%s %s %s %s", backendType, endPoint, accessKey, secretKey)
		log.Println(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	}

	// Prints documentation message.
	printObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if globalIsSSL {
		printCertificateMsg(globalPublicCerts)
	}
}

// Prints common server startup message. Prints credential, region and browser access.
func printGatewayCommonMsg(apiEndpoints []string, accessKey, secretKey string) {
	apiEndpointStr := strings.Join(apiEndpoints, "  ")
	// Colorize the message and print.
	log.Println(colorBlue("\nEndpoint: ") + colorBold(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 1), apiEndpointStr)))
	log.Println(colorBlue("AccessKey: ") + colorBold(fmt.Sprintf("%s ", accessKey)))
	log.Println(colorBlue("SecretKey: ") + colorBold(fmt.Sprintf("%s ", secretKey)))
}
