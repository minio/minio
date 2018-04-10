/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"crypto/x509"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
)

// Documentation links, these are part of message printing code.
const (
	mcQuickStartGuide     = "https://docs.minio.io/docs/minio-client-quickstart-guide"
	goQuickStartGuide     = "https://docs.minio.io/docs/golang-client-quickstart-guide"
	jsQuickStartGuide     = "https://docs.minio.io/docs/javascript-client-quickstart-guide"
	javaQuickStartGuide   = "https://docs.minio.io/docs/java-client-quickstart-guide"
	pyQuickStartGuide     = "https://docs.minio.io/docs/python-client-quickstart-guide"
	dotnetQuickStartGuide = "https://docs.minio.io/docs/dotnet-client-quickstart-guide"
)

// generates format string depending on the string length and padding.
func getFormatStr(strLen int, padding int) string {
	formatStr := fmt.Sprintf("%ds", strLen+padding)
	return "%" + formatStr
}

// Prints the formatted startup message.
func printStartupMessage(apiEndPoints []string) {

	strippedAPIEndpoints := stripStandardPorts(apiEndPoints)
	// If cache layer is enabled, print cache capacity.
	cacheObjectAPI := newCacheObjectsFn()
	if cacheObjectAPI != nil {
		printCacheStorageInfo(cacheObjectAPI.StorageInfo(context.Background()))
	}
	// Object layer is initialized then print StorageInfo.
	objAPI := newObjectLayerFn()
	if objAPI != nil {
		printStorageInfo(objAPI.StorageInfo(context.Background()))
	}

	// Prints credential, region and browser access.
	printServerCommonMsg(strippedAPIEndpoints)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	printCLIAccessMsg(strippedAPIEndpoints[0], "myminio")

	// Prints documentation message.
	printObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if globalIsSSL {
		printCertificateMsg(globalPublicCerts)
	}
}

// strip api endpoints list with standard ports such as
// port "80" and "443" before displaying on the startup
// banner.  Returns a new list of API endpoints.
func stripStandardPorts(apiEndpoints []string) (newAPIEndpoints []string) {
	newAPIEndpoints = make([]string, len(apiEndpoints))
	// Check all API endpoints for standard ports and strip them.
	for i, apiEndpoint := range apiEndpoints {
		url, err := url.Parse(apiEndpoint)
		if err != nil {
			newAPIEndpoints[i] = apiEndpoint
			continue
		}
		host, port := mustSplitHostPort(url.Host)
		// For standard HTTP(s) ports such as "80" and "443"
		// apiEndpoints should only be host without port.
		switch {
		case url.Scheme == "http" && port == "80":
			fallthrough
		case url.Scheme == "https" && port == "443":
			url.Host = host
			newAPIEndpoints[i] = url.String()
		default:
			newAPIEndpoints[i] = apiEndpoint
		}
	}
	return newAPIEndpoints
}

// Prints common server startup message. Prints credential, region and browser access.
func printServerCommonMsg(apiEndpoints []string) {
	// Get saved credentials.
	cred := globalServerConfig.GetCredential()

	// Get saved region.
	region := globalServerConfig.GetRegion()

	apiEndpointStr := strings.Join(apiEndpoints, "  ")

	// Colorize the message and print.
	logger.StartupMessage(colorBlue("Endpoint: ") + colorBold(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 1), apiEndpointStr)))
	logger.StartupMessage(colorBlue("AccessKey: ") + colorBold(fmt.Sprintf("%s ", cred.AccessKey)))
	logger.StartupMessage(colorBlue("SecretKey: ") + colorBold(fmt.Sprintf("%s ", cred.SecretKey)))
	if region != "" {
		logger.StartupMessage(colorBlue("Region: ") + colorBold(fmt.Sprintf(getFormatStr(len(region), 3), region)))
	}
	printEventNotifiers()

	if globalIsBrowserEnabled {
		logger.StartupMessage(colorBlue("\nBrowser Access:"))
		logger.StartupMessage(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 3), apiEndpointStr))
	}
}

// Prints bucket notification configurations.
func printEventNotifiers() {
	arns := globalNotificationSys.GetARNList()
	if len(arns) == 0 {
		return
	}

	arnMsg := colorBlue("SQS ARNs: ")
	for _, arn := range arns {
		arnMsg += colorBold(fmt.Sprintf(getFormatStr(len(arn), 1), arn))
	}

	logger.StartupMessage(arnMsg)
}

// Prints startup message for command line access. Prints link to our documentation
// and custom platform specific message.
func printCLIAccessMsg(endPoint string, alias string) {
	// Get saved credentials.
	cred := globalServerConfig.GetCredential()

	// Configure 'mc', following block prints platform specific information for minio client.
	logger.StartupMessage(colorBlue("\nCommand-line Access: ") + mcQuickStartGuide)
	if runtime.GOOS == globalWindowsOSName {
		mcMessage := fmt.Sprintf("$ mc.exe config host add %s %s %s %s", alias, endPoint, cred.AccessKey, cred.SecretKey)
		logger.StartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	} else {
		mcMessage := fmt.Sprintf("$ mc config host add %s %s %s %s", alias, endPoint, cred.AccessKey, cred.SecretKey)
		logger.StartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	}
}

// Prints startup message for Object API acces, prints link to our SDK documentation.
func printObjectAPIMsg() {
	logger.StartupMessage(colorBlue("\nObject API (Amazon S3 compatible):"))
	logger.StartupMessage(colorBlue("   Go: ") + fmt.Sprintf(getFormatStr(len(goQuickStartGuide), 8), goQuickStartGuide))
	logger.StartupMessage(colorBlue("   Java: ") + fmt.Sprintf(getFormatStr(len(javaQuickStartGuide), 6), javaQuickStartGuide))
	logger.StartupMessage(colorBlue("   Python: ") + fmt.Sprintf(getFormatStr(len(pyQuickStartGuide), 4), pyQuickStartGuide))
	logger.StartupMessage(colorBlue("   JavaScript: ") + jsQuickStartGuide)
	logger.StartupMessage(colorBlue("   .NET: ") + fmt.Sprintf(getFormatStr(len(dotnetQuickStartGuide), 6), dotnetQuickStartGuide))
}

// Get formatted disk/storage info message.
func getStorageInfoMsg(storageInfo StorageInfo) string {
	msg := fmt.Sprintf("%s %s Free, %s Total", colorBlue("Drive Capacity:"),
		humanize.IBytes(uint64(storageInfo.Free)),
		humanize.IBytes(uint64(storageInfo.Total)))
	if storageInfo.Backend.Type == Erasure {
		diskInfo := fmt.Sprintf(" %d Online, %d Offline. ", storageInfo.Backend.OnlineDisks, storageInfo.Backend.OfflineDisks)
		msg += colorBlue("\nStatus:") + fmt.Sprintf(getFormatStr(len(diskInfo), 8), diskInfo)
	}
	return msg
}

// Prints startup message of storage capacity and erasure information.
func printStorageInfo(storageInfo StorageInfo) {
	logger.StartupMessage(getStorageInfoMsg(storageInfo) + "\n")
}

func printCacheStorageInfo(storageInfo StorageInfo) {
	msg := fmt.Sprintf("%s %s Free, %s Total", colorBlue("Cache Capacity:"),
		humanize.IBytes(uint64(storageInfo.Free)),
		humanize.IBytes(uint64(storageInfo.Total)))
	logger.StartupMessage(msg)
}

// Prints certificate expiry date warning
func getCertificateChainMsg(certs []*x509.Certificate) string {
	msg := colorBlue("\nCertificate expiry info:\n")
	totalCerts := len(certs)
	var expiringCerts int
	for i := totalCerts - 1; i >= 0; i-- {
		cert := certs[i]
		if cert.NotAfter.Before(UTCNow().Add(globalMinioCertExpireWarnDays)) {
			expiringCerts++
			msg += fmt.Sprintf(colorBold("#%d %s will expire on %s\n"), expiringCerts, cert.Subject.CommonName, cert.NotAfter)
		}
	}
	if expiringCerts > 0 {
		return msg
	}
	return ""
}

// Prints the certificate expiry message.
func printCertificateMsg(certs []*x509.Certificate) {
	logger.StartupMessage(getCertificateChainMsg(certs))
}
