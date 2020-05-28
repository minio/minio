/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"crypto/x509"
	"fmt"
	"net"
	"runtime"
	"strings"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	color "github.com/minio/minio/pkg/color"
	xnet "github.com/minio/minio/pkg/net"
)

// Documentation links, these are part of message printing code.
const (
	mcQuickStartGuide      = "https://docs.min.io/docs/minio-client-quickstart-guide"
	mcAdminQuickStartGuide = "https://docs.min.io/docs/minio-admin-complete-guide.html"
	goQuickStartGuide      = "https://docs.min.io/docs/golang-client-quickstart-guide"
	jsQuickStartGuide      = "https://docs.min.io/docs/javascript-client-quickstart-guide"
	javaQuickStartGuide    = "https://docs.min.io/docs/java-client-quickstart-guide"
	pyQuickStartGuide      = "https://docs.min.io/docs/python-client-quickstart-guide"
	dotnetQuickStartGuide  = "https://docs.min.io/docs/dotnet-client-quickstart-guide"
)

// generates format string depending on the string length and padding.
func getFormatStr(strLen int, padding int) string {
	formatStr := fmt.Sprintf("%ds", strLen+padding)
	return "%" + formatStr
}

func mustGetStorageInfo(objAPI ObjectLayer) StorageInfo {
	storageInfo, _ := objAPI.StorageInfo(GlobalContext, false)
	return storageInfo
}

func printStartupSafeModeMessage(apiEndpoints []string, err error) {
	logStartupMessage(color.RedBold("Server startup failed with '%v'", err))
	logStartupMessage(color.RedBold("Server switching to safe mode"))
	logStartupMessage(color.RedBold("Please use 'mc admin config' commands fix this issue"))

	// Object layer is initialized then print StorageInfo in safe mode.
	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI != nil {
		if msg := getStorageInfoMsgSafeMode(mustGetStorageInfo(objAPI)); msg != "" {
			logStartupMessage(msg)
		}
	}

	// Get saved credentials.
	cred := globalActiveCred

	// Get saved region.
	region := globalServerRegion

	strippedAPIEndpoints := stripStandardPorts(apiEndpoints)

	apiEndpointStr := strings.Join(strippedAPIEndpoints, "  ")

	// Colorize the message and print.
	logStartupMessage(color.Red("Endpoint: ") + color.Bold(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 1), apiEndpointStr)))
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		logStartupMessage(color.Red("AccessKey: ") + color.Bold(fmt.Sprintf("%s ", cred.AccessKey)))
		logStartupMessage(color.Red("SecretKey: ") + color.Bold(fmt.Sprintf("%s ", cred.SecretKey)))
		if region != "" {
			logStartupMessage(color.Red("Region: ") + color.Bold(fmt.Sprintf(getFormatStr(len(region), 3), region)))
		}
	}

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	alias := "myminio"
	endPoint := strippedAPIEndpoints[0]

	// Configure 'mc', following block prints platform specific information for minio client admin commands.
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		logStartupMessage(color.RedBold("\nCommand-line Access: ") + mcAdminQuickStartGuide)
		if runtime.GOOS == globalWindowsOSName {
			mcMessage := fmt.Sprintf("> mc.exe config host add %s %s %s %s --api s3v4", alias,
				endPoint, cred.AccessKey, cred.SecretKey)
			logStartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
			mcMessage = "> mc.exe admin config --help"
			logStartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
		} else {
			mcMessage := fmt.Sprintf("$ mc config host add %s %s %s %s --api s3v4", alias,
				endPoint, cred.AccessKey, cred.SecretKey)
			logStartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
			mcMessage = "$ mc admin config --help"
			logStartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
		}
	}
}

// Prints the formatted startup message.
func printStartupMessage(apiEndpoints []string) {

	strippedAPIEndpoints := stripStandardPorts(apiEndpoints)
	// If cache layer is enabled, print cache capacity.
	cachedObjAPI := newCachedObjectLayerFn()
	if cachedObjAPI != nil {
		printCacheStorageInfo(cachedObjAPI.StorageInfo(GlobalContext))
	}

	// Object layer is initialized then print StorageInfo.
	objAPI := newObjectLayerFn()
	if objAPI != nil {
		printStorageInfo(mustGetStorageInfo(objAPI))
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
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		if globalIsSSL {
			printCertificateMsg(globalPublicCerts)
		}
	}
}

// Returns true if input is not IPv4, false if it is.
func isNotIPv4(host string) bool {
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	ip := net.ParseIP(h)
	ok := ip.To4() != nil // This is always true of IP is IPv4

	// Returns true if input is not IPv4.
	return !ok
}

// strip api endpoints list with standard ports such as
// port "80" and "443" before displaying on the startup
// banner.  Returns a new list of API endpoints.
func stripStandardPorts(apiEndpoints []string) (newAPIEndpoints []string) {
	newAPIEndpoints = make([]string, len(apiEndpoints))
	// Check all API endpoints for standard ports and strip them.
	for i, apiEndpoint := range apiEndpoints {
		u, err := xnet.ParseHTTPURL(apiEndpoint)
		if err != nil {
			continue
		}
		if globalMinioHost == "" && isNotIPv4(u.Host) {
			// Skip all non-IPv4 endpoints when we bind to all interfaces.
			continue
		}
		newAPIEndpoints[i] = u.String()
	}
	return newAPIEndpoints
}

// Prints common server startup message. Prints credential, region and browser access.
func printServerCommonMsg(apiEndpoints []string) {
	// Get saved credentials.
	cred := globalActiveCred

	// Get saved region.
	region := globalServerRegion

	apiEndpointStr := strings.Join(apiEndpoints, "  ")

	// Colorize the message and print.
	logStartupMessage(color.Blue("Endpoint: ") + color.Bold(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 1), apiEndpointStr)))
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		logStartupMessage(color.Blue("AccessKey: ") + color.Bold(fmt.Sprintf("%s ", cred.AccessKey)))
		logStartupMessage(color.Blue("SecretKey: ") + color.Bold(fmt.Sprintf("%s ", cred.SecretKey)))
		if region != "" {
			logStartupMessage(color.Blue("Region: ") + color.Bold(fmt.Sprintf(getFormatStr(len(region), 3), region)))
		}
	}
	printEventNotifiers()

	if globalBrowserEnabled {
		logStartupMessage(color.Blue("\nBrowser Access:"))
		logStartupMessage(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 3), apiEndpointStr))
	}
}

// Prints bucket notification configurations.
func printEventNotifiers() {
	if globalNotificationSys == nil {
		return
	}

	arns := globalNotificationSys.GetARNList(true)
	if len(arns) == 0 {
		return
	}

	arnMsg := color.Blue("SQS ARNs: ")
	for _, arn := range arns {
		arnMsg += color.Bold(fmt.Sprintf(getFormatStr(len(arn), 1), arn))
	}

	logStartupMessage(arnMsg)
}

// Prints startup message for command line access. Prints link to our documentation
// and custom platform specific message.
func printCLIAccessMsg(endPoint string, alias string) {
	// Get saved credentials.
	cred := globalActiveCred

	// Configure 'mc', following block prints platform specific information for minio client.
	if color.IsTerminal() && !globalCLIContext.Anonymous {
		logStartupMessage(color.Blue("\nCommand-line Access: ") + mcQuickStartGuide)
		if runtime.GOOS == globalWindowsOSName {
			mcMessage := fmt.Sprintf("$ mc.exe config host add %s %s %s %s", alias,
				endPoint, cred.AccessKey, cred.SecretKey)
			logStartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
		} else {
			mcMessage := fmt.Sprintf("$ mc config host add %s %s %s %s", alias,
				endPoint, cred.AccessKey, cred.SecretKey)
			logStartupMessage(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
		}
	}
}

// Prints startup message for Object API acces, prints link to our SDK documentation.
func printObjectAPIMsg() {
	logStartupMessage(color.Blue("\nObject API (Amazon S3 compatible):"))
	logStartupMessage(color.Blue("   Go: ") + fmt.Sprintf(getFormatStr(len(goQuickStartGuide), 8), goQuickStartGuide))
	logStartupMessage(color.Blue("   Java: ") + fmt.Sprintf(getFormatStr(len(javaQuickStartGuide), 6), javaQuickStartGuide))
	logStartupMessage(color.Blue("   Python: ") + fmt.Sprintf(getFormatStr(len(pyQuickStartGuide), 4), pyQuickStartGuide))
	logStartupMessage(color.Blue("   JavaScript: ") + jsQuickStartGuide)
	logStartupMessage(color.Blue("   .NET: ") + fmt.Sprintf(getFormatStr(len(dotnetQuickStartGuide), 6), dotnetQuickStartGuide))
}

// Get formatted disk/storage info message.
func getStorageInfoMsgSafeMode(storageInfo StorageInfo) string {
	var msg string
	var mcMessage string
	if storageInfo.Backend.Type == BackendErasure {
		if storageInfo.Backend.OfflineDisks.Sum() > 0 {
			mcMessage = "Use `mc admin info` to look for latest server/disk info\n"
		}
		diskInfo := fmt.Sprintf(" %d Online, %d Offline. ", storageInfo.Backend.OnlineDisks.Sum(), storageInfo.Backend.OfflineDisks.Sum())
		msg += color.Red("Status:") + fmt.Sprintf(getFormatStr(len(diskInfo), 8), diskInfo)
	}
	if len(mcMessage) > 0 {
		msg = fmt.Sprintf("%s %s", mcMessage, msg)
	}
	return msg
}

// Get formatted disk/storage info message.
func getStorageInfoMsg(storageInfo StorageInfo) string {
	var msg string
	var mcMessage string
	if storageInfo.Backend.Type == BackendErasure {
		if storageInfo.Backend.OfflineDisks.Sum() > 0 {
			mcMessage = "Use `mc admin info` to look for latest server/disk info\n"
		}

		diskInfo := fmt.Sprintf(" %d Online, %d Offline. ", storageInfo.Backend.OnlineDisks.Sum(), storageInfo.Backend.OfflineDisks.Sum())
		msg += color.Blue("Status:") + fmt.Sprintf(getFormatStr(len(diskInfo), 8), diskInfo)
		if len(mcMessage) > 0 {
			msg = fmt.Sprintf("%s %s", mcMessage, msg)
		}
	}
	return msg
}

// Prints startup message of storage capacity and erasure information.
func printStorageInfo(storageInfo StorageInfo) {
	if msg := getStorageInfoMsg(storageInfo); msg != "" {
		if globalCLIContext.Quiet {
			logger.Info(msg)
		}
		logStartupMessage(msg)
	}
}

func printCacheStorageInfo(storageInfo CacheStorageInfo) {
	msg := fmt.Sprintf("%s %s Free, %s Total", color.Blue("Cache Capacity:"),
		humanize.IBytes(uint64(storageInfo.Free)),
		humanize.IBytes(uint64(storageInfo.Total)))
	logStartupMessage(msg)
}

// Prints the certificate expiry message.
func printCertificateMsg(certs []*x509.Certificate) {
	for _, cert := range certs {
		logStartupMessage(config.CertificateText(cert))
	}
}
