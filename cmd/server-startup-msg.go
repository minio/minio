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
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
)

// Documentation links, these are part of message printing code.
const (
	mcQuickStartGuide     = "https://docs.min.io/docs/minio-client-quickstart-guide"
	goQuickStartGuide     = "https://docs.min.io/docs/golang-client-quickstart-guide"
	jsQuickStartGuide     = "https://docs.min.io/docs/javascript-client-quickstart-guide"
	javaQuickStartGuide   = "https://docs.min.io/docs/java-client-quickstart-guide"
	pyQuickStartGuide     = "https://docs.min.io/docs/python-client-quickstart-guide"
	dotnetQuickStartGuide = "https://docs.min.io/docs/dotnet-client-quickstart-guide"
)

// generates format string depending on the string length and padding.
func GetFormatStr(strLen int, padding int) string {
	formatStr := fmt.Sprintf("%ds", strLen+padding)
	return "%" + formatStr
}

func mustGetStorageInfo(objAPI ObjectLayer) StorageInfo {
	storageInfo, _ := objAPI.StorageInfo(GlobalContext)
	return storageInfo
}

// Prints the formatted startup message.
func printStartupMessage(apiEndpoints []string, err error) {
	if err != nil {
		LogStartupMessage(color.RedBold("Server startup failed with '%v'", err))
		LogStartupMessage(color.RedBold("Not all features may be available on this server"))
		LogStartupMessage(color.RedBold("Please use 'mc admin' commands to further investigate this issue"))
	}

	strippedAPIEndpoints := StripStandardPorts(apiEndpoints)
	// If cache layer is enabled, print cache capacity.
	cachedObjAPI := NewCachedObjectLayerFn()
	if cachedObjAPI != nil {
		PrintCacheStorageInfo(cachedObjAPI.StorageInfo(GlobalContext))
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
	PrintCLIAccessMsg(strippedAPIEndpoints[0], "myminio")

	// Prints documentation message.
	PrintObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if color.IsTerminal() && !GlobalCLIContext.Anonymous {
		if GlobalIsTLS {
			PrintCertificateMsg(GlobalPublicCerts)
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
func StripStandardPorts(apiEndpoints []string) (newAPIEndpoints []string) {
	newAPIEndpoints = make([]string, len(apiEndpoints))
	// Check all API endpoints for standard ports and strip them.
	for i, apiEndpoint := range apiEndpoints {
		u, err := xnet.ParseHTTPURL(apiEndpoint)
		if err != nil {
			continue
		}
		if GlobalMinioHost == "" && isNotIPv4(u.Host) {
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
	LogStartupMessage(color.Blue("Endpoint: ") + color.Bold(fmt.Sprintf("%s ", apiEndpointStr)))
	if color.IsTerminal() && !GlobalCLIContext.Anonymous {
		LogStartupMessage(color.Blue("RootUser: ") + color.Bold(fmt.Sprintf("%s ", cred.AccessKey)))
		LogStartupMessage(color.Blue("RootPass: ") + color.Bold(fmt.Sprintf("%s ", cred.SecretKey)))
		if region != "" {
			LogStartupMessage(color.Blue("Region: ") + color.Bold(fmt.Sprintf(GetFormatStr(len(region), 2), region)))
		}
	}
	PrintEventNotifiers()

	if GlobalBrowserEnabled {
		LogStartupMessage(color.Blue("\nBrowser Access:"))
		LogStartupMessage(fmt.Sprintf(GetFormatStr(len(apiEndpointStr), 3), apiEndpointStr))
	}
}

// Prints bucket notification configurations.
func PrintEventNotifiers() {
	if GlobalNotificationSys == nil {
		return
	}

	arns := GlobalNotificationSys.GetARNList(true)
	if len(arns) == 0 {
		return
	}

	arnMsg := color.Blue("SQS ARNs: ")
	for _, arn := range arns {
		arnMsg += color.Bold(fmt.Sprintf("%s ", arn))
	}

	LogStartupMessage(arnMsg)
}

// Prints startup message for command line access. Prints link to our documentation
// and custom platform specific message.
func PrintCLIAccessMsg(endPoint string, alias string) {
	// Get saved credentials.
	cred := globalActiveCred

	// Configure 'mc', following block prints platform specific information for minio client.
	if color.IsTerminal() && !GlobalCLIContext.Anonymous {
		LogStartupMessage(color.Blue("\nCommand-line Access: ") + mcQuickStartGuide)
		if runtime.GOOS == globalWindowsOSName {
			mcMessage := fmt.Sprintf("$ mc.exe alias set %s %s %s %s", alias,
				endPoint, cred.AccessKey, cred.SecretKey)
			LogStartupMessage(fmt.Sprintf(GetFormatStr(len(mcMessage), 3), mcMessage))
		} else {
			mcMessage := fmt.Sprintf("$ mc alias set %s %s %s %s", alias,
				endPoint, cred.AccessKey, cred.SecretKey)
			LogStartupMessage(fmt.Sprintf(GetFormatStr(len(mcMessage), 3), mcMessage))
		}
	}
}

// Prints startup message for Object API acces, prints link to our SDK documentation.
func PrintObjectAPIMsg() {
	LogStartupMessage(color.Blue("\nObject API (Amazon S3 compatible):"))
	LogStartupMessage(color.Blue("   Go: ") + fmt.Sprintf(GetFormatStr(len(goQuickStartGuide), 8), goQuickStartGuide))
	LogStartupMessage(color.Blue("   Java: ") + fmt.Sprintf(GetFormatStr(len(javaQuickStartGuide), 6), javaQuickStartGuide))
	LogStartupMessage(color.Blue("   Python: ") + fmt.Sprintf(GetFormatStr(len(pyQuickStartGuide), 4), pyQuickStartGuide))
	LogStartupMessage(color.Blue("   JavaScript: ") + jsQuickStartGuide)
	LogStartupMessage(color.Blue("   .NET: ") + fmt.Sprintf(GetFormatStr(len(dotnetQuickStartGuide), 6), dotnetQuickStartGuide))
}

// Get formatted disk/storage info message.
func getStorageInfoMsg(storageInfo StorageInfo) string {
	var msg string
	var mcMessage string
	onlineDisks, offlineDisks := getOnlineOfflineDisksStats(storageInfo.Disks)
	if storageInfo.Backend.Type == madmin.Erasure {
		if offlineDisks.Sum() > 0 {
			mcMessage = "Use `mc admin info` to look for latest server/disk info\n"
		}

		diskInfo := fmt.Sprintf(" %d Online, %d Offline. ", onlineDisks.Sum(), offlineDisks.Sum())
		msg += color.Blue("Status:") + fmt.Sprintf(GetFormatStr(len(diskInfo), 8), diskInfo)
		if len(mcMessage) > 0 {
			msg = fmt.Sprintf("%s %s", mcMessage, msg)
		}
	}
	return msg
}

// Prints startup message of storage capacity and erasure information.
func printStorageInfo(storageInfo StorageInfo) {
	if msg := getStorageInfoMsg(storageInfo); msg != "" {
		if GlobalCLIContext.Quiet {
			logger.Info(msg)
		}
		LogStartupMessage(msg)
	}
}

func PrintCacheStorageInfo(storageInfo CacheStorageInfo) {
	msg := fmt.Sprintf("%s %s Free, %s Total", color.Blue("Cache Capacity:"),
		humanize.IBytes(storageInfo.Free),
		humanize.IBytes(storageInfo.Total))
	LogStartupMessage(msg)
}

// Prints the certificate expiry message.
func PrintCertificateMsg(certs []*x509.Certificate) {
	for _, cert := range certs {
		LogStartupMessage(config.CertificateText(cert))
	}
}
