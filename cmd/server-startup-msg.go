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
	"crypto/x509"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	humanize "github.com/dustin/go-humanize"
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

	// Object layer is initialized then print StorageInfo.
	objAPI := newObjectLayerFn()
	if objAPI != nil {
		printStorageInfo(objAPI.StorageInfo())
		// Storage class info only printed for Erasure backend
		if objAPI.StorageInfo().Backend.Type == Erasure {
			printStorageClassInfoMsg(objAPI.StorageInfo())
		}
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
	log.Println(colorBlue("Endpoint: ") + colorBold(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 1), apiEndpointStr)))
	log.Println(colorBlue("AccessKey: ") + colorBold(fmt.Sprintf("%s ", cred.AccessKey)))
	log.Println(colorBlue("SecretKey: ") + colorBold(fmt.Sprintf("%s ", cred.SecretKey)))
	if region != "" {
		log.Println(colorBlue("Region: ") + colorBold(fmt.Sprintf(getFormatStr(len(region), 3), region)))
	}
	printEventNotifiers()

	if globalIsBrowserEnabled {
		log.Println(colorBlue("\nBrowser Access:"))
		log.Println(fmt.Sprintf(getFormatStr(len(apiEndpointStr), 3), apiEndpointStr))
	}
}

// Prints bucket notification configurations.
func printEventNotifiers() {
	if globalEventNotifier == nil {
		// In case initEventNotifier() was not done or failed.
		return
	}
	// Get all configured external notification targets
	externalTargets := globalEventNotifier.GetAllExternalTargets()
	if len(externalTargets) == 0 {
		return
	}
	arnMsg := colorBlue("SQS ARNs: ")
	for queueArn := range externalTargets {
		arnMsg += colorBold(fmt.Sprintf(getFormatStr(len(queueArn), 1), queueArn))
	}
	log.Println(arnMsg)
}

// Prints startup message for command line access. Prints link to our documentation
// and custom platform specific message.
func printCLIAccessMsg(endPoint string, alias string) {
	// Get saved credentials.
	cred := globalServerConfig.GetCredential()

	// Configure 'mc', following block prints platform specific information for minio client.
	log.Println(colorBlue("\nCommand-line Access: ") + mcQuickStartGuide)
	if runtime.GOOS == globalWindowsOSName {
		mcMessage := fmt.Sprintf("$ mc.exe config host add %s %s %s %s", alias, endPoint, cred.AccessKey, cred.SecretKey)
		log.Println(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	} else {
		mcMessage := fmt.Sprintf("$ mc config host add %s %s %s %s", alias, endPoint, cred.AccessKey, cred.SecretKey)
		log.Println(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	}
}

// Prints startup message for Object API acces, prints link to our SDK documentation.
func printObjectAPIMsg() {
	log.Println(colorBlue("\nObject API (Amazon S3 compatible):"))
	log.Println(colorBlue("   Go: ") + fmt.Sprintf(getFormatStr(len(goQuickStartGuide), 8), goQuickStartGuide))
	log.Println(colorBlue("   Java: ") + fmt.Sprintf(getFormatStr(len(javaQuickStartGuide), 6), javaQuickStartGuide))
	log.Println(colorBlue("   Python: ") + fmt.Sprintf(getFormatStr(len(pyQuickStartGuide), 4), pyQuickStartGuide))
	log.Println(colorBlue("   JavaScript: ") + jsQuickStartGuide)
	log.Println(colorBlue("   .NET: ") + fmt.Sprintf(getFormatStr(len(dotnetQuickStartGuide), 6), dotnetQuickStartGuide))
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

func printStorageClassInfoMsg(storageInfo StorageInfo) {
	standardClassMsg := getStandardStorageClassInfoMsg(storageInfo)
	rrsClassMsg := getRRSStorageClassInfoMsg(storageInfo)
	storageClassMsg := fmt.Sprintf(getFormatStr(len(standardClassMsg), 3), standardClassMsg) + fmt.Sprintf(getFormatStr(len(rrsClassMsg), 3), rrsClassMsg)
	// Print storage class section only if data is present
	if storageClassMsg != "" {
		log.Println(colorBlue("Storage Class:"))
		log.Println(storageClassMsg)
	}
}

func getStandardStorageClassInfoMsg(storageInfo StorageInfo) string {
	var msg string
	if maxDiskFailures := storageInfo.Backend.StandardSCParity - storageInfo.Backend.OfflineDisks; maxDiskFailures >= 0 {
		msg += fmt.Sprintf("Objects with "+standardStorageClass+" class can withstand [%d] drive failure(s).\n", maxDiskFailures)
	}
	return msg
}

func getRRSStorageClassInfoMsg(storageInfo StorageInfo) string {
	var msg string
	if maxDiskFailures := storageInfo.Backend.RRSCParity - storageInfo.Backend.OfflineDisks; maxDiskFailures >= 0 {
		msg += fmt.Sprintf("Objects with "+reducedRedundancyStorageClass+" class can withstand [%d] drive failure(s).\n", maxDiskFailures)
	}
	return msg
}

// Prints startup message of storage capacity and erasure information.
func printStorageInfo(storageInfo StorageInfo) {
	log.Println(getStorageInfoMsg(storageInfo))
	log.Println()
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
	log.Println(getCertificateChainMsg(certs))
}
