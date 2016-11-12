/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"runtime"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/mc/pkg/console"
)

// Documentation links, these are part of message printing code.
const (
	mcQuickStartGuide   = "https://docs.minio.io/docs/minio-client-quickstart-guide"
	goQuickStartGuide   = "https://docs.minio.io/docs/golang-client-quickstart-guide"
	jsQuickStartGuide   = "https://docs.minio.io/docs/javascript-client-quickstart-guide"
	javaQuickStartGuide = "https://docs.minio.io/docs/java-client-quickstart-guide"
	pyQuickStartGuide   = "https://docs.minio.io/docs/python-client-quickstart-guide"
)

// generates format string depending on the string length and padding.
func getFormatStr(strLen int, padding int) string {
	formatStr := fmt.Sprintf("%ds", strLen+padding)
	return "%" + formatStr
}

// Prints the formatted startup message.
func printStartupMessage(endPoints []string) {
	// If quiet flag is set do not print startup message.
	if globalQuiet {
		return
	}
	printServerCommonMsg(endPoints)
	printCLIAccessMsg(endPoints[0])
	printObjectAPIMsg()

	objAPI := newObjectLayerFn()
	if objAPI != nil {
		printStorageInfo(objAPI.StorageInfo())
	}

	if isSSL() {
		certs, err := readCertificateChain()
		fatalIf(err, "Unable to read certificate chain.")
		printCertificateMsg(certs)
	}
}

// Prints common server startup message. Prints credential, region and browser access.
func printServerCommonMsg(endPoints []string) {
	// Get saved credentials.
	cred := serverConfig.GetCredential()

	// Get saved region.
	region := serverConfig.GetRegion()

	endPointStr := strings.Join(endPoints, "  ")
	// Colorize the message and print.
	console.Println(colorBlue("\nEndpoint: ") + colorBold(fmt.Sprintf(getFormatStr(len(endPointStr), 1), endPointStr)))
	console.Println(colorBlue("AccessKey: ") + colorBold(fmt.Sprintf("%s ", cred.AccessKeyID)))
	console.Println(colorBlue("SecretKey: ") + colorBold(fmt.Sprintf("%s ", cred.SecretAccessKey)))
	console.Println(colorBlue("Region: ") + colorBold(fmt.Sprintf(getFormatStr(len(region), 3), region)))
	printEventNotifiers()

	console.Println(colorBlue("\nBrowser Access:"))
	console.Println(fmt.Sprintf(getFormatStr(len(endPointStr), 3), endPointStr))
}

// Prints bucket notification configurations.
func printEventNotifiers() {
	if globalEventNotifier == nil {
		// In case initEventNotifier() was not done or failed.
		return
	}
	arnMsg := colorBlue("SQS ARNs: ")
	if len(globalEventNotifier.external.targets) == 0 {
		arnMsg += colorBold(fmt.Sprintf(getFormatStr(len("<none>"), 1), "<none>"))
	}
	for queueArn := range globalEventNotifier.external.targets {
		arnMsg += colorBold(fmt.Sprintf(getFormatStr(len(queueArn), 1), queueArn))
	}
	console.Println(arnMsg)
}

// Prints startup message for command line access. Prints link to our documentation
// and custom platform specific message.
func printCLIAccessMsg(endPoint string) {
	// Get saved credentials.
	cred := serverConfig.GetCredential()

	// Configure 'mc', following block prints platform specific information for minio client.
	console.Println(colorBlue("\nCommand-line Access: ") + mcQuickStartGuide)
	if runtime.GOOS == "windows" {
		mcMessage := fmt.Sprintf("$ mc.exe config host add myminio %s %s %s", endPoint, cred.AccessKeyID, cred.SecretAccessKey)
		console.Println(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	} else {
		mcMessage := fmt.Sprintf("$ mc config host add myminio %s %s %s", endPoint, cred.AccessKeyID, cred.SecretAccessKey)
		console.Println(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	}
}

// Prints startup message for Object API acces, prints link to our SDK documentation.
func printObjectAPIMsg() {
	console.Println(colorBlue("\nObject API (Amazon S3 compatible):"))
	console.Println(colorBlue("   Go: ") + fmt.Sprintf(getFormatStr(len(goQuickStartGuide), 8), goQuickStartGuide))
	console.Println(colorBlue("   Java: ") + fmt.Sprintf(getFormatStr(len(javaQuickStartGuide), 6), javaQuickStartGuide))
	console.Println(colorBlue("   Python: ") + fmt.Sprintf(getFormatStr(len(pyQuickStartGuide), 4), pyQuickStartGuide))
	console.Println(colorBlue("   JavaScript: ") + jsQuickStartGuide)
}

// Get formatted disk/storage info message.
func getStorageInfoMsg(storageInfo StorageInfo) string {
	msg := fmt.Sprintf("%s %s Free, %s Total", colorBlue("Drive Capacity:"),
		humanize.IBytes(uint64(storageInfo.Free)),
		humanize.IBytes(uint64(storageInfo.Total)))
	if storageInfo.Backend.Type == XL {
		diskInfo := fmt.Sprintf(" %d Online, %d Offline. ", storageInfo.Backend.OnlineDisks, storageInfo.Backend.OfflineDisks)
		if maxDiskFailures := storageInfo.Backend.ReadQuorum - storageInfo.Backend.OfflineDisks; maxDiskFailures >= 0 {
			diskInfo += fmt.Sprintf("We can withstand [%d] more drive failure(s).", maxDiskFailures)
		}
		msg += colorBlue("\nStatus:") + fmt.Sprintf(getFormatStr(len(diskInfo), 8), diskInfo)
	}
	return msg
}

// Prints startup message of storage capacity and erasure information.
func printStorageInfo(storageInfo StorageInfo) {
	console.Println()
	console.Println(getStorageInfoMsg(storageInfo))
}

// Prints certificate expiry date warning
func getCertificateChainMsg(certs []*x509.Certificate) string {
	msg := colorBlue("\nCertificate expiry info:\n")
	totalCerts := len(certs)
	var expiringCerts int
	for i := totalCerts - 1; i >= 0; i-- {
		cert := certs[i]
		if cert.NotAfter.Before(time.Now().UTC().Add(globalMinioCertExpireWarnDays)) {
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
	console.Println(getCertificateChainMsg(certs))
}
