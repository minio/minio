// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	xnet "github.com/minio/pkg/v3/net"

	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/logger"
)

// generates format string depending on the string length and padding.
func getFormatStr(strLen int, padding int) string {
	formatStr := fmt.Sprintf("%ds", strLen+padding)
	return "%" + formatStr
}

// Prints the formatted startup message.
func printStartupMessage(apiEndpoints []string, err error) {
	banner := strings.Repeat("-", len(MinioBannerName))
	if globalIsDistErasure {
		logger.Startup(color.Bold(banner))
	}
	logger.Startup(color.Bold(MinioBannerName))
	if err != nil {
		if globalConsoleSys != nil {
			globalConsoleSys.Send(GlobalContext, fmt.Sprintf("Server startup failed with '%v', some features may be missing", err))
		}
	}

	if !globalSubnetConfig.Registered() {
		var builder strings.Builder
		startupBanner(&builder)
		logger.Startup(builder.String())
	}

	strippedAPIEndpoints := stripStandardPorts(apiEndpoints, globalMinioHost)

	// Prints credential, region and browser access.
	printServerCommonMsg(strippedAPIEndpoints)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	printCLIAccessMsg(strippedAPIEndpoints[0], "myminio")

	// Prints documentation message.
	printObjectAPIMsg()
	if globalIsDistErasure {
		logger.Startup(color.Bold(banner))
	}
}

// Returns true if input is IPv6
func isIPv6(host string) bool {
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	ip := net.ParseIP(h)
	return ip.To16() != nil && ip.To4() == nil
}

// strip api endpoints list with standard ports such as
// port "80" and "443" before displaying on the startup
// banner.  Returns a new list of API endpoints.
func stripStandardPorts(apiEndpoints []string, host string) (newAPIEndpoints []string) {
	if len(apiEndpoints) == 1 {
		return apiEndpoints
	}
	newAPIEndpoints = make([]string, len(apiEndpoints))
	// Check all API endpoints for standard ports and strip them.
	for i, apiEndpoint := range apiEndpoints {
		_, err := xnet.ParseHTTPURL(apiEndpoint)
		if err != nil {
			continue
		}
		u, err := url.Parse(apiEndpoint)
		if err != nil {
			continue
		}
		if host == "" && isIPv6(u.Hostname()) {
			// Skip all IPv6 endpoints
			continue
		}
		if u.Port() == "80" && u.Scheme == "http" || u.Port() == "443" && u.Scheme == "https" {
			u.Host = u.Hostname()
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
	region := globalSite.Region()

	apiEndpointStr := strings.TrimSpace(strings.Join(apiEndpoints, "  "))
	// Colorize the message and print.
	logger.Startup(color.Blue("API: ") + color.Bold(fmt.Sprintf("%s ", apiEndpointStr)))
	if color.IsTerminal() && (!globalServerCtxt.Anonymous && !globalServerCtxt.JSON && globalAPIConfig.permitRootAccess()) {
		logger.Startup(color.Blue("   RootUser: ") + color.Bold("%s ", cred.AccessKey))
		logger.Startup(color.Blue("   RootPass: ") + color.Bold("%s \n", cred.SecretKey))
		if region != "" {
			logger.Startup(color.Blue("   Region: ") + color.Bold("%s", fmt.Sprintf(getFormatStr(len(region), 2), region)))
		}
	}

	if globalBrowserEnabled {
		consoleEndpointStr := strings.Join(stripStandardPorts(getConsoleEndpoints(), globalMinioConsoleHost), " ")
		logger.Startup(color.Blue("WebUI: ") + color.Bold(fmt.Sprintf("%s ", consoleEndpointStr)))
		if color.IsTerminal() && (!globalServerCtxt.Anonymous && !globalServerCtxt.JSON && globalAPIConfig.permitRootAccess()) {
			logger.Startup(color.Blue("   RootUser: ") + color.Bold("%s ", cred.AccessKey))
			logger.Startup(color.Blue("   RootPass: ") + color.Bold("%s ", cred.SecretKey))
		}
	}

	printEventNotifiers()
	printLambdaTargets()
}

// Prints startup message for Object API access, prints link to our SDK documentation.
func printObjectAPIMsg() {
	logger.Startup(color.Blue("\nDocs: ") + "https://docs.min.io")
}

func printLambdaTargets() {
	if globalLambdaTargetList == nil || globalLambdaTargetList.Empty() {
		return
	}

	arnMsg := color.Blue("Object Lambda ARNs: ")
	for _, arn := range globalLambdaTargetList.List(globalSite.Region()) {
		arnMsg += color.Bold(fmt.Sprintf("%s ", arn))
	}
	logger.Startup(arnMsg + "\n")
}

// Prints bucket notification configurations.
func printEventNotifiers() {
	if globalNotificationSys == nil {
		return
	}

	arns := globalEventNotifier.GetARNList()
	if len(arns) == 0 {
		return
	}

	arnMsg := color.Blue("SQS ARNs: ")
	for _, arn := range arns {
		arnMsg += color.Bold(fmt.Sprintf("%s ", arn))
	}

	logger.Startup(arnMsg + "\n")
}

// Prints startup message for command line access. Prints link to our documentation
// and custom platform specific message.
func printCLIAccessMsg(endPoint string, alias string) {
	// Get saved credentials.
	cred := globalActiveCred

	const mcQuickStartGuide = "https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart"

	// Configure 'mc', following block prints platform specific information for minio client.
	if color.IsTerminal() && (!globalServerCtxt.Anonymous && globalAPIConfig.permitRootAccess()) {
		logger.Startup(color.Blue("\nCLI: ") + mcQuickStartGuide)
		mcMessage := fmt.Sprintf("$ mc alias set '%s' '%s' '%s' '%s'", alias,
			endPoint, cred.AccessKey, cred.SecretKey)
		logger.Startup(fmt.Sprintf(getFormatStr(len(mcMessage), 3), mcMessage))
	}
}
