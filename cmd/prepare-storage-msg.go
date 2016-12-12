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
	"fmt"
	"net"
	"net/url"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/mc/pkg/console"
)

// Helper to generate integer sequences into a friendlier user consumable format.
func formatInts(i int, t int) string {
	if i < 10 {
		if t < 10 {
			return fmt.Sprintf("0%d/0%d", i, t)
		}
		return fmt.Sprintf("0%d/%d", i, t)
	}
	return fmt.Sprintf("%d/%d", i, t)
}

// Print a given message once.
type printOnceFunc func(msg string)

// Print once is a constructor returning a function printing once.
// internally print uses sync.Once to perform exactly one action.
func printOnceFn() printOnceFunc {
	var once sync.Once
	return func(msg string) {
		once.Do(func() {
			if !globalQuiet {
				console.Println(msg)
			}
		})
	}
}

// Prints custom message when healing is required for XL and Distributed XL backend.
func printHealMsg(endpoints []*url.URL, storageDisks []StorageAPI, fn printOnceFunc) {
	msg := getHealMsg(endpoints, storageDisks)
	fn(msg)
}

// Heal endpoint constructs the final endpoint URL for control heal command.
// Disk heal endpoint needs to be just a URL and no special paths.
// This function constructs the right endpoint under various conditions
// for single node XL, distributed XL and when minio server is bound
// to a specific ip:port.
func getHealEndpoint(tls bool, firstEndpoint *url.URL) (cEndpoint *url.URL) {
	scheme := "http"
	if tls {
		scheme = "https"
	}
	cEndpoint = &url.URL{
		Scheme: scheme,
	}
	// Bind to `--address host:port` was specified.
	if globalMinioHost != "" {
		cEndpoint.Host = net.JoinHostPort(globalMinioHost, globalMinioPort)
		return cEndpoint
	}
	// For distributed XL setup.
	if firstEndpoint.Host != "" {
		cEndpoint.Host = firstEndpoint.Host
		return cEndpoint
	}
	// For single node XL setup, we need to find the endpoint.
	cEndpoint.Host = globalMinioAddr
	// Fetch all the listening ips. For single node XL we
	// just use the first host.
	hosts, _, err := getListenIPs(cEndpoint.Host)
	if err == nil {
		cEndpoint.Host = net.JoinHostPort(hosts[0], globalMinioPort)
	}
	return cEndpoint
}

// Constructs a formatted heal message, when cluster is found to be in state where it requires healing.
// healing is optional, server continues to initialize object layer after printing this message.
// it is upto the end user to perform a heal if needed.
func getHealMsg(endpoints []*url.URL, storageDisks []StorageAPI) string {
	msg := fmt.Sprintln("\nData volume requires HEALING. Healing is not implemented yet stay tuned:")
	// FIXME:  Enable this after we bring in healing.
	//	msg += "MINIO_ACCESS_KEY=%s "
	//	msg += "MINIO_SECRET_KEY=%s "
	//	msg += "minio control heal %s"
	//	creds := serverConfig.GetCredential()
	//	msg = fmt.Sprintf(msg, creds.AccessKeyID, creds.SecretAccessKey, getHealEndpoint(isSSL(), endpoints[0]))
	disksInfo, _, _ := getDisksInfo(storageDisks)
	for i, info := range disksInfo {
		if storageDisks[i] == nil {
			continue
		}
		msg += fmt.Sprintf(
			"\n[%s] %s - %s %s",
			formatInts(i+1, len(storageDisks)),
			endpoints[i],
			humanize.IBytes(uint64(info.Total)),
			func() string {
				if info.Total > 0 {
					return "online"
				}
				return "offline"
			}(),
		)
	}
	return msg
}

// Prints regular message when we have sufficient disks to start the cluster.
func printRegularMsg(endpoints []*url.URL, storageDisks []StorageAPI, fn printOnceFunc) {
	msg := getStorageInitMsg("\nInitializing data volume.", endpoints, storageDisks)
	fn(msg)
}

// Constructs a formatted regular message when we have sufficient disks to start the cluster.
func getStorageInitMsg(titleMsg string, endpoints []*url.URL, storageDisks []StorageAPI) string {
	msg := colorBlue(titleMsg)
	disksInfo, _, _ := getDisksInfo(storageDisks)
	for i, info := range disksInfo {
		if storageDisks[i] == nil {
			continue
		}
		msg += fmt.Sprintf(
			"\n[%s] %s - %s %s",
			formatInts(i+1, len(storageDisks)),
			endpoints[i],
			humanize.IBytes(uint64(info.Total)),
			func() string {
				if info.Total > 0 {
					return "online"
				}
				return "offline"
			}(),
		)
	}
	return msg
}

// Prints initialization message when cluster is being initialized for the first time.
func printFormatMsg(endpoints []*url.URL, storageDisks []StorageAPI, fn printOnceFunc) {
	msg := getStorageInitMsg("\nInitializing data volume for the first time.", endpoints, storageDisks)
	fn(msg)
}

func printConfigErrMsg(storageDisks []StorageAPI, sErrs []error, fn printOnceFunc) {
	msg := getConfigErrMsg(storageDisks, sErrs)
	fn(msg)
}

// Generate a formatted message when cluster is misconfigured.
func getConfigErrMsg(storageDisks []StorageAPI, sErrs []error) string {
	msg := colorBlue("\nDetected configuration inconsistencies in the cluster. Please fix following servers.")
	for i, disk := range storageDisks {
		if disk == nil {
			continue
		}
		if sErrs[i] == nil {
			continue
		}
		msg += fmt.Sprintf(
			"\n[%s] %s : %s",
			formatInts(i+1, len(storageDisks)),
			storageDisks[i],
			sErrs[i],
		)
	}
	return msg
}
