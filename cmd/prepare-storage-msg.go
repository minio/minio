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
	"sync"

	humanize "github.com/dustin/go-humanize"
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
			log.Println(msg)
		})
	}
}

// Prints custom message when healing is required for XL and Distributed XL backend.
func printHealMsg(endpoints EndpointList, storageDisks []StorageAPI, fn printOnceFunc) {
	msg := getHealMsg(endpoints, storageDisks)
	fn(msg)
}

// Disks offline and online strings..
const (
	diskOffline = "offline"
	diskOnline  = "online"
)

// Constructs a formatted heal message, when cluster is found to be in state where it requires healing.
// healing is optional, server continues to initialize object layer after printing this message.
// it is upto the end user to perform a heal if needed.
func getHealMsg(endpoints EndpointList, storageDisks []StorageAPI) string {
	healFmtCmd := `"mc admin heal myminio"`
	msg := fmt.Sprintf("New disk(s) were found, format them by running - %s\n",
		healFmtCmd)
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
					return diskOnline
				}
				return diskOffline
			}(),
		)
	}
	return msg
}

// Prints regular message when we have sufficient disks to start the cluster.
func printRegularMsg(endpoints EndpointList, storageDisks []StorageAPI, fn printOnceFunc) {
	msg := getStorageInitMsg("Initializing data volume.", endpoints, storageDisks)
	fn(msg)
}

// Constructs a formatted regular message when we have sufficient disks to start the cluster.
func getStorageInitMsg(titleMsg string, endpoints EndpointList, storageDisks []StorageAPI) string {
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
					return diskOnline
				}
				return diskOffline
			}(),
		)
	}
	return msg
}

// Prints initialization message when cluster is being initialized for the first time.
func printFormatMsg(endpoints EndpointList, storageDisks []StorageAPI, fn printOnceFunc) {
	msg := getStorageInitMsg("Initializing data volume for the first time.", endpoints, storageDisks)
	fn(msg)
}

// Combines each disk errors in a newline formatted string.
// this is a helper function in printing messages across
// all disks.
func combineDiskErrs(storageDisks []StorageAPI, sErrs []error) string {
	var msg string
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
