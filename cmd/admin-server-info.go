/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"net/http"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
)

// getLocalServerProperty - returns madmin.ServerProperties for only the
// local endpoints from given list of endpoints
func getLocalServerProperty(endpointServerPools EndpointServerPools, r *http.Request) madmin.ServerProperties {
	var localEndpoints Endpoints
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(endpointServerPools)
	}
	network := make(map[string]string)
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			nodeName := endpoint.Host
			if nodeName == "" {
				nodeName = r.Host
			}
			if endpoint.IsLocal {
				// Only proceed for local endpoints
				network[nodeName] = "online"
				localEndpoints = append(localEndpoints, endpoint)
				continue
			}
			_, present := network[nodeName]
			if !present {
				if err := isServerResolvable(endpoint, 2*time.Second); err == nil {
					network[nodeName] = "online"
				} else {
					network[nodeName] = "offline"
					// log once the error
					logger.LogOnceIf(context.Background(), err, nodeName)
				}
			}
		}
	}

	return madmin.ServerProperties{
		State:    "ok",
		Endpoint: addr,
		Uptime:   UTCNow().Unix() - globalBootTime.Unix(),
		Version:  Version,
		CommitID: CommitID,
		Network:  network,
		Disks:    getLocalDisks(endpointServerPools),
	}
}

func getLocalDisks(endpointServerPools EndpointServerPools) []madmin.Disk {
	var localEndpoints Endpoints
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if !endpoint.IsLocal {
				continue
			}
			localEndpoints = append(localEndpoints, endpoint)
		}
	}

	// FIXME: Recreates disks...
	localDisks, _ := initStorageDisksWithErrors(localEndpoints)
	defer closeStorageDisks(localDisks)
	storageInfo, _ := getStorageInfo(localDisks, localEndpoints.GetAllStrings())
	return storageInfo.Disks
}
