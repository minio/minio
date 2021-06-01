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
	"context"
	"net/http"
	"runtime"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
)

// getLocalServerProperty - returns madmin.ServerProperties for only the
// local endpoints from given list of endpoints
func getLocalServerProperty(endpointServerPools EndpointServerPools, r *http.Request) madmin.ServerProperties {
	var localEndpoints Endpoints
	addr := r.Host
	if globalIsDistErasure {
		addr = globalLocalNodeName
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
				network[nodeName] = string(madmin.ItemOnline)
				localEndpoints = append(localEndpoints, endpoint)
				continue
			}
			_, present := network[nodeName]
			if !present {
				if err := isServerResolvable(endpoint, 2*time.Second); err == nil {
					network[nodeName] = string(madmin.ItemOnline)
				} else {
					network[nodeName] = string(madmin.ItemOffline)
					// log once the error
					logger.LogOnceIf(context.Background(), err, nodeName)
				}
			}
		}
	}

	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	props := madmin.ServerProperties{
		State:    string(madmin.ItemInitializing),
		Endpoint: addr,
		Uptime:   UTCNow().Unix() - globalBootTime.Unix(),
		Version:  Version,
		CommitID: CommitID,
		Network:  network,
		MemStats: madmin.MemStats{
			Alloc:      memstats.Alloc,
			TotalAlloc: memstats.TotalAlloc,
			Mallocs:    memstats.Mallocs,
			Frees:      memstats.Frees,
			HeapAlloc:  memstats.HeapAlloc,
		},
	}

	objLayer := newObjectLayerFn()
	if objLayer != nil && !globalIsGateway {
		// only need Disks information in server mode.
		storageInfo, _ := objLayer.LocalStorageInfo(GlobalContext)
		props.State = string(madmin.ItemOnline)
		props.Disks = storageInfo.Disks
	}

	return props
}
