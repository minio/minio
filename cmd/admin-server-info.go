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
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger"
)

// getLocalServerProperty - returns madmin.ServerProperties for only the
// local endpoints from given list of endpoints
func getLocalServerProperty(endpointServerPools EndpointServerPools, r *http.Request) madmin.ServerProperties {
	var localEndpoints Endpoints
	addr := globalLocalNodeName
	if r != nil {
		addr = r.Host
	}
	if globalIsDistErasure {
		addr = globalLocalNodeName
	}
	network := make(map[string]string)
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			nodeName := endpoint.Host
			if nodeName == "" {
				nodeName = addr
			}
			if endpoint.IsLocal {
				// Only proceed for local endpoints
				network[nodeName] = string(madmin.ItemOnline)
				localEndpoints = append(localEndpoints, endpoint)
				continue
			}
			_, present := network[nodeName]
			if !present {
				if err := isServerResolvable(endpoint, 5*time.Second); err == nil {
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

	gcStats := debug.GCStats{
		// If stats.PauseQuantiles is non-empty, ReadGCStats fills
		// it with quantiles summarizing the distribution of pause time.
		// For example, if len(stats.PauseQuantiles) is 5, it will be
		// filled with the minimum, 25%, 50%, 75%, and maximum pause times.
		PauseQuantiles: make([]time.Duration, 5),
	}
	debug.ReadGCStats(&gcStats)
	// Truncate GC stats to max 5 entries.
	if len(gcStats.PauseEnd) > 5 {
		gcStats.PauseEnd = gcStats.PauseEnd[len(gcStats.PauseEnd)-5:]
	}
	if len(gcStats.Pause) > 5 {
		gcStats.Pause = gcStats.Pause[len(gcStats.Pause)-5:]
	}

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
		GoMaxProcs:     runtime.GOMAXPROCS(0),
		NumCPU:         runtime.NumCPU(),
		RuntimeVersion: runtime.Version(),
		GCStats: &madmin.GCStats{
			LastGC:     gcStats.LastGC,
			NumGC:      gcStats.NumGC,
			PauseTotal: gcStats.PauseTotal,
			Pause:      gcStats.Pause,
			PauseEnd:   gcStats.PauseEnd,
		},
		MinioEnvVars: make(map[string]string, 10),
	}

	sensitive := map[string]struct{}{
		config.EnvAccessKey:         {},
		config.EnvSecretKey:         {},
		config.EnvRootUser:          {},
		config.EnvRootPassword:      {},
		config.EnvMinIOSubnetAPIKey: {},
		config.EnvKMSSecretKey:      {},
	}
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "MINIO") && !strings.HasPrefix(v, "_MINIO") {
			continue
		}
		split := strings.SplitN(v, "=", 2)
		key := split[0]
		value := ""
		if len(split) > 1 {
			value = split[1]
		}

		// Do not send sensitive creds.
		if _, ok := sensitive[key]; ok || strings.Contains(strings.ToLower(key), "password") || strings.HasSuffix(strings.ToLower(key), "key") {
			props.MinioEnvVars[key] = "*** EXISTS, REDACTED ***"
			continue
		}
		props.MinioEnvVars[key] = value
	}

	objLayer := newObjectLayerFn()
	if objLayer != nil {
		// only need Disks information in server mode.
		storageInfo, _ := objLayer.LocalStorageInfo(GlobalContext)
		props.State = string(madmin.ItemOnline)
		props.Disks = storageInfo.Disks
	}

	return props
}
