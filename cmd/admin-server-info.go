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
	"net/http"
	"os"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/madmin"
)

// getLocalServerProperty - returns madmin.ServerProperties for only the
// local endpoints from given list of endpoints
func getLocalServerProperty(endpointZones EndpointZones, r *http.Request) madmin.ServerProperties {
	var disks []madmin.Disk
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(endpointZones)
	}
	network := make(map[string]string)
	for _, ep := range endpointZones {
		for _, endpoint := range ep.Endpoints {
			nodeName := endpoint.Host
			if nodeName == "" {
				nodeName = r.Host
			}
			if endpoint.IsLocal {
				// Only proceed for local endpoints
				network[nodeName] = "online"
				var di = madmin.Disk{
					DrivePath: endpoint.Path,
				}
				diInfo, err := disk.GetInfo(endpoint.Path)
				if err != nil {
					if os.IsNotExist(err) || isSysErrPathNotFound(err) {
						di.State = madmin.DriveStateMissing
					} else {
						di.State = madmin.DriveStateCorrupt
					}
				} else {
					di.State = madmin.DriveStateOk
					di.DrivePath = endpoint.Path
					di.TotalSpace = diInfo.Total
					di.UsedSpace = diInfo.Total - diInfo.Free
					di.Utilization = float64((diInfo.Total - diInfo.Free) / diInfo.Total * 100)
				}
				disks = append(disks, di)
			} else {
				_, present := network[nodeName]
				if !present {
					err := IsServerResolvable(endpoint)
					if err == nil {
						network[nodeName] = "online"
					} else {
						network[nodeName] = "offline"
					}
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
		Disks:    disks,
	}
}
