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

package api

import "github.com/minio/minio/internal/config"

// Help template for storageclass feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         apiRequestsMax,
			Description: `set the maximum number of concurrent requests, e.g. "1600"`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         apiRequestsDeadline,
			Description: `set the deadline for API requests waiting to be processed e.g. "1m"`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiCorsAllowOrigin,
			Description: `set comma separated list of origins allowed for CORS requests e.g. "https://example1.com,https://example2.com"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         apiRemoteTransportDeadline,
			Description: `set the deadline for API requests on remote transports while proxying between federated instances e.g. "2h"`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiReplicationWorkers,
			Description: `set the number of replication workers, defaults to 100`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         apiReplicationFailedWorkers,
			Description: `set the number of replication workers for recently failed replicas, defaults to 4`,
			Optional:    true,
			Type:        "number",
		},
	}
)
