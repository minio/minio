// Copyright (c) 2015-2023 MinIO, Inc.
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

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help holds configuration keys and their default values for api subsystem.
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         apiRequestsMax,
			Description: `set the maximum number of concurrent requests (default: auto)`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         apiClusterDeadline,
			Description: `set the deadline for cluster readiness check` + defaultHelpPostfix(apiClusterDeadline),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiCorsAllowOrigin,
			Description: `set comma separated list of origins allowed for CORS requests` + defaultHelpPostfix(apiCorsAllowOrigin),
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         apiRemoteTransportDeadline,
			Description: `set the deadline for API requests on remote transports while proxying between federated instances e.g. "2h"` + defaultHelpPostfix(apiRemoteTransportDeadline),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiListQuorum,
			Description: `set the acceptable quorum expected for list operations e.g. "optimal", "reduced", "disk", "strict", "auto"` + defaultHelpPostfix(apiListQuorum),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         apiReplicationPriority,
			Description: `set replication priority` + defaultHelpPostfix(apiReplicationPriority),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         apiReplicationMaxWorkers,
			Description: `set the maximum number of replication workers` + defaultHelpPostfix(apiReplicationMaxWorkers),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         apiTransitionWorkers,
			Description: `set the number of transition workers` + defaultHelpPostfix(apiTransitionWorkers),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         apiStaleUploadsExpiry,
			Description: `set to expire stale multipart uploads older than this values` + defaultHelpPostfix(apiStaleUploadsExpiry),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiStaleUploadsCleanupInterval,
			Description: `set to change intervals when stale multipart uploads are expired` + defaultHelpPostfix(apiStaleUploadsCleanupInterval),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiDeleteCleanupInterval,
			Description: `set to change intervals when deleted objects are permanently deleted from ".trash" folder` + defaultHelpPostfix(apiDeleteCleanupInterval),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiODirect,
			Description: "set to enable or disable O_DIRECT for writes under special conditions. NOTE: do not disable O_DIRECT without prior testing" + defaultHelpPostfix(apiODirect),
			Optional:    true,
			Type:        "boolean",
		},
		config.HelpKV{
			Key:         apiRootAccess,
			Description: "turn 'off' root credential access for all API calls including s3, admin operations" + defaultHelpPostfix(apiRootAccess),
			Optional:    true,
			Type:        "boolean",
		},
		config.HelpKV{
			Key:         apiSyncEvents,
			Description: "set to enable synchronous bucket notifications" + defaultHelpPostfix(apiSyncEvents),
			Optional:    true,
			Type:        "boolean",
		},
		config.HelpKV{
			Key:         apiObjectMaxVersions,
			Description: "set max allowed number of versions per object" + defaultHelpPostfix(apiObjectMaxVersions),
			Optional:    true,
			Type:        "number",
		},
	}
)
