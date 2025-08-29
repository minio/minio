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

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// API sub-system constants
const (
	apiRequestsMax             = "requests_max"
	apiClusterDeadline         = "cluster_deadline"
	apiCorsAllowOrigin         = "cors_allow_origin"
	apiRemoteTransportDeadline = "remote_transport_deadline"
	apiListQuorum              = "list_quorum"
	apiReplicationPriority     = "replication_priority"
	apiReplicationMaxWorkers   = "replication_max_workers"
	apiReplicationMaxLWorkers  = "replication_max_lrg_workers"

	apiTransitionWorkers           = "transition_workers"
	apiStaleUploadsCleanupInterval = "stale_uploads_cleanup_interval"
	apiStaleUploadsExpiry          = "stale_uploads_expiry"
	apiDeleteCleanupInterval       = "delete_cleanup_interval"
	apiDisableODirect              = "disable_odirect"
	apiODirect                     = "odirect"
	apiGzipObjects                 = "gzip_objects"
	apiRootAccess                  = "root_access"
	apiSyncEvents                  = "sync_events"
	apiObjectMaxVersions           = "object_max_versions"

	EnvAPIRequestsMax             = "MINIO_API_REQUESTS_MAX"
	EnvAPIRequestsDeadline        = "MINIO_API_REQUESTS_DEADLINE"
	EnvAPIClusterDeadline         = "MINIO_API_CLUSTER_DEADLINE"
	EnvAPICorsAllowOrigin         = "MINIO_API_CORS_ALLOW_ORIGIN"
	EnvAPIRemoteTransportDeadline = "MINIO_API_REMOTE_TRANSPORT_DEADLINE"
	EnvAPITransitionWorkers       = "MINIO_API_TRANSITION_WORKERS"
	EnvAPIListQuorum              = "MINIO_API_LIST_QUORUM"
	EnvAPISecureCiphers           = "MINIO_API_SECURE_CIPHERS" // default config.EnableOn
	EnvAPIReplicationPriority     = "MINIO_API_REPLICATION_PRIORITY"
	EnvAPIReplicationMaxWorkers   = "MINIO_API_REPLICATION_MAX_WORKERS"
	EnvAPIReplicationMaxLWorkers  = "MINIO_API_REPLICATION_MAX_LRG_WORKERS"

	EnvAPIStaleUploadsCleanupInterval = "MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL"
	EnvAPIStaleUploadsExpiry          = "MINIO_API_STALE_UPLOADS_EXPIRY"
	EnvAPIDeleteCleanupInterval       = "MINIO_API_DELETE_CLEANUP_INTERVAL"
	EnvDeleteCleanupInterval          = "MINIO_DELETE_CLEANUP_INTERVAL"
	EnvAPIODirect                     = "MINIO_API_ODIRECT"
	EnvAPIDisableODirect              = "MINIO_API_DISABLE_ODIRECT"
	EnvAPIGzipObjects                 = "MINIO_API_GZIP_OBJECTS"
	EnvAPIRootAccess                  = "MINIO_API_ROOT_ACCESS" // default config.EnableOn
	EnvAPISyncEvents                  = "MINIO_API_SYNC_EVENTS" // default "off"
	EnvAPIObjectMaxVersions           = "MINIO_API_OBJECT_MAX_VERSIONS"
	EnvAPIObjectMaxVersionsLegacy     = "_MINIO_OBJECT_MAX_VERSIONS"
)

// Deprecated key and ENVs
const (
	apiReadyDeadline            = "ready_deadline"
	apiRequestsDeadline         = "requests_deadline"
	apiReplicationWorkers       = "replication_workers"
	apiReplicationFailedWorkers = "replication_failed_workers"
)

// DefaultKVS - default storage class config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   apiRequestsMax,
			Value: "0",
		},
		config.KV{
			Key:   apiClusterDeadline,
			Value: "10s",
		},
		config.KV{
			Key:   apiCorsAllowOrigin,
			Value: "*",
		},
		config.KV{
			Key:   apiRemoteTransportDeadline,
			Value: "2h",
		},
		config.KV{
			Key:   apiListQuorum,
			Value: "strict",
		},
		config.KV{
			Key:   apiReplicationPriority,
			Value: "auto",
		},
		config.KV{
			Key:   apiReplicationMaxWorkers,
			Value: "500",
		},
		config.KV{
			Key:   apiReplicationMaxLWorkers,
			Value: "10",
		},
		config.KV{
			Key:   apiTransitionWorkers,
			Value: "100",
		},
		config.KV{
			Key:   apiStaleUploadsCleanupInterval,
			Value: "6h",
		},
		config.KV{
			Key:   apiStaleUploadsExpiry,
			Value: "24h",
		},
		config.KV{
			Key:   apiDeleteCleanupInterval,
			Value: "5m",
		},
		config.KV{
			Key:           apiDisableODirect,
			Value:         "",
			HiddenIfEmpty: true,
		},
		config.KV{
			Key:   apiODirect,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   apiGzipObjects,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   apiRootAccess,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   apiSyncEvents,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   apiObjectMaxVersions,
			Value: "9223372036854775807",
		},
	}
)

// Config storage class configuration
type Config struct {
	RequestsMax                 int           `json:"requests_max"`
	ClusterDeadline             time.Duration `json:"cluster_deadline"`
	CorsAllowOrigin             []string      `json:"cors_allow_origin"`
	RemoteTransportDeadline     time.Duration `json:"remote_transport_deadline"`
	ListQuorum                  string        `json:"list_quorum"`
	ReplicationPriority         string        `json:"replication_priority"`
	ReplicationMaxWorkers       int           `json:"replication_max_workers"`
	ReplicationMaxLWorkers      int           `json:"replication_max_lrg_workers"`
	TransitionWorkers           int           `json:"transition_workers"`
	StaleUploadsCleanupInterval time.Duration `json:"stale_uploads_cleanup_interval"`
	StaleUploadsExpiry          time.Duration `json:"stale_uploads_expiry"`
	DeleteCleanupInterval       time.Duration `json:"delete_cleanup_interval"`
	EnableODirect               bool          `json:"enable_odirect"`
	GzipObjects                 bool          `json:"gzip_objects"`
	RootAccess                  bool          `json:"root_access"`
	SyncEvents                  bool          `json:"sync_events"`
	ObjectMaxVersions           int64         `json:"object_max_versions"`
}

// UnmarshalJSON - Validate SS and RRS parity when unmarshalling JSON.
func (sCfg *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(sCfg),
	}
	return json.Unmarshal(data, &aux)
}

// LookupConfig - lookup api config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	deprecatedKeys := []string{
		apiReadyDeadline,
		apiRequestsDeadline,
		"extend_list_cache_life",
		apiReplicationWorkers,
		apiReplicationFailedWorkers,
		"expiry_workers",
	}

	disableODirect := env.Get(EnvAPIDisableODirect, kvs.Get(apiDisableODirect)) == config.EnableOn
	enableODirect := env.Get(EnvAPIODirect, kvs.Get(apiODirect)) == config.EnableOn
	gzipObjects := env.Get(EnvAPIGzipObjects, kvs.Get(apiGzipObjects)) == config.EnableOn
	rootAccess := env.Get(EnvAPIRootAccess, kvs.Get(apiRootAccess)) == config.EnableOn

	cfg = Config{
		EnableODirect: enableODirect || !disableODirect,
		GzipObjects:   gzipObjects,
		RootAccess:    rootAccess,
	}

	var corsAllowOrigin []string
	corsList := env.Get(EnvAPICorsAllowOrigin, kvs.Get(apiCorsAllowOrigin))
	if corsList == "" {
		corsAllowOrigin = []string{"*"} // defaults to '*'
	} else {
		corsAllowOrigin = strings.Split(corsList, ",")
		if slices.Contains(corsAllowOrigin, "") {
			return cfg, errors.New("invalid cors value")
		}
	}
	cfg.CorsAllowOrigin = corsAllowOrigin

	if err = config.CheckValidKeys(config.APISubSys, kvs, DefaultKVS, deprecatedKeys...); err != nil {
		return cfg, err
	}

	// Check environment variables parameters
	requestsMax, err := strconv.Atoi(env.Get(EnvAPIRequestsMax, kvs.GetWithDefault(apiRequestsMax, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	cfg.RequestsMax = requestsMax
	if requestsMax < 0 {
		return cfg, errors.New("invalid API max requests value")
	}

	clusterDeadline, err := time.ParseDuration(env.Get(EnvAPIClusterDeadline, kvs.GetWithDefault(apiClusterDeadline, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	cfg.ClusterDeadline = clusterDeadline

	remoteTransportDeadline, err := time.ParseDuration(env.Get(EnvAPIRemoteTransportDeadline, kvs.GetWithDefault(apiRemoteTransportDeadline, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	cfg.RemoteTransportDeadline = remoteTransportDeadline

	listQuorum := env.Get(EnvAPIListQuorum, kvs.GetWithDefault(apiListQuorum, DefaultKVS))
	switch listQuorum {
	case "strict", "optimal", "reduced", "disk", "auto":
	default:
		return cfg, fmt.Errorf("invalid value %v for list_quorum: will default to 'strict'", listQuorum)
	}
	cfg.ListQuorum = listQuorum

	replicationPriority := env.Get(EnvAPIReplicationPriority, kvs.GetWithDefault(apiReplicationPriority, DefaultKVS))
	switch replicationPriority {
	case "slow", "fast", "auto":
	default:
		return cfg, fmt.Errorf("invalid value %v for replication_priority", replicationPriority)
	}
	cfg.ReplicationPriority = replicationPriority
	replicationMaxWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationMaxWorkers, kvs.GetWithDefault(apiReplicationMaxWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if replicationMaxWorkers <= 0 || replicationMaxWorkers > 500 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Number of replication workers should be between 1 and 500")
	}
	cfg.ReplicationMaxWorkers = replicationMaxWorkers

	replicationMaxLWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationMaxLWorkers, kvs.GetWithDefault(apiReplicationMaxLWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if replicationMaxLWorkers <= 0 || replicationMaxLWorkers > 10 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Number of replication workers for transfers >=128MiB should be between 1 and 10 per node")
	}

	cfg.ReplicationMaxLWorkers = replicationMaxLWorkers

	transitionWorkers, err := strconv.Atoi(env.Get(EnvAPITransitionWorkers, kvs.GetWithDefault(apiTransitionWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	cfg.TransitionWorkers = transitionWorkers

	v := env.Get(EnvAPIDeleteCleanupInterval, kvs.Get(apiDeleteCleanupInterval))
	if v == "" {
		v = env.Get(EnvDeleteCleanupInterval, kvs.GetWithDefault(apiDeleteCleanupInterval, DefaultKVS))
	}

	deleteCleanupInterval, err := time.ParseDuration(v)
	if err != nil {
		return cfg, err
	}
	cfg.DeleteCleanupInterval = deleteCleanupInterval

	staleUploadsCleanupInterval, err := time.ParseDuration(env.Get(EnvAPIStaleUploadsCleanupInterval, kvs.GetWithDefault(apiStaleUploadsCleanupInterval, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	cfg.StaleUploadsCleanupInterval = staleUploadsCleanupInterval

	staleUploadsExpiry, err := time.ParseDuration(env.Get(EnvAPIStaleUploadsExpiry, kvs.GetWithDefault(apiStaleUploadsExpiry, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	cfg.StaleUploadsExpiry = staleUploadsExpiry

	cfg.SyncEvents = env.Get(EnvAPISyncEvents, kvs.Get(apiSyncEvents)) == config.EnableOn

	maxVerStr := env.Get(EnvAPIObjectMaxVersions, "")
	if maxVerStr == "" {
		maxVerStr = env.Get(EnvAPIObjectMaxVersionsLegacy, kvs.Get(apiObjectMaxVersions))
	}
	if maxVerStr != "" {
		maxVersions, err := strconv.ParseInt(maxVerStr, 10, 64)
		if err != nil {
			return cfg, err
		}
		if maxVersions <= 0 {
			return cfg, fmt.Errorf("invalid object max versions value: %v", maxVersions)
		}
		cfg.ObjectMaxVersions = maxVersions
	} else {
		cfg.ObjectMaxVersions = math.MaxInt64
	}

	return cfg, nil
}
