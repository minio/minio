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

import (
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// API sub-system constants
const (
	apiRequestsMax                 = "requests_max"
	apiRequestsDeadline            = "requests_deadline"
	apiClusterDeadline             = "cluster_deadline"
	apiCorsAllowOrigin             = "cors_allow_origin"
	apiRemoteTransportDeadline     = "remote_transport_deadline"
	apiListQuorum                  = "list_quorum"
	apiReplicationWorkers          = "replication_workers"
	apiReplicationFailedWorkers    = "replication_failed_workers"
	apiTransitionWorkers           = "transition_workers"
	apiStaleUploadsCleanupInterval = "stale_uploads_cleanup_interval"
	apiStaleUploadsExpiry          = "stale_uploads_expiry"
	apiDeleteCleanupInterval       = "delete_cleanup_interval"
	apiDisableODirect              = "disable_odirect"
	apiGzipObjects                 = "gzip_objects"

	EnvAPIRequestsMax              = "MINIO_API_REQUESTS_MAX"
	EnvAPIRequestsDeadline         = "MINIO_API_REQUESTS_DEADLINE"
	EnvAPIClusterDeadline          = "MINIO_API_CLUSTER_DEADLINE"
	EnvAPICorsAllowOrigin          = "MINIO_API_CORS_ALLOW_ORIGIN"
	EnvAPIRemoteTransportDeadline  = "MINIO_API_REMOTE_TRANSPORT_DEADLINE"
	EnvAPIListQuorum               = "MINIO_API_LIST_QUORUM"
	EnvAPISecureCiphers            = "MINIO_API_SECURE_CIPHERS" // default "on"
	EnvAPIReplicationWorkers       = "MINIO_API_REPLICATION_WORKERS"
	EnvAPIReplicationFailedWorkers = "MINIO_API_REPLICATION_FAILED_WORKERS"
	EnvAPITransitionWorkers        = "MINIO_API_TRANSITION_WORKERS"

	EnvAPIStaleUploadsCleanupInterval = "MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL"
	EnvAPIStaleUploadsExpiry          = "MINIO_API_STALE_UPLOADS_EXPIRY"
	EnvAPIDeleteCleanupInterval       = "MINIO_API_DELETE_CLEANUP_INTERVAL"
	EnvDeleteCleanupInterval          = "MINIO_DELETE_CLEANUP_INTERVAL"
	EnvAPIDisableODirect              = "MINIO_API_DISABLE_ODIRECT"
	EnvAPIGzipObjects                 = "MINIO_API_GZIP_OBJECTS"
)

// Deprecated key and ENVs
const (
	apiReadyDeadline    = "ready_deadline"
	EnvAPIReadyDeadline = "MINIO_API_READY_DEADLINE"
)

// DefaultKVS - default storage class config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   apiRequestsMax,
			Value: "0",
		},
		config.KV{
			Key:   apiRequestsDeadline,
			Value: "10s",
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
			Key:   apiReplicationWorkers,
			Value: "250",
		},
		config.KV{
			Key:   apiReplicationFailedWorkers,
			Value: "8",
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
			Key:   apiDisableODirect,
			Value: "off",
		},
		config.KV{
			Key:   apiGzipObjects,
			Value: "off",
		},
	}
)

// Config storage class configuration
type Config struct {
	RequestsMax                 int           `json:"requests_max"`
	RequestsDeadline            time.Duration `json:"requests_deadline"`
	ClusterDeadline             time.Duration `json:"cluster_deadline"`
	CorsAllowOrigin             []string      `json:"cors_allow_origin"`
	RemoteTransportDeadline     time.Duration `json:"remote_transport_deadline"`
	ListQuorum                  string        `json:"list_quorum"`
	ReplicationWorkers          int           `json:"replication_workers"`
	ReplicationFailedWorkers    int           `json:"replication_failed_workers"`
	TransitionWorkers           int           `json:"transition_workers"`
	StaleUploadsCleanupInterval time.Duration `json:"stale_uploads_cleanup_interval"`
	StaleUploadsExpiry          time.Duration `json:"stale_uploads_expiry"`
	DeleteCleanupInterval       time.Duration `json:"delete_cleanup_interval"`
	DisableODirect              bool          `json:"disable_odirect"`
	GzipObjects                 bool          `json:"gzip_objects"`
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
	// remove this since we have removed this already.
	kvs.Delete(apiReadyDeadline)
	kvs.Delete("extend_list_cache_life")

	if err = config.CheckValidKeys(config.APISubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	// Check environment variables parameters
	requestsMax, err := strconv.Atoi(env.Get(EnvAPIRequestsMax, kvs.GetWithDefault(apiRequestsMax, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	if requestsMax < 0 {
		return cfg, errors.New("invalid API max requests value")
	}

	requestsDeadline, err := time.ParseDuration(env.Get(EnvAPIRequestsDeadline, kvs.GetWithDefault(apiRequestsDeadline, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	clusterDeadline, err := time.ParseDuration(env.Get(EnvAPIClusterDeadline, kvs.GetWithDefault(apiClusterDeadline, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	corsAllowOrigin := strings.Split(env.Get(EnvAPICorsAllowOrigin, kvs.Get(apiCorsAllowOrigin)), ",")

	remoteTransportDeadline, err := time.ParseDuration(env.Get(EnvAPIRemoteTransportDeadline, kvs.GetWithDefault(apiRemoteTransportDeadline, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	listQuorum := env.Get(EnvAPIListQuorum, kvs.GetWithDefault(apiListQuorum, DefaultKVS))
	switch listQuorum {
	case "strict", "optimal", "reduced", "disk":
	default:
		return cfg, errors.New("invalid value for list strict quorum")
	}

	replicationWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationWorkers, kvs.GetWithDefault(apiReplicationWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	if replicationWorkers <= 0 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Minimum number of replication workers should be 1")
	}

	replicationFailedWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationFailedWorkers, kvs.GetWithDefault(apiReplicationFailedWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	if replicationFailedWorkers <= 0 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Minimum number of replication failed workers should be 1")
	}

	transitionWorkers, err := strconv.Atoi(env.Get(EnvAPITransitionWorkers, kvs.GetWithDefault(apiTransitionWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if transitionWorkers < runtime.GOMAXPROCS(0)/2 {
		return cfg, config.ErrInvalidTransitionWorkersValue(nil)
	}

	v := env.Get(EnvAPIDeleteCleanupInterval, kvs.Get(apiDeleteCleanupInterval))
	if v == "" {
		v = env.Get(EnvDeleteCleanupInterval, kvs.GetWithDefault(apiDeleteCleanupInterval, DefaultKVS))
	}

	deleteCleanupInterval, err := time.ParseDuration(v)
	if err != nil {
		return cfg, err
	}

	staleUploadsCleanupInterval, err := time.ParseDuration(env.Get(EnvAPIStaleUploadsCleanupInterval, kvs.GetWithDefault(apiStaleUploadsCleanupInterval, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	staleUploadsExpiry, err := time.ParseDuration(env.Get(EnvAPIStaleUploadsExpiry, kvs.GetWithDefault(apiStaleUploadsExpiry, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	disableODirect := env.Get(EnvAPIDisableODirect, kvs.Get(apiDisableODirect)) == config.EnableOn

	gzipObjects := env.Get(EnvAPIGzipObjects, kvs.Get(apiGzipObjects)) == config.EnableOn

	return Config{
		RequestsMax:                 requestsMax,
		RequestsDeadline:            requestsDeadline,
		ClusterDeadline:             clusterDeadline,
		CorsAllowOrigin:             corsAllowOrigin,
		RemoteTransportDeadline:     remoteTransportDeadline,
		ListQuorum:                  listQuorum,
		ReplicationWorkers:          replicationWorkers,
		ReplicationFailedWorkers:    replicationFailedWorkers,
		TransitionWorkers:           transitionWorkers,
		StaleUploadsCleanupInterval: staleUploadsCleanupInterval,
		StaleUploadsExpiry:          staleUploadsExpiry,
		DeleteCleanupInterval:       deleteCleanupInterval,
		DisableODirect:              disableODirect,
		GzipObjects:                 gzipObjects,
	}, nil
}
