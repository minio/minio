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
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// API sub-system constants
const (
	apiRequestsMax              = "requests_max"
	apiRequestsDeadline         = "requests_deadline"
	apiClusterDeadline          = "cluster_deadline"
	apiCorsAllowOrigin          = "cors_allow_origin"
	apiRemoteTransportDeadline  = "remote_transport_deadline"
	apiListQuorum               = "list_quorum"
	apiExtendListCacheLife      = "extend_list_cache_life"
	apiReplicationWorkers       = "replication_workers"
	apiReplicationFailedWorkers = "replication_failed_workers"

	EnvAPIRequestsMax              = "MINIO_API_REQUESTS_MAX"
	EnvAPIRequestsDeadline         = "MINIO_API_REQUESTS_DEADLINE"
	EnvAPIClusterDeadline          = "MINIO_API_CLUSTER_DEADLINE"
	EnvAPICorsAllowOrigin          = "MINIO_API_CORS_ALLOW_ORIGIN"
	EnvAPIRemoteTransportDeadline  = "MINIO_API_REMOTE_TRANSPORT_DEADLINE"
	EnvAPIListQuorum               = "MINIO_API_LIST_QUORUM"
	EnvAPIExtendListCacheLife      = "MINIO_API_EXTEND_LIST_CACHE_LIFE"
	EnvAPISecureCiphers            = "MINIO_API_SECURE_CIPHERS"
	EnvAPIReplicationWorkers       = "MINIO_API_REPLICATION_WORKERS"
	EnvAPIReplicationFailedWorkers = "MINIO_API_REPLICATION_FAILED_WORKERS"
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
			Value: "optimal",
		},
		config.KV{
			Key:   apiExtendListCacheLife,
			Value: "0s",
		},
		config.KV{
			Key:   apiReplicationWorkers,
			Value: "250",
		},
		config.KV{
			Key:   apiReplicationFailedWorkers,
			Value: "8",
		},
	}
)

// Config storage class configuration
type Config struct {
	RequestsMax              int           `json:"requests_max"`
	RequestsDeadline         time.Duration `json:"requests_deadline"`
	ClusterDeadline          time.Duration `json:"cluster_deadline"`
	CorsAllowOrigin          []string      `json:"cors_allow_origin"`
	RemoteTransportDeadline  time.Duration `json:"remote_transport_deadline"`
	ListQuorum               string        `json:"list_quorum"`
	ExtendListLife           time.Duration `json:"extend_list_cache_life"`
	ReplicationWorkers       int           `json:"replication_workers"`
	ReplicationFailedWorkers int           `json:"replication_failed_workers"`
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

// GetListQuorum interprets list quorum values and returns appropriate
// acceptable quorum expected for list operations
func (sCfg Config) GetListQuorum() int {
	switch sCfg.ListQuorum {
	case "reduced":
		return 2
	case "disk":
		// smallest possible value, generally meant for testing.
		return 1
	case "strict":
		return -1
	}
	// Defaults to 3 drives per set, defaults to "optimal" value
	return 3
}

// LookupConfig - lookup api config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	// remove this since we have removed this already.
	kvs.Delete(apiReadyDeadline)

	if err = config.CheckValidKeys(config.APISubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	// Check environment variables parameters
	requestsMax, err := strconv.Atoi(env.Get(EnvAPIRequestsMax, kvs.Get(apiRequestsMax)))
	if err != nil {
		return cfg, err
	}

	if requestsMax < 0 {
		return cfg, errors.New("invalid API max requests value")
	}

	requestsDeadline, err := time.ParseDuration(env.Get(EnvAPIRequestsDeadline, kvs.Get(apiRequestsDeadline)))
	if err != nil {
		return cfg, err
	}

	clusterDeadline, err := time.ParseDuration(env.Get(EnvAPIClusterDeadline, kvs.Get(apiClusterDeadline)))
	if err != nil {
		return cfg, err
	}

	corsAllowOrigin := strings.Split(env.Get(EnvAPICorsAllowOrigin, kvs.Get(apiCorsAllowOrigin)), ",")

	remoteTransportDeadline, err := time.ParseDuration(env.Get(EnvAPIRemoteTransportDeadline, kvs.Get(apiRemoteTransportDeadline)))
	if err != nil {
		return cfg, err
	}

	listQuorum := env.Get(EnvAPIListQuorum, kvs.Get(apiListQuorum))
	switch listQuorum {
	case "strict", "optimal", "reduced", "disk":
	default:
		return cfg, errors.New("invalid value for list strict quorum")
	}

	listLife, err := time.ParseDuration(env.Get(EnvAPIExtendListCacheLife, kvs.Get(apiExtendListCacheLife)))
	if err != nil {
		return cfg, err
	}

	replicationWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationWorkers, kvs.Get(apiReplicationWorkers)))
	if err != nil {
		return cfg, err
	}

	if replicationWorkers <= 0 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Minimum number of replication workers should be 1")
	}

	replicationFailedWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationFailedWorkers, kvs.Get(apiReplicationFailedWorkers)))
	if err != nil {
		return cfg, err
	}

	if replicationFailedWorkers <= 0 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Minimum number of replication failed workers should be 1")
	}

	return Config{
		RequestsMax:              requestsMax,
		RequestsDeadline:         requestsDeadline,
		ClusterDeadline:          clusterDeadline,
		CorsAllowOrigin:          corsAllowOrigin,
		RemoteTransportDeadline:  remoteTransportDeadline,
		ListQuorum:               listQuorum,
		ExtendListLife:           listLife,
		ReplicationWorkers:       replicationWorkers,
		ReplicationFailedWorkers: replicationFailedWorkers,
	}, nil
}
