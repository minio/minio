/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package api

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// API sub-system constants
const (
	apiRequestsMax             = "requests_max"
	apiRequestsDeadline        = "requests_deadline"
	apiReadyDeadline           = "ready_deadline"
	apiCorsAllowOrigin         = "cors_allow_origin"
	apiRemoteTransportDeadline = "remote_transport_deadline"

	EnvAPIRequestsMax             = "MINIO_API_REQUESTS_MAX"
	EnvAPIRequestsDeadline        = "MINIO_API_REQUESTS_DEADLINE"
	EnvAPIReadyDeadline           = "MINIO_API_READY_DEADLINE"
	EnvAPICorsAllowOrigin         = "MINIO_API_CORS_ALLOW_ORIGIN"
	EnvAPIRemoteTransportDeadline = "MINIO_API_REMOTE_TRANSPORT_DEADLINE"
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
			Key:   apiReadyDeadline,
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
	}
)

// Config storage class configuration
type Config struct {
	RequestsMax             int           `json:"requests_max"`
	RequestsDeadline        time.Duration `json:"requests_deadline"`
	ReadyDeadline           time.Duration `json:"ready_deadline"`
	CorsAllowOrigin         []string      `json:"cors_allow_origin"`
	RemoteTransportDeadline time.Duration `json:"remote_transport_deadline"`
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

	readyDeadline, err := time.ParseDuration(env.Get(EnvAPIReadyDeadline, kvs.Get(apiReadyDeadline)))
	if err != nil {
		return cfg, err
	}

	corsAllowOrigin := strings.Split(env.Get(EnvAPICorsAllowOrigin, kvs.Get(apiCorsAllowOrigin)), ",")

	remoteTransportDeadline, err := time.ParseDuration(env.Get(EnvAPIRemoteTransportDeadline, kvs.Get(apiRemoteTransportDeadline)))
	if err != nil {
		return cfg, err
	}

	return Config{
		RequestsMax:             requestsMax,
		RequestsDeadline:        requestsDeadline,
		ReadyDeadline:           readyDeadline,
		CorsAllowOrigin:         corsAllowOrigin,
		RemoteTransportDeadline: remoteTransportDeadline,
	}, nil
}
