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

package logger

import (
	"strings"

	"github.com/minio/minio/pkg/env"
)

// Console logger target
type Console struct {
	Enabled bool `json:"enabled"`
}

// HTTP logger target
type HTTP struct {
	Enabled  bool   `json:"enabled"`
	Endpoint string `json:"endpoint"`
}

// Config console and http logger targets
type Config struct {
	Console Console         `json:"console"`
	HTTP    map[string]HTTP `json:"http"`
	Audit   map[string]HTTP `json:"audit"`
}

// HTTP endpoint logger
const (
	EnvLoggerHTTPEndpoint      = "MINIO_LOGGER_HTTP_ENDPOINT"
	EnvAuditLoggerHTTPEndpoint = "MINIO_AUDIT_LOGGER_HTTP_ENDPOINT"
)

// Default target name when no targets are found
const (
	defaultTarget = "_"
)

// LookupConfig - lookup logger config, override with ENVs if set.
func LookupConfig(cfg Config) (Config, error) {
	envs := env.List(EnvLoggerHTTPEndpoint)
	for _, e := range envs {
		target := strings.TrimPrefix(e, EnvLoggerHTTPEndpoint)
		if target == "" {
			target = defaultTarget
		}
		_, ok := cfg.HTTP[target]
		if ok {
			cfg.HTTP[target] = HTTP{
				Enabled:  true,
				Endpoint: env.Get(e, cfg.HTTP[target].Endpoint),
			}
		}
	}
	aenvs := env.List(EnvAuditLoggerHTTPEndpoint)
	for _, e := range aenvs {
		target := strings.TrimPrefix(e, EnvAuditLoggerHTTPEndpoint)
		if target == "" {
			target = defaultTarget
		}
		_, ok := cfg.Audit[target]
		if ok {
			cfg.Audit[target] = HTTP{
				Enabled:  true,
				Endpoint: env.Get(e, cfg.Audit[target].Endpoint),
			}
		}
	}

	return cfg, nil
}
