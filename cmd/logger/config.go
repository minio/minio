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

// NewConfig - initialize new logger config.
func NewConfig() Config {
	cfg := Config{
		// Console logging is on by default
		Console: Console{
			Enabled: true,
		},
		HTTP:  make(map[string]HTTP),
		Audit: make(map[string]HTTP),
	}

	// Create an example HTTP logger
	cfg.HTTP[defaultTarget] = HTTP{
		Endpoint: "https://username:password@example.com/api",
	}

	// Create an example Audit logger
	cfg.Audit[defaultTarget] = HTTP{
		Endpoint: "https://username:password@example.com/api/audit",
	}

	return cfg
}

// LookupConfig - lookup logger config, override with ENVs if set.
func LookupConfig(cfg Config) (Config, error) {
	if cfg.HTTP == nil {
		cfg.HTTP = make(map[string]HTTP)
	}
	if cfg.Audit == nil {
		cfg.Audit = make(map[string]HTTP)
	}
	envs := env.List(EnvLoggerHTTPEndpoint)
	for _, k := range envs {
		target := strings.TrimPrefix(k, EnvLoggerHTTPEndpoint+defaultTarget)
		if target == EnvLoggerHTTPEndpoint {
			target = defaultTarget
		}
		cfg.HTTP[target] = HTTP{
			Enabled:  true,
			Endpoint: env.Get(k, cfg.HTTP[target].Endpoint),
		}
	}
	aenvs := env.List(EnvAuditLoggerHTTPEndpoint)
	for _, k := range aenvs {
		target := strings.TrimPrefix(k, EnvAuditLoggerHTTPEndpoint+defaultTarget)
		if target == EnvAuditLoggerHTTPEndpoint {
			target = defaultTarget
		}
		cfg.Audit[target] = HTTP{
			Enabled:  true,
			Endpoint: env.Get(k, cfg.Audit[target].Endpoint),
		}
	}

	return cfg, nil
}
