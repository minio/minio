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

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Console logger target
type Console struct {
	Enabled bool `json:"enabled"`
}

// HTTP logger target
type HTTP struct {
	Enabled   bool   `json:"enabled"`
	Endpoint  string `json:"endpoint"`
	AuthToken string `json:"authToken"`
}

// Config console and http logger targets
type Config struct {
	Console Console         `json:"console"`
	HTTP    map[string]HTTP `json:"http"`
	Audit   map[string]HTTP `json:"audit"`
}

// HTTP endpoint logger
const (
	Endpoint  = "endpoint"
	AuthToken = "auth_token"

	EnvLoggerHTTPEndpoint  = "MINIO_LOGGER_HTTP_ENDPOINT"
	EnvLoggerHTTPAuthToken = "MINIO_LOGGER_HTTP_AUTH_TOKEN"

	EnvLoggerHTTPAuditEndpoint  = "MINIO_LOGGER_HTTP_AUDIT_ENDPOINT"
	EnvLoggerHTTPAuditAuthToken = "MINIO_LOGGER_HTTP_AUDIT_AUTH_TOKEN"
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
	cfg.HTTP[config.Default] = HTTP{
		Endpoint: "https://username:password@example.com/api",
	}

	// Create an example Audit logger
	cfg.Audit[config.Default] = HTTP{
		Endpoint: "https://username:password@example.com/api/audit",
	}

	return cfg
}

// SetLoggerHTTPAudit - helper for migrating older config to newer KV format.
func SetLoggerHTTPAudit(scfg config.Config, k string, args HTTP) {
	scfg[config.LoggerHTTPAuditSubSys][k] = config.KVS{
		config.State: func() string {
			if args.Enabled {
				return config.StateOn
			}
			return config.StateOff
		}(),
		Endpoint:  args.Endpoint,
		AuthToken: args.AuthToken,
	}
}

// SetLoggerHTTP helper for migrating older config to newer KV format.
func SetLoggerHTTP(scfg config.Config, k string, args HTTP) {
	scfg[config.LoggerHTTPSubSys][k] = config.KVS{
		config.State: func() string {
			if args.Enabled {
				return config.StateOn
			}
			return config.StateOff
		}(),
		Endpoint:  args.Endpoint,
		AuthToken: args.AuthToken,
	}
}

// LookupConfig - lookup logger config, override with ENVs if set.
func LookupConfig(scfg config.Config) (Config, error) {
	cfg := NewConfig()
	envs := env.List(EnvLoggerHTTPEndpoint)
	var loggerTargets []string
	for _, k := range envs {
		target := strings.TrimPrefix(k, EnvLoggerHTTPEndpoint+config.Default)
		if target == EnvLoggerHTTPEndpoint {
			target = config.Default
		}
		loggerTargets = append(loggerTargets, target)
	}

	var loggerAuditTargets []string
	for _, k := range envs {
		target := strings.TrimPrefix(k, EnvLoggerHTTPAuditEndpoint+config.Default)
		if target == EnvLoggerHTTPAuditEndpoint {
			target = config.Default
		}
		loggerAuditTargets = append(loggerAuditTargets, target)
	}

	for starget, kv := range scfg[config.LoggerHTTPSubSys] {
		if kv.Get(config.State) != config.StateOn {
			continue
		}
		endpointEnv := EnvLoggerHTTPEndpoint
		if starget != config.Default {
			endpointEnv = EnvLoggerHTTPEndpoint + config.Default + starget
		}
		authTokenEnv := EnvLoggerHTTPAuthToken
		if starget != config.Default {
			authTokenEnv = EnvLoggerHTTPAuthToken + config.Default + starget
		}
		cfg.HTTP[starget] = HTTP{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, kv.Get(Endpoint)),
			AuthToken: env.Get(authTokenEnv, kv.Get(AuthToken)),
		}
	}

	for starget, kv := range scfg[config.LoggerHTTPAuditSubSys] {
		if kv.Get(config.State) != config.StateOn {
			continue
		}
		endpointEnv := EnvLoggerHTTPAuditEndpoint
		if starget != config.Default {
			endpointEnv = EnvLoggerHTTPAuditEndpoint + config.Default + starget
		}
		authTokenEnv := EnvLoggerHTTPAuditAuthToken
		if starget != config.Default {
			authTokenEnv = EnvLoggerHTTPAuditAuthToken + config.Default + starget
		}
		cfg.HTTP[starget] = HTTP{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, kv.Get(Endpoint)),
			AuthToken: env.Get(authTokenEnv, kv.Get(AuthToken)),
		}
	}

	for _, target := range loggerTargets {
		endpointEnv := EnvLoggerHTTPEndpoint
		if target != config.Default {
			endpointEnv = EnvLoggerHTTPEndpoint + config.Default + target
		}
		authTokenEnv := EnvLoggerHTTPAuthToken
		if target != config.Default {
			authTokenEnv = EnvLoggerHTTPAuthToken + config.Default + target
		}
		cfg.HTTP[target] = HTTP{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, ""),
			AuthToken: env.Get(authTokenEnv, ""),
		}
	}

	for _, target := range loggerAuditTargets {
		endpointEnv := EnvLoggerHTTPAuditEndpoint
		if target != config.Default {
			endpointEnv = EnvLoggerHTTPAuditEndpoint + config.Default + target
		}
		authTokenEnv := EnvLoggerHTTPAuditAuthToken
		if target != config.Default {
			authTokenEnv = EnvLoggerHTTPAuditAuthToken + config.Default + target
		}
		cfg.HTTP[target] = HTTP{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, ""),
			AuthToken: env.Get(authTokenEnv, ""),
		}
	}

	return cfg, nil
}
