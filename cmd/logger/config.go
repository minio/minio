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

	EnvLoggerWebhookEnable    = "MINIO_LOGGER_WEBHOOK_ENABLE"
	EnvLoggerWebhookEndpoint  = "MINIO_LOGGER_WEBHOOK_ENDPOINT"
	EnvLoggerWebhookAuthToken = "MINIO_LOGGER_WEBHOOK_AUTH_TOKEN"

	EnvAuditWebhookEnable    = "MINIO_AUDIT_WEBHOOK_ENABLE"
	EnvAuditWebhookEndpoint  = "MINIO_AUDIT_WEBHOOK_ENDPOINT"
	EnvAuditWebhookAuthToken = "MINIO_AUDIT_WEBHOOK_AUTH_TOKEN"
)

// Default KVS for loggerHTTP and loggerAuditHTTP
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   Endpoint,
			Value: "",
		},
		config.KV{
			Key:   AuthToken,
			Value: "",
		},
	}
	DefaultAuditKVS = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   Endpoint,
			Value: "",
		},
		config.KV{
			Key:   AuthToken,
			Value: "",
		},
	}
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

func lookupLegacyConfig() (Config, error) {
	cfg := NewConfig()

	var loggerTargets []string
	envs := env.List(legacyEnvLoggerHTTPEndpoint)
	for _, k := range envs {
		target := strings.TrimPrefix(k, legacyEnvLoggerHTTPEndpoint+config.Default)
		if target == legacyEnvLoggerHTTPEndpoint {
			target = config.Default
		}
		loggerTargets = append(loggerTargets, target)
	}

	// Load HTTP logger from the environment if found
	for _, target := range loggerTargets {
		endpointEnv := legacyEnvLoggerHTTPEndpoint
		if target != config.Default {
			endpointEnv = legacyEnvLoggerHTTPEndpoint + config.Default + target
		}
		endpoint := env.Get(endpointEnv, "")
		if endpoint == "" {
			continue
		}
		cfg.HTTP[target] = HTTP{
			Enabled:  true,
			Endpoint: endpoint,
		}
	}

	// List legacy audit ENVs if any.
	var loggerAuditTargets []string
	envs = env.List(legacyEnvAuditLoggerHTTPEndpoint)
	for _, k := range envs {
		target := strings.TrimPrefix(k, legacyEnvAuditLoggerHTTPEndpoint+config.Default)
		if target == legacyEnvAuditLoggerHTTPEndpoint {
			target = config.Default
		}
		loggerAuditTargets = append(loggerAuditTargets, target)
	}

	for _, target := range loggerAuditTargets {
		endpointEnv := legacyEnvAuditLoggerHTTPEndpoint
		if target != config.Default {
			endpointEnv = legacyEnvAuditLoggerHTTPEndpoint + config.Default + target
		}
		endpoint := env.Get(endpointEnv, "")
		if endpoint == "" {
			continue
		}
		cfg.Audit[target] = HTTP{
			Enabled:  true,
			Endpoint: endpoint,
		}
	}

	return cfg, nil

}

// LookupConfig - lookup logger config, override with ENVs if set.
func LookupConfig(scfg config.Config) (Config, error) {
	// Lookup for legacy environment variables first
	cfg, err := lookupLegacyConfig()
	if err != nil {
		return cfg, err
	}

	envs := env.List(EnvLoggerWebhookEndpoint)
	var loggerTargets []string
	for _, k := range envs {
		target := strings.TrimPrefix(k, EnvLoggerWebhookEndpoint+config.Default)
		if target == EnvLoggerWebhookEndpoint {
			target = config.Default
		}
		loggerTargets = append(loggerTargets, target)
	}

	var loggerAuditTargets []string
	envs = env.List(EnvAuditWebhookEndpoint)
	for _, k := range envs {
		target := strings.TrimPrefix(k, EnvAuditWebhookEndpoint+config.Default)
		if target == EnvAuditWebhookEndpoint {
			target = config.Default
		}
		loggerAuditTargets = append(loggerAuditTargets, target)
	}

	// Load HTTP logger from the environment if found
	for _, target := range loggerTargets {
		if v, ok := cfg.HTTP[target]; ok && v.Enabled {
			// This target is already enabled using the
			// legacy environment variables, ignore.
			continue
		}
		enableEnv := EnvLoggerWebhookEnable
		if target != config.Default {
			enableEnv = EnvLoggerWebhookEnable + config.Default + target
		}
		enable, err := config.ParseBool(env.Get(enableEnv, ""))
		if err != nil || !enable {
			continue
		}
		endpointEnv := EnvLoggerWebhookEndpoint
		if target != config.Default {
			endpointEnv = EnvLoggerWebhookEndpoint + config.Default + target
		}
		authTokenEnv := EnvLoggerWebhookAuthToken
		if target != config.Default {
			authTokenEnv = EnvLoggerWebhookAuthToken + config.Default + target
		}
		cfg.HTTP[target] = HTTP{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, ""),
			AuthToken: env.Get(authTokenEnv, ""),
		}
	}

	for _, target := range loggerAuditTargets {
		if v, ok := cfg.Audit[target]; ok && v.Enabled {
			// This target is already enabled using the
			// legacy environment variables, ignore.
			continue
		}
		enableEnv := EnvAuditWebhookEnable
		if target != config.Default {
			enableEnv = EnvAuditWebhookEnable + config.Default + target
		}
		enable, err := config.ParseBool(env.Get(enableEnv, ""))
		if err != nil || !enable {
			continue
		}
		endpointEnv := EnvAuditWebhookEndpoint
		if target != config.Default {
			endpointEnv = EnvAuditWebhookEndpoint + config.Default + target
		}
		authTokenEnv := EnvAuditWebhookAuthToken
		if target != config.Default {
			authTokenEnv = EnvAuditWebhookAuthToken + config.Default + target
		}
		cfg.Audit[target] = HTTP{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, ""),
			AuthToken: env.Get(authTokenEnv, ""),
		}
	}

	for starget, kv := range scfg[config.LoggerWebhookSubSys] {
		if l, ok := cfg.HTTP[starget]; ok && l.Enabled {
			// Ignore this HTTP logger config since there is
			// a target with the same name loaded and enabled
			// from the environment.
			continue
		}
		subSysTarget := config.LoggerWebhookSubSys
		if starget != config.Default {
			subSysTarget = config.LoggerWebhookSubSys + config.SubSystemSeparator + starget
		}
		if err := config.CheckValidKeys(subSysTarget, kv, DefaultKVS); err != nil {
			return cfg, err
		}
		enabled, err := config.ParseBool(kv.Get(config.Enable))
		if err != nil {
			return cfg, err
		}
		if !enabled {
			continue
		}
		cfg.HTTP[starget] = HTTP{
			Enabled:   true,
			Endpoint:  kv.Get(Endpoint),
			AuthToken: kv.Get(AuthToken),
		}
	}

	for starget, kv := range scfg[config.AuditWebhookSubSys] {
		if l, ok := cfg.Audit[starget]; ok && l.Enabled {
			// Ignore this audit config since another target
			// with the same name is already loaded and enabled
			// in the shell environment.
			continue
		}
		subSysTarget := config.AuditWebhookSubSys
		if starget != config.Default {
			subSysTarget = config.AuditWebhookSubSys + config.SubSystemSeparator + starget
		}
		if err := config.CheckValidKeys(subSysTarget, kv, DefaultAuditKVS); err != nil {
			return cfg, err
		}
		enabled, err := config.ParseBool(kv.Get(config.Enable))
		if err != nil {
			return cfg, err
		}
		if !enabled {
			continue
		}
		cfg.Audit[starget] = HTTP{
			Enabled:   true,
			Endpoint:  kv.Get(Endpoint),
			AuthToken: kv.Get(AuthToken),
		}
	}

	return cfg, nil
}
