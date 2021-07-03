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

package logger

import (
	"strings"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Console logger target
type Console struct {
	Enabled bool `json:"enabled"`
}

// HTTP logger target
type HTTP struct {
	Enabled    bool   `json:"enabled"`
	Endpoint   string `json:"endpoint"`
	AuthToken  string `json:"authToken"`
	ClientCert string `json:"clientCert"`
	ClientKey  string `json:"clientKey"`
}

// Config console and http logger targets
type Config struct {
	Console Console         `json:"console"`
	HTTP    map[string]HTTP `json:"http"`
	Audit   map[string]HTTP `json:"audit"`
}

// HTTP endpoint logger
const (
	Endpoint   = "endpoint"
	AuthToken  = "auth_token"
	ClientCert = "client_cert"
	ClientKey  = "client_key"

	EnvLoggerWebhookEnable    = "MINIO_LOGGER_WEBHOOK_ENABLE"
	EnvLoggerWebhookEndpoint  = "MINIO_LOGGER_WEBHOOK_ENDPOINT"
	EnvLoggerWebhookAuthToken = "MINIO_LOGGER_WEBHOOK_AUTH_TOKEN"

	EnvAuditWebhookEnable     = "MINIO_AUDIT_WEBHOOK_ENABLE"
	EnvAuditWebhookEndpoint   = "MINIO_AUDIT_WEBHOOK_ENDPOINT"
	EnvAuditWebhookAuthToken  = "MINIO_AUDIT_WEBHOOK_AUTH_TOKEN"
	EnvAuditWebhookClientCert = "MINIO_AUDIT_WEBHOOK_CLIENT_CERT"
	EnvAuditWebhookClientKey  = "MINIO_AUDIT_WEBHOOK_CLIENT_KEY"
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
		config.KV{
			Key:   ClientCert,
			Value: "",
		},
		config.KV{
			Key:   ClientKey,
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
		clientCertEnv := EnvAuditWebhookClientCert
		if target != config.Default {
			clientCertEnv = EnvAuditWebhookClientCert + config.Default + target
		}
		clientKeyEnv := EnvAuditWebhookClientKey
		if target != config.Default {
			clientKeyEnv = EnvAuditWebhookClientKey + config.Default + target
		}
		err = config.EnsureCertAndKey(env.Get(clientCertEnv, ""), env.Get(clientKeyEnv, ""))
		if err != nil {
			return cfg, err
		}
		cfg.Audit[target] = HTTP{
			Enabled:    true,
			Endpoint:   env.Get(endpointEnv, ""),
			AuthToken:  env.Get(authTokenEnv, ""),
			ClientCert: env.Get(clientCertEnv, ""),
			ClientKey:  env.Get(clientKeyEnv, ""),
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
		err = config.EnsureCertAndKey(kv.Get(ClientCert), kv.Get(ClientKey))
		if err != nil {
			return cfg, err
		}
		cfg.Audit[starget] = HTTP{
			Enabled:    true,
			Endpoint:   kv.Get(Endpoint),
			AuthToken:  kv.Get(AuthToken),
			ClientCert: kv.Get(ClientCert),
			ClientKey:  kv.Get(ClientKey),
		}
	}

	return cfg, nil
}
