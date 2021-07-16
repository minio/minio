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
	"crypto/tls"
	"strconv"
	"strings"

	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"

	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
)

// Console logger target
type Console struct {
	Enabled bool `json:"enabled"`
}

// Audit/Logger constants
const (
	Endpoint   = "endpoint"
	AuthToken  = "auth_token"
	ClientCert = "client_cert"
	ClientKey  = "client_key"

	KafkaBrokers       = "brokers"
	KafkaTopic         = "topic"
	KafkaTLS           = "tls"
	KafkaTLSSkipVerify = "tls_skip_verify"
	KafkaTLSClientAuth = "tls_client_auth"
	KafkaSASL          = "sasl"
	KafkaSASLUsername  = "sasl_username"
	KafkaSASLPassword  = "sasl_password"
	KafkaSASLMechanism = "sasl_mechanism"
	KafkaClientTLSCert = "client_tls_cert"
	KafkaClientTLSKey  = "client_tls_key"
	KafkaVersion       = "version"

	EnvLoggerWebhookEnable    = "MINIO_LOGGER_WEBHOOK_ENABLE"
	EnvLoggerWebhookEndpoint  = "MINIO_LOGGER_WEBHOOK_ENDPOINT"
	EnvLoggerWebhookAuthToken = "MINIO_LOGGER_WEBHOOK_AUTH_TOKEN"

	EnvAuditWebhookEnable     = "MINIO_AUDIT_WEBHOOK_ENABLE"
	EnvAuditWebhookEndpoint   = "MINIO_AUDIT_WEBHOOK_ENDPOINT"
	EnvAuditWebhookAuthToken  = "MINIO_AUDIT_WEBHOOK_AUTH_TOKEN"
	EnvAuditWebhookClientCert = "MINIO_AUDIT_WEBHOOK_CLIENT_CERT"
	EnvAuditWebhookClientKey  = "MINIO_AUDIT_WEBHOOK_CLIENT_KEY"

	EnvKafkaEnable        = "MINIO_AUDIT_KAFKA_ENABLE"
	EnvKafkaBrokers       = "MINIO_AUDIT_KAFKA_BROKERS"
	EnvKafkaTopic         = "MINIO_AUDIT_KAFKA_TOPIC"
	EnvKafkaTLS           = "MINIO_AUDIT_KAFKA_TLS"
	EnvKafkaTLSSkipVerify = "MINIO_AUDIT_KAFKA_TLS_SKIP_VERIFY"
	EnvKafkaTLSClientAuth = "MINIO_AUDIT_KAFKA_TLS_CLIENT_AUTH"
	EnvKafkaSASLEnable    = "MINIO_AUDIT_KAFKA_SASL"
	EnvKafkaSASLUsername  = "MINIO_AUDIT_KAFKA_SASL_USERNAME"
	EnvKafkaSASLPassword  = "MINIO_AUDIT_KAFKA_SASL_PASSWORD"
	EnvKafkaSASLMechanism = "MINIO_AUDIT_KAFKA_SASL_MECHANISM"
	EnvKafkaClientTLSCert = "MINIO_AUDIT_KAFKA_CLIENT_TLS_CERT"
	EnvKafkaClientTLSKey  = "MINIO_AUDIT_KAFKA_CLIENT_TLS_KEY"
	EnvKafkaVersion       = "MINIO_AUDIT_KAFKA_VERSION"
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

	DefaultAuditWebhookKVS = config.KVS{
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

	DefaultAuditKafkaKVS = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   KafkaTopic,
			Value: "",
		},
		config.KV{
			Key:   KafkaBrokers,
			Value: "",
		},
		config.KV{
			Key:   KafkaSASLUsername,
			Value: "",
		},
		config.KV{
			Key:   KafkaSASLPassword,
			Value: "",
		},
		config.KV{
			Key:   KafkaSASLMechanism,
			Value: "plain",
		},
		config.KV{
			Key:   KafkaClientTLSCert,
			Value: "",
		},
		config.KV{
			Key:   KafkaClientTLSKey,
			Value: "",
		},
		config.KV{
			Key:   KafkaTLSClientAuth,
			Value: "0",
		},
		config.KV{
			Key:   KafkaSASL,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   KafkaTLS,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   KafkaTLSSkipVerify,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   KafkaVersion,
			Value: "",
		},
	}
)

// Config console and http logger targets
type Config struct {
	Console      Console                 `json:"console"`
	HTTP         map[string]http.Config  `json:"http"`
	AuditWebhook map[string]http.Config  `json:"audit"`
	AuditKafka   map[string]kafka.Config `json:"audit_kafka"`
}

// NewConfig - initialize new logger config.
func NewConfig() Config {
	cfg := Config{
		// Console logging is on by default
		Console: Console{
			Enabled: true,
		},
		HTTP:         make(map[string]http.Config),
		AuditWebhook: make(map[string]http.Config),
		AuditKafka:   make(map[string]kafka.Config),
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
		cfg.HTTP[target] = http.Config{
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
		cfg.AuditWebhook[target] = http.Config{
			Enabled:  true,
			Endpoint: endpoint,
		}
	}

	return cfg, nil

}

// GetAuditKafka - returns a map of registered notification 'kafka' targets
func GetAuditKafka(kafkaKVS map[string]config.KVS) (map[string]kafka.Config, error) {
	kafkaTargets := make(map[string]kafka.Config)
	for k, kv := range config.Merge(kafkaKVS, EnvKafkaEnable, DefaultAuditKafkaKVS) {
		enableEnv := EnvKafkaEnable
		if k != config.Default {
			enableEnv = enableEnv + config.Default + k
		}
		enabled, err := config.ParseBool(env.Get(enableEnv, kv.Get(config.Enable)))
		if err != nil {
			return nil, err
		}
		if !enabled {
			continue
		}
		var brokers []xnet.Host
		brokersEnv := EnvKafkaBrokers
		if k != config.Default {
			brokersEnv = brokersEnv + config.Default + k
		}
		kafkaBrokers := env.Get(brokersEnv, kv.Get(KafkaBrokers))
		if len(kafkaBrokers) == 0 {
			return nil, config.Errorf("kafka 'brokers' cannot be empty")
		}
		for _, s := range strings.Split(kafkaBrokers, config.ValueSeparator) {
			var host *xnet.Host
			host, err = xnet.ParseHost(s)
			if err != nil {
				break
			}
			brokers = append(brokers, *host)
		}
		if err != nil {
			return nil, err
		}

		clientAuthEnv := EnvKafkaTLSClientAuth
		if k != config.Default {
			clientAuthEnv = clientAuthEnv + config.Default + k
		}
		clientAuth, err := strconv.Atoi(env.Get(clientAuthEnv, kv.Get(KafkaTLSClientAuth)))
		if err != nil {
			return nil, err
		}

		topicEnv := EnvKafkaTopic
		if k != config.Default {
			topicEnv = topicEnv + config.Default + k
		}

		versionEnv := EnvKafkaVersion
		if k != config.Default {
			versionEnv = versionEnv + config.Default + k
		}

		kafkaArgs := kafka.Config{
			Enabled: enabled,
			Brokers: brokers,
			Topic:   env.Get(topicEnv, kv.Get(KafkaTopic)),
			Version: env.Get(versionEnv, kv.Get(KafkaVersion)),
		}

		tlsEnableEnv := EnvKafkaTLS
		if k != config.Default {
			tlsEnableEnv = tlsEnableEnv + config.Default + k
		}
		tlsSkipVerifyEnv := EnvKafkaTLSSkipVerify
		if k != config.Default {
			tlsSkipVerifyEnv = tlsSkipVerifyEnv + config.Default + k
		}

		tlsClientTLSCertEnv := EnvKafkaClientTLSCert
		if k != config.Default {
			tlsClientTLSCertEnv = tlsClientTLSCertEnv + config.Default + k
		}

		tlsClientTLSKeyEnv := EnvKafkaClientTLSKey
		if k != config.Default {
			tlsClientTLSKeyEnv = tlsClientTLSKeyEnv + config.Default + k
		}

		kafkaArgs.TLS.Enable = env.Get(tlsEnableEnv, kv.Get(KafkaTLS)) == config.EnableOn
		kafkaArgs.TLS.SkipVerify = env.Get(tlsSkipVerifyEnv, kv.Get(KafkaTLSSkipVerify)) == config.EnableOn
		kafkaArgs.TLS.ClientAuth = tls.ClientAuthType(clientAuth)

		kafkaArgs.TLS.ClientTLSCert = env.Get(tlsClientTLSCertEnv, kv.Get(KafkaClientTLSCert))
		kafkaArgs.TLS.ClientTLSKey = env.Get(tlsClientTLSKeyEnv, kv.Get(KafkaClientTLSKey))

		saslEnableEnv := EnvKafkaSASLEnable
		if k != config.Default {
			saslEnableEnv = saslEnableEnv + config.Default + k
		}
		saslUsernameEnv := EnvKafkaSASLUsername
		if k != config.Default {
			saslUsernameEnv = saslUsernameEnv + config.Default + k
		}
		saslPasswordEnv := EnvKafkaSASLPassword
		if k != config.Default {
			saslPasswordEnv = saslPasswordEnv + config.Default + k
		}
		saslMechanismEnv := EnvKafkaSASLMechanism
		if k != config.Default {
			saslMechanismEnv = saslMechanismEnv + config.Default + k
		}
		kafkaArgs.SASL.Enable = env.Get(saslEnableEnv, kv.Get(KafkaSASL)) == config.EnableOn
		kafkaArgs.SASL.User = env.Get(saslUsernameEnv, kv.Get(KafkaSASLUsername))
		kafkaArgs.SASL.Password = env.Get(saslPasswordEnv, kv.Get(KafkaSASLPassword))
		kafkaArgs.SASL.Mechanism = env.Get(saslMechanismEnv, kv.Get(KafkaSASLMechanism))

		kafkaTargets[k] = kafkaArgs
	}

	return kafkaTargets, nil
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
		cfg.HTTP[target] = http.Config{
			Enabled:   true,
			Endpoint:  env.Get(endpointEnv, ""),
			AuthToken: env.Get(authTokenEnv, ""),
		}
	}

	for _, target := range loggerAuditTargets {
		if v, ok := cfg.AuditWebhook[target]; ok && v.Enabled {
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
		cfg.AuditWebhook[target] = http.Config{
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
		cfg.HTTP[starget] = http.Config{
			Enabled:   true,
			Endpoint:  kv.Get(Endpoint),
			AuthToken: kv.Get(AuthToken),
		}
	}

	for starget, kv := range scfg[config.AuditWebhookSubSys] {
		if l, ok := cfg.AuditWebhook[starget]; ok && l.Enabled {
			// Ignore this audit config since another target
			// with the same name is already loaded and enabled
			// in the shell environment.
			continue
		}
		subSysTarget := config.AuditWebhookSubSys
		if starget != config.Default {
			subSysTarget = config.AuditWebhookSubSys + config.SubSystemSeparator + starget
		}
		if err := config.CheckValidKeys(subSysTarget, kv, DefaultAuditWebhookKVS); err != nil {
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
		cfg.AuditWebhook[starget] = http.Config{
			Enabled:    true,
			Endpoint:   kv.Get(Endpoint),
			AuthToken:  kv.Get(AuthToken),
			ClientCert: kv.Get(ClientCert),
			ClientKey:  kv.Get(ClientKey),
		}
	}

	cfg.AuditKafka, err = GetAuditKafka(scfg[config.AuditKafkaSubSys])
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}
