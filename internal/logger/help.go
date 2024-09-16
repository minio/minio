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
	"github.com/minio/minio/internal/config"
)

// Help template for logger http and audit
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Endpoint,
			Description: `HTTP(s) endpoint e.g. "http://localhost:8080/minio/logs/server"`,
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         AuthToken,
			Description: `opaque string or JWT authorization token`,
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         ClientCert,
			Description: "mTLS certificate for webhook authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         ClientKey,
			Description: "mTLS certificate key for webhook authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         BatchSize,
			Description: "Number of events per HTTP send to webhook target",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         QueueSize,
			Description: "configure channel queue size for webhook targets",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         QueueDir,
			Description: `staging dir for undelivered logger messages e.g. '/home/logger-events'`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         Proxy,
			Description: "proxy url endpoint e.g. http(s)://proxy",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         MaxRetry,
			Description: `maximum retry count before we start dropping logged event(s)`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         RetryInterval,
			Description: `sleep between each retries, allowed maximum value is '1m' e.g. '10s'`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         httpTimeout,
			Description: `defines the maximum duration for each http request`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpWebhook = config.HelpKVS{
		config.HelpKV{
			Key:         Endpoint,
			Description: `HTTP(s) endpoint e.g. "http://localhost:8080/minio/logs/audit"`,
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         AuthToken,
			Description: `opaque string or JWT authorization token`,
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         ClientCert,
			Description: "mTLS certificate for webhook authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         ClientKey,
			Description: "mTLS certificate key for webhook authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         BatchSize,
			Description: "Number of events per HTTP send to webhook target",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         QueueSize,
			Description: "configure channel queue size for webhook targets",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         QueueDir,
			Description: `staging dir for undelivered audit messages e.g. '/home/audit-events'`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         MaxRetry,
			Description: `maximum retry count before we start dropping audit event(s)`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         RetryInterval,
			Description: `sleep between each retries, allowed maximum value is '1m' e.g. '10s'`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         httpTimeout,
			Description: `defines the maximum duration for each http request`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpKafka = config.HelpKVS{
		config.HelpKV{
			Key:         KafkaBrokers,
			Description: "comma separated list of Kafka broker addresses",
			Type:        "csv",
		},
		config.HelpKV{
			Key:         KafkaTopic,
			Description: "Kafka topic used for bucket notifications",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KafkaSASLUsername,
			Description: "username for SASL/PLAIN or SASL/SCRAM authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         KafkaSASLPassword,
			Description: "password for SASL/PLAIN or SASL/SCRAM authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         KafkaSASLMechanism,
			Description: "sasl authentication mechanism, default 'plain'",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KafkaTLSClientAuth,
			Description: "clientAuth determines the Kafka server's policy for TLS client auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KafkaSASL,
			Description: "set to 'on' to enable SASL authentication",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         KafkaTLS,
			Description: "set to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         KafkaTLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "on" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         KafkaClientTLSCert,
			Description: "path to client certificate for mTLS auth",
			Optional:    true,
			Type:        "path",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         KafkaClientTLSKey,
			Description: "path to client key for mTLS auth",
			Optional:    true,
			Type:        "path",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         KafkaVersion,
			Description: "specify the version of the Kafka cluster",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         QueueSize,
			Description: "configure channel queue size for Kafka targets",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         QueueDir,
			Description: `staging dir for undelivered audit messages to Kafka e.g. '/home/audit-events'`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
