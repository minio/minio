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

package notify

import (
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/event/target"
)

const (
	formatComment     = `'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'`
	queueDirComment   = `staging dir for undelivered messages e.g. '/home/events'`
	queueLimitComment = `maximum limit for undelivered messages, defaults to '100000'`
)

// Help template inputs for all notification targets
var (
	HelpWebhook = config.HelpKVS{
		config.HelpKV{
			Key:         target.WebhookEndpoint,
			Description: "webhook server endpoint e.g. http://localhost:8080/minio/events",
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.WebhookAuthToken,
			Description: "opaque string or JWT authorization token",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         target.WebhookQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.WebhookQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.WebhookClientCert,
			Description: "client cert for Webhook mTLS auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.WebhookClientKey,
			Description: "client cert key for Webhook mTLS auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
	}

	HelpAMQP = config.HelpKVS{
		config.HelpKV{
			Key:         target.AmqpURL,
			Description: "AMQP server endpoint e.g. `amqp://myuser:mypassword@localhost:5672`",
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.AmqpExchange,
			Description: "name of the AMQP exchange",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpExchangeType,
			Description: "AMQP exchange type",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpRoutingKey,
			Description: "routing key for publishing",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.AmqpMandatory,
			Description: "quietly ignore undelivered messages when set to 'off', default is 'on'",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpDurable,
			Description: "persist queue across broker restarts when set to 'on', default is 'off'",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpNoWait,
			Description: "non-blocking message delivery when set to 'on', default is 'off'",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpInternal,
			Description: "set to 'on' for exchange to be not used directly by publishers, but only when bound to other exchanges",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpAutoDeleted,
			Description: "auto delete queue when set to 'on', when there are no consumers",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpDeliveryMode,
			Description: "set to '1' for non-persistent or '2' for persistent queue",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.AmqpPublisherConfirms,
			Description: "enable consumer acknowledgement and publisher confirms, use this along with queue_dir for guaranteed delivery of all events",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.AmqpQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
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
			Key:         target.KafkaBrokers,
			Description: "comma separated list of Kafka broker addresses",
			Type:        "csv",
		},
		config.HelpKV{
			Key:         target.KafkaTopic,
			Description: "Kafka topic used for bucket notifications",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaSASLUsername,
			Description: "username for SASL/PLAIN or SASL/SCRAM authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.KafkaSASLPassword,
			Description: "password for SASL/PLAIN or SASL/SCRAM authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         target.KafkaSASLMechanism,
			Description: "sasl authentication mechanism, default 'plain'",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaTLSClientAuth,
			Description: "clientAuth determines the Kafka server's policy for TLS client auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaSASL,
			Description: "set to 'on' to enable SASL authentication",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaTLS,
			Description: "set to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaTLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "on" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaClientTLSCert,
			Description: "path to client certificate for mTLS auth",
			Optional:    true,
			Type:        "path",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.KafkaClientTLSKey,
			Description: "path to client key for mTLS auth",
			Optional:    true,
			Type:        "path",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.KafkaQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.KafkaQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.KafkaVersion,
			Description: "specify the version of the Kafka cluster",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.KafkaCompressionCodec,
			Description: "specify compression_codec of the Kafka cluster",
			Optional:    true,
			Type:        "none|snappy|gzip|lz4|zstd",
		},
		config.HelpKV{
			Key:         target.KafkaCompressionLevel,
			Description: "specify compression level of the Kafka cluster",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.KafkaBatchSize,
			Description: "batch size of the events; used only when queue_dir is set",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.KafkaBatchCommitTimeout,
			Description: "commit timeout set for the batch; used only when batch_size > 1",
			Optional:    true,
			Type:        "duration",
		},
	}

	HelpMQTT = config.HelpKVS{
		config.HelpKV{
			Key:         target.MqttBroker,
			Description: "MQTT server endpoint e.g. `tcp://localhost:1883`",
			Type:        "uri",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.MqttTopic,
			Description: "name of the MQTT topic to publish",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttUsername,
			Description: "MQTT username",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.MqttPassword,
			Description: "MQTT password",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         target.MqttQoS,
			Description: "set the quality of service priority, defaults to '0'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.MqttKeepAliveInterval,
			Description: "keep-alive interval for MQTT connections in s,m,h,d",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.MqttReconnectInterval,
			Description: "reconnect interval for MQTT connections in s,m,h,d",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.MqttQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MqttQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpPostgres = config.HelpKVS{
		config.HelpKV{
			Key:         target.PostgresConnectionString,
			Description: `Postgres server connection-string e.g. "host=localhost port=5432 dbname=minio_events user=postgres password=password sslmode=disable"`,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.PostgresTable,
			Description: "DB table name to store/update events, table is auto-created",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.PostgresQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.PostgresQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.PostgresMaxOpenConnections,
			Description: "To set the maximum number of open connections to the database. The value is set to `2` by default.",
			Optional:    true,
			Type:        "number",
		},
	}

	HelpMySQL = config.HelpKVS{
		config.HelpKV{
			Key:         target.MySQLDSNString,
			Description: `MySQL data-source-name connection string e.g. "<user>:<password>@tcp(<host>:<port>)/<database>"`,
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.MySQLTable,
			Description: "DB table name to store/update events, table is auto-created",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.MySQLQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MySQLQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.MySQLMaxOpenConnections,
			Description: "To set the maximum number of open connections to the database. The value is set to `2` by default.",
			Optional:    true,
			Type:        "number",
		},
	}

	HelpNATS = config.HelpKVS{
		config.HelpKV{
			Key:         target.NATSAddress,
			Description: "NATS server address e.g. '0.0.0.0:4222'",
			Type:        "address",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.NATSSubject,
			Description: "NATS subscription subject",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSUsername,
			Description: "NATS username",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.NATSPassword,
			Description: "NATS password",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         target.NATSToken,
			Description: "NATS token",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         target.NATSTLS,
			Description: "set to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSTLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "on" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSPingInterval,
			Description: "client ping commands interval in s,m,h,d. Disabled by default",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.NATSCertAuthority,
			Description: "path to certificate chain of the target NATS server",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.NATSClientCert,
			Description: "client cert for NATS mTLS auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.NATSClientKey,
			Description: "client cert key for NATS mTLS auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.NATSJetStream,
			Description: "enable JetStream support",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NATSQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSStreaming,
			Description: "[DEPRECATED] set to 'on', to use streaming NATS server",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingAsync,
			Description: "[DEPRECATED] set to 'on', to enable asynchronous publish",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingMaxPubAcksInFlight,
			Description: "[DEPRECATED] number of messages to publish without waiting for ACKs",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSStreamingClusterID,
			Description: "[DEPRECATED] unique ID for NATS streaming cluster",
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

	HelpNSQ = config.HelpKVS{
		config.HelpKV{
			Key:         target.NSQAddress,
			Description: "NSQ server address e.g. '127.0.0.1:4150'",
			Type:        "address",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.NSQTopic,
			Description: "NSQ topic",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NSQTLS,
			Description: "set to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQTLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "on" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NSQQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpES = config.HelpKVS{
		config.HelpKV{
			Key:         target.ElasticURL,
			Description: "Elasticsearch server's address, with optional authentication info",
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.ElasticIndex,
			Description: `Elasticsearch index to store/update events, index is auto-created`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.ElasticFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.ElasticQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.ElasticQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.ElasticUsername,
			Description: "username for Elasticsearch basic-auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.ElasticPassword,
			Description: "password for Elasticsearch basic-auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpRedis = config.HelpKVS{
		config.HelpKV{
			Key:         target.RedisAddress,
			Description: "Redis server's address. For example: `localhost:6379`",
			Type:        "address",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.RedisKey,
			Description: "Redis key to store/update events, key is auto-created",
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.RedisFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.RedisPassword,
			Description: "Redis server password",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         target.RedisUser,
			Description: "Redis server user for the auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.RedisQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
