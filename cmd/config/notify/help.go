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

package notify

import (
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/event/target"
)

// Help template inputs for all notification targets
var (
	HelpAMQP = config.HelpKVS{
		config.HelpKV{
			Key:         target.AmqpURL,
			Description: "AMQP server endpoint e.g. `amqp://myuser:mypassword@localhost:5672`",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.AmqpExchange,
			Description: "name of the AMQP exchange",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpExchangeType,
			Description: "kind of AMQP exchange type",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpRoutingKey,
			Description: "routing key for publishing",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpMandatory,
			Description: "set this to 'on' for server to return an unroutable message with a Return method. If this flag is 'off', the server silently drops the message",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpDurable,
			Description: "set this to 'on' for queue to survive broker restarts",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpNoWait,
			Description: "when no_wait is 'on', declare without waiting for a confirmation from the server",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpInternal,
			Description: "set this to 'on' for exchange to be not used directly by publishers, but only when bound to other exchanges",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpAutoDeleted,
			Description: "set this to 'on' for queue that has had at least one consumer is deleted when last consumer unsubscribes",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpDeliveryMode,
			Description: "delivery queue implementation use non-persistent (1) or persistent (2)",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.AmqpQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.AmqpQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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
		},
		config.HelpKV{
			Key:         target.KafkaSASLPassword,
			Description: "password for SASL/PLAIN or SASL/SCRAM authentication",
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
			Description: "set this to 'on' to enable SASL authentication",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaTLS,
			Description: "set this to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaTLSSkipVerify,
			Description: "set this to 'on' to disable client verification of server certificate chain",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.KafkaQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.KafkaClientTLSCert,
			Description: "Set path to client certificate",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.KafkaClientTLSKey,
			Description: "Set path to client key",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpMQTT = config.HelpKVS{
		config.HelpKV{
			Key:         target.MqttBroker,
			Description: "MQTT server endpoint e.g. `tcp://localhost:1883`",
			Type:        "uri",
		},
		config.HelpKV{
			Key:         target.MqttTopic,
			Description: "name of the MQTT topic to publish on, e.g. `minio`",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttUsername,
			Description: "username to connect to the MQTT server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttPassword,
			Description: "password to connect to the MQTT server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttQoS,
			Description: "set the Quality of Service Level for MQTT endpoint",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.MqttKeepAliveInterval,
			Description: "keep alive interval for MQTT connections",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.MqttReconnectInterval,
			Description: "reconnect interval for MQTT connections",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.MqttQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MqttQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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
		},
		config.HelpKV{
			Key:         target.ElasticFormat,
			Description: "set this to `namespace` or `access`, defaults to 'namespace'",
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.ElasticIndex,
			Description: "the name of an Elasticsearch index in which MinIO will store document",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.ElasticQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.ElasticQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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

	HelpWebhook = config.HelpKVS{
		config.HelpKV{
			Key:         target.WebhookEndpoint,
			Description: "webhook server endpoint e.g. http://localhost:8080/minio/events",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.WebhookAuthToken,
			Description: "authorization token used for webhook server endpoint",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.WebhookQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.WebhookQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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

	HelpRedis = config.HelpKVS{
		config.HelpKV{
			Key:         target.RedisAddress,
			Description: "Redis server's address. For example: `localhost:6379`",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.RedisFormat,
			Description: "specifies how data is populated, a hash is used in case of `namespace` format and a list in case of `access` format, defaults to 'namespace'",
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.RedisKey,
			Description: "name of the Redis key under which events are stored",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisPassword,
			Description: "Redis server's password",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.RedisQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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
			Description: "connection string parameters for the PostgreSQL server",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresFormat,
			Description: "specifies how data is populated, `namespace` format and `access` format, defaults to 'namespace'",
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.PostgresTable,
			Description: "table name in which events will be stored/updated. If the table does not exist, the MinIO server creates it at start-up",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresHost,
			Description: "host name of the PostgreSQL server. Defaults to `localhost`. IPv6 host should be enclosed with `[` and `]`",
			Optional:    true,
			Type:        "hostname",
		},
		config.HelpKV{
			Key:         target.PostgresPort,
			Description: "port on which to connect to PostgreSQL server, defaults to `5432`",
			Optional:    true,
			Type:        "port",
		},
		config.HelpKV{
			Key:         target.PostgresUsername,
			Description: "database username, defaults to user running the MinIO process if not specified",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresPassword,
			Description: "database password",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresDatabase,
			Description: "postgres Database name",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.PostgresQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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

	HelpMySQL = config.HelpKVS{
		config.HelpKV{
			Key:         target.MySQLDSNString,
			Description: "data source name connection string for the MySQL server",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLTable,
			Description: "table name in which events will be stored/updated. If the table does not exist, the MinIO server creates it at start-up",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLFormat,
			Description: "specifies how data is populated, `namespace` format and `access` format, defaults to 'namespace'",
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.MySQLHost,
			Description: "host name of the MySQL server (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "hostname",
		},
		config.HelpKV{
			Key:         target.MySQLPort,
			Description: "port on which to connect to the MySQL server (used only if `dsn_string` is empty)",
			Optional:    true,
			Type:        "port",
		},
		config.HelpKV{
			Key:         target.MySQLUsername,
			Description: "database user-name (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLPassword,
			Description: "database password (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLDatabase,
			Description: "database name (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MySQLQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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

	HelpNATS = config.HelpKVS{
		config.HelpKV{
			Key:         target.NATSAddress,
			Description: "NATS server address e.g. '0.0.0.0:4222'",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.NATSSubject,
			Description: "NATS subject that represents this subscription",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSUsername,
			Description: "username to be used when connecting to the server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSPassword,
			Description: "password to be used when connecting to a server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSToken,
			Description: "token to be used when connecting to a server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSSecure,
			Description: "set this to 'on', enables TLS secure connections that skip server verification (not recommended)",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSPingInterval,
			Description: "client ping commands interval to the server, disabled by default",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.NATSStreaming,
			Description: "set this to 'on', to use streaming NATS server",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingAsync,
			Description: "set this to 'on', to enable asynchronous publish, process the ACK or error state",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingMaxPubAcksInFlight,
			Description: "specifies how many messages can be published without getting ACKs back from NATS streaming server",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSStreamingClusterID,
			Description: "unique ID for the NATS streaming cluster",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NATSCertAuthority,
			Description: "certificate chain of the target NATS server if self signed certs were used",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSClientCert,
			Description: "TLS Cert used for NATS configured to require client certificates",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSClientKey,
			Description: "TLS Key used for NATS configured to require client certificates",
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
		},
		config.HelpKV{
			Key:         target.NSQTopic,
			Description: "NSQ topic unique per target",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NSQTLS,
			Description: "set this to 'on', to enable TLS negotiation",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQTLSSkipVerify,
			Description: "set this to 'on', to disable client verification of server certificates",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQQueueDir,
			Description: "local directory where events are stored e.g. '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NSQQueueLimit,
			Description: "enable persistent event store queue limit, defaults to '10000'",
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
