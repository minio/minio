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
			Description: "AMQP server endpoint, e.g. `amqp://myuser:mypassword@localhost:5672`",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.AmqpExchange,
			Description: "Name of the AMQP exchange",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpExchangeType,
			Description: "Kind of AMQP exchange type",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpRoutingKey,
			Description: "Routing key for publishing",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.AmqpMandatory,
			Description: "Set this to 'on' for server to return an unroutable message with a Return method. If this flag is 'off', the server silently drops the message",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpDurable,
			Description: "Set this to 'on' for queue to survive broker restarts",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpNoWait,
			Description: "When no_wait is 'on', declare without waiting for a confirmation from the server",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpInternal,
			Description: "Set this to 'on' for exchange to be not used directly by publishers, but only when bound to other exchanges",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpAutoDeleted,
			Description: "Set this to 'on' for queue that has had at least one consumer is deleted when last consumer unsubscribes",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.AmqpDeliveryMode,
			Description: "Delivery queue implementation use non-persistent (1) or persistent (2)",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.AmqpQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.AmqpQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the AMQP target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpKafka = config.HelpKVS{
		config.HelpKV{
			Key:         target.KafkaBrokers,
			Description: "Comma separated list of Kafka broker addresses",
			Type:        "csv",
		},
		config.HelpKV{
			Key:         target.KafkaTopic,
			Description: "The Kafka topic for a given message",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaSASLUsername,
			Description: "Username for SASL/PLAIN  or SASL/SCRAM authentication",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaSASLPassword,
			Description: "Password for SASL/PLAIN  or SASL/SCRAM authentication",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaTLSClientAuth,
			Description: "ClientAuth determines the Kafka server's policy for TLS client auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.KafkaSASL,
			Description: "Set this to 'on' to enable SASL authentication",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaTLS,
			Description: "Set this to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaTLSSkipVerify,
			Description: "Set this to 'on' to disable client verification of server certificate chain",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.KafkaQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.KafkaQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the Kafka target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpMQTT = config.HelpKVS{
		config.HelpKV{
			Key:         target.MqttBroker,
			Description: "MQTT server endpoint, e.g. `tcp://localhost:1883`",
			Type:        "uri",
		},
		config.HelpKV{
			Key:         target.MqttTopic,
			Description: "Name of the MQTT topic to publish on, e.g. `minio`",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttUsername,
			Description: "Username to connect to the MQTT server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttPassword,
			Description: "Password to connect to the MQTT server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MqttQoS,
			Description: "Set the Quality of Service Level for MQTT endpoint",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.MqttKeepAliveInterval,
			Description: "Keep alive interval for MQTT connections",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.MqttReconnectInterval,
			Description: "Reconnect interval for MQTT connections",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.MqttQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MqttQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the MQTT target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpES = config.HelpKVS{
		config.HelpKV{
			Key:         target.ElasticURL,
			Description: "The Elasticsearch server's address, with optional authentication info",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.ElasticFormat,
			Description: "Either `namespace` or `access`, defaults to 'namespace'",
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.ElasticIndex,
			Description: "The name of an Elasticsearch index in which MinIO will store document",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.ElasticQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.ElasticQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the Elasticsearch target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpWebhook = config.HelpKVS{
		config.HelpKV{
			Key:         target.WebhookEndpoint,
			Description: "Webhook server endpoint eg: http://localhost:8080/minio/events",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.WebhookAuthToken,
			Description: "Authorization token used for webhook server endpoint",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.WebhookQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.WebhookQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the Webhook target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpRedis = config.HelpKVS{
		config.HelpKV{
			Key:         target.RedisAddress,
			Description: "The Redis server's address. For example: `localhost:6379`",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.RedisFormat,
			Description: "Specify how data is populated, a hash is used in case of `namespace` format and a list in case of `access` format, defaults to 'namespace'",
			Type:        "namespace|access",
		},
		config.HelpKV{
			Key:         target.RedisKey,
			Description: "The name of the Redis key under which events are stored",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisPassword,
			Description: "The Redis server's password",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.RedisQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the Redis target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpPostgres = config.HelpKVS{
		config.HelpKV{
			Key:         target.PostgresConnectionString,
			Description: "Connection string parameters for the PostgreSQL server",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresFormat,
			Description: "Specify how data is populated, `namespace` format and `access` format, defaults to 'namespace'",
			Type:        "namespace|access",
		},
		config.HelpKV{
			Key:         target.PostgresTable,
			Description: "Table name in which events will be stored/updated. If the table does not exist, the MinIO server creates it at start-up",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresHost,
			Description: "Host name of the PostgreSQL server. Defaults to `localhost`. IPv6 host should be enclosed with `[` and `]`",
			Optional:    true,
			Type:        "hostname",
		},
		config.HelpKV{
			Key:         target.PostgresPort,
			Description: "Port on which to connect to PostgreSQL server, defaults to `5432`",
			Optional:    true,
			Type:        "port",
		},
		config.HelpKV{
			Key:         target.PostgresUsername,
			Description: "Database username, defaults to user running the MinIO process if not specified",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresPassword,
			Description: "Database password",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresDatabase,
			Description: "Postgres Database name",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.PostgresQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the Postgres target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpMySQL = config.HelpKVS{
		config.HelpKV{
			Key:         target.MySQLDSNString,
			Description: "Data-Source-Name connection string for the MySQL server",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLTable,
			Description: "Table name in which events will be stored/updated. If the table does not exist, the MinIO server creates it at start-up",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLFormat,
			Description: "Specify how data is populated, `namespace` format and `access` format, defaults to 'namespace'",
			Type:        "namespace|access",
		},
		config.HelpKV{
			Key:         target.MySQLHost,
			Description: "Host name of the MySQL server (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "hostname",
		},
		config.HelpKV{
			Key:         target.MySQLPort,
			Description: "Port on which to connect to the MySQL server (used only if `dsn_string` is empty)",
			Optional:    true,
			Type:        "port",
		},
		config.HelpKV{
			Key:         target.MySQLUsername,
			Description: "Database user-name (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLPassword,
			Description: "Database password (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLDatabase,
			Description: "Database name (used only if `dsnString` is empty)",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MySQLQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the MySQL target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpNATS = config.HelpKVS{
		config.HelpKV{
			Key:         target.NATSAddress,
			Description: "NATS server address eg: '0.0.0.0:4222'",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.NATSSubject,
			Description: "NATS subject that represents this subscription",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSUsername,
			Description: "Username to be used when connecting to the server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSPassword,
			Description: "Password to be used when connecting to a server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSToken,
			Description: "Token to be used when connecting to a server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSSecure,
			Description: "Set this to 'on', enables TLS secure connections that skip server verification (not recommended)",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSPingInterval,
			Description: "Client ping commands interval to the server, disabled by default",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.NATSStreaming,
			Description: "Set this to 'on', to use streaming NATS server",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingAsync,
			Description: "Set this to 'on', to enable asynchronous publish, process the ACK or error state",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingMaxPubAcksInFlight,
			Description: "Specifies how many messages can be published without getting ACKs back from NATS streaming server",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSStreamingClusterID,
			Description: "Unique ID for the NATS streaming cluster",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NATSCertAuthority,
			Description: "Certificate chain of the target NATS server if self signed certs were used",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSClientCert,
			Description: "TLS Cert used to authenticate against NATS configured to require client certificates",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSClientKey,
			Description: "TLS Key used to authenticate against NATS configured to require client certificates",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the NATS target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpNSQ = config.HelpKVS{
		config.HelpKV{
			Key:         target.NSQAddress,
			Description: "NSQ server address eg: '127.0.0.1:4150'",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.NSQTopic,
			Description: "NSQ topic unique per target",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NSQTLS,
			Description: "Set this to 'on', to enable TLS negotiation",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQTLSSkipVerify,
			Description: "Set this to 'on', to disable client verification of server certificates",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQQueueDir,
			Description: "Local directory where events are stored eg: '/home/events'",
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NSQQueueLimit,
			Description: "Enable persistent event store queue limit, defaults to '10000'",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the NSQ target setting",
			Optional:    true,
			Type:        "sentence",
		},
	}
)
