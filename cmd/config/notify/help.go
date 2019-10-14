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
	HelpAMQP = config.HelpKV{
		config.State:            "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:          "A comment to describe the AMQP target setting",
		target.AmqpURL:          "(Required) AMQP server endpoint, e.g. `amqp://myuser:mypassword@localhost:5672`",
		target.AmqpExchange:     "Name of the AMQP exchange",
		target.AmqpExchangeType: "Kind of AMQP exchange type",
		target.AmqpRoutingKey:   "Routing key for publishing",
		target.AmqpMandatory:    "Set this to 'on' for server to return an unroutable message with a Return method. If this flag is 'off', the server silently drops the message",
		target.AmqpDurable:      "Set this to 'on' for queue to surive broker restarts",
		target.AmqpNoWait:       "When no_wait is 'on', declare without waiting for a confirmation from the server",
		target.AmqpInternal:     "Set this to 'on' for exchange to be not used directly by publishers, but only when bound to other exchanges",
		target.AmqpAutoDeleted:  "Set this to 'on' for queue that has had at least one consumer is deleted when last consumer unsubscribes",
		target.AmqpDeliveryMode: "Delivery queue implementation use non-persistent (1) or persistent (2)",
		target.AmqpQueueLimit:   "Enable persistent event store queue limit, defaults to '10000'",
		target.AmqpQueueDir:     "Local directory where events are stored eg: '/home/events'",
	}

	HelpKafka = config.HelpKV{
		config.State:              "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:            "A comment to describe the Kafka target setting",
		target.KafkaTopic:         "The Kafka topic for a given message",
		target.KafkaBrokers:       "Command separated list of Kafka broker addresses",
		target.KafkaSASLUsername:  "Username for SASL/PLAIN  or SASL/SCRAM authentication",
		target.KafkaSASLPassword:  "Password for SASL/PLAIN  or SASL/SCRAM authentication",
		target.KafkaTLSClientAuth: "ClientAuth determines the Kafka server's policy for TLS client auth",
		target.KafkaSASLEnable:    "Set this to 'on' to enable SASL authentication",
		target.KafkaTLSEnable:     "Set this to 'on' to enable TLS",
		target.KafkaTLSSkipVerify: "Set this to 'on' to disable client verification of server certificate chain",
		target.KafkaQueueLimit:    "Enable persistent event store queue limit, defaults to '10000'",
		target.KafkaQueueDir:      "Local directory where events are stored eg: '/home/events'",
	}

	HelpMQTT = config.HelpKV{
		config.State:                 "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:               "A comment to describe the MQTT target setting",
		target.MqttBroker:            "(Required) MQTT server endpoint, e.g. `tcp://localhost:1883`",
		target.MqttTopic:             "(Required) Name of the MQTT topic to publish on, e.g. `minio`",
		target.MqttUsername:          "Username to connect to the MQTT server (if required)",
		target.MqttPassword:          "Password to connect to the MQTT server (if required)",
		target.MqttQoS:               "Set the Quality of Service Level for MQTT endpoint",
		target.MqttKeepAliveInterval: "Optional keep alive interval for MQTT connections",
		target.MqttReconnectInterval: "Optional reconnect interval for MQTT connections",
		target.MqttQueueDir:          "Local directory where events are stored eg: '/home/events'",
		target.MqttQueueLimit:        "Enable persistent event store queue limit, defaults to '10000'",
	}

	HelpES = config.HelpKV{
		config.State:             "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:           "A comment to describe the Elasticsearch target setting",
		target.ElasticURL:        "(Required) The Elasticsearch server's address, with optional authentication info",
		target.ElasticFormat:     "(Required) Either `namespace` or `access`, defaults to 'namespace'",
		target.ElasticIndex:      "(Required) The name of an Elasticsearch index in which MinIO will store document",
		target.ElasticQueueDir:   "Local directory where events are stored eg: '/home/events'",
		target.ElasticQueueLimit: "Enable persistent event store queue limit, defaults to '10000'",
	}

	HelpWebhook = config.HelpKV{
		config.State:             "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:           "A comment to describe the Webhook target setting",
		target.WebhookEndpoint:   "Webhook server endpoint eg: http://localhost:8080/minio/events",
		target.WebhookAuthToken:  "Authorization token used for webhook server endpoint (optional)",
		target.WebhookQueueLimit: "Enable persistent event store queue limit, defaults to '10000'",
		target.WebhookQueueDir:   "Local directory where events are stored eg: '/home/events'",
	}

	HelpRedis = config.HelpKV{
		config.State:           "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:         "A comment to describe the Redis target setting",
		target.RedisFormat:     "Specify how data is populated, a hash is used in case of `namespace` format and a list in case of `access` format, defaults to 'namespace'",
		target.RedisAddress:    "(Required) The Redis server's address. For example: `localhost:6379`",
		target.RedisKey:        "The name of the redis key under which events are stored",
		target.RedisPassword:   "(Optional) The Redis server's password",
		target.RedisQueueDir:   "Local directory where events are stored eg: '/home/events'",
		target.RedisQueueLimit: "Enable persistent event store queue limit, defaults to '10000'",
	}

	HelpPostgres = config.HelpKV{
		config.State:                    "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:                  "A comment to describe the Postgres target setting",
		target.PostgresFormat:           "Specify how data is populated, `namespace` format and `access` format, defaults to 'namespace'",
		target.PostgresConnectionString: "Connection string parameters for the PostgreSQL server",
		target.PostgresTable:            "(Required) Table name in which events will be stored/updated. If the table does not exist, the MinIO server creates it at start-up",
		target.PostgresHost:             "(Optional) Host name of the PostgreSQL server. Defaults to `localhost`. IPv6 host should be enclosed with `[` and `]`",
		target.PostgresPort:             "(Optional) Port on which to connect to PostgreSQL server, defaults to `5432`",
		target.PostgresUsername:         "Database username, defaults to user running the MinIO process if not specified",
		target.PostgresPassword:         "Database password",
		target.PostgresDatabase:         "Database name",
		target.PostgresQueueDir:         "Local directory where events are stored eg: '/home/events'",
		target.PostgresQueueLimit:       "Enable persistent event store queue limit, defaults to '10000'",
	}

	HelpMySQL = config.HelpKV{
		config.State:           "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:         "A comment to describe the MySQL target setting",
		target.MySQLFormat:     "Specify how data is populated, `namespace` format and `access` format, defaults to 'namespace'",
		target.MySQLHost:       "Host name of the MySQL server (used only if `dsnString` is empty)",
		target.MySQLPort:       "Port on which to connect to the MySQL server (used only if `dsn_string` is empty)",
		target.MySQLUsername:   "Database user-name (used only if `dsnString` is empty)",
		target.MySQLPassword:   "Database password (used only if `dsnString` is empty)",
		target.MySQLDatabase:   "Database name (used only if `dsnString` is empty)",
		target.MySQLDSNString:  "Data-Source-Name connection string for the MySQL server",
		target.MySQLTable:      "(Required) Table name in which events will be stored/updated. If the table does not exist, the MinIO server creates it at start-up",
		target.MySQLQueueLimit: "Enable persistent event store queue limit, defaults to '10000'",
		target.MySQLQueueDir:   "Local directory where events are stored eg: '/home/events'",
	}

	HelpNATS = config.HelpKV{
		config.State:                           "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:                         "A comment to describe the NATS target setting",
		target.NATSAddress:                     "NATS server address eg: '0.0.0.0:4222'",
		target.NATSSubject:                     "NATS subject that represents this subscription",
		target.NATSUsername:                    "Username to be used when connecting to the server",
		target.NATSPassword:                    "Password to be used when connecting to a server",
		target.NATSToken:                       "Token to be used when connecting to a server",
		target.NATSSecure:                      "Set this to 'on', enables TLS secure connections that skip server verification (not recommended)",
		target.NATSPingInterval:                "Client ping commands interval to the server, disabled by default",
		target.NATSStreamingEnable:             "Set this to 'on', to use streaming NATS server",
		target.NATSStreamingAsync:              "Set this to 'on', to enable asynchronous publish, process the ACK or error state",
		target.NATSStreamingMaxPubAcksInFlight: "Specifies how many messages can be published without getting ACKs back from NATS streaming server",
		target.NATSStreamingClusterID:          "Unique ID for the NATS streaming cluster",
		target.NATSQueueLimit:                  "Enable persistent event store queue limit, defaults to '10000'",
		target.NATSQueueDir:                    "Local directory where events are stored eg: '/home/events'",
	}

	HelpNSQ = config.HelpKV{
		config.State:            "(Required) Is this server endpoint configuration active/enabled",
		config.Comment:          "A comment to describe the NSQ target setting",
		target.NSQAddress:       "NSQ server address eg: '127.0.0.1:4150'",
		target.NSQTopic:         "NSQ topic unique per target",
		target.NSQTLSEnable:     "Set this to 'on', to enable TLS negotiation",
		target.NSQTLSSkipVerify: "Set this to 'on', to disable client verification of server certificates",
		target.NSQQueueLimit:    "Enable persistent event store queue limit, defaults to '10000'",
		target.NSQQueueDir:      "Local directory where events are stored eg: '/home/events'",
	}
)
