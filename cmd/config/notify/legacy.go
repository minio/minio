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
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/event/target"
)

// SetNotifyKafka - helper for config migration from older config.
func SetNotifyKafka(s config.Config, kName string, cfg target.KafkaArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyKafkaSubSys][kName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key: target.KafkaBrokers,
			Value: func() string {
				var brokers []string
				for _, broker := range cfg.Brokers {
					brokers = append(brokers, broker.String())
				}
				return strings.Join(brokers, config.ValueSeparator)
			}(),
		},
		config.KV{
			Key:   target.KafkaTopic,
			Value: cfg.Topic,
		},
		config.KV{
			Key:   target.KafkaQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.KafkaClientTLSCert,
			Value: cfg.TLS.ClientTLSCert,
		},
		config.KV{
			Key:   target.KafkaClientTLSKey,
			Value: cfg.TLS.ClientTLSKey,
		},
		config.KV{
			Key:   target.KafkaQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
		config.KV{
			Key:   target.KafkaTLS,
			Value: config.FormatBool(cfg.TLS.Enable),
		},
		config.KV{
			Key:   target.KafkaTLSSkipVerify,
			Value: config.FormatBool(cfg.TLS.SkipVerify),
		},
		config.KV{
			Key:   target.KafkaTLSClientAuth,
			Value: strconv.Itoa(int(cfg.TLS.ClientAuth)),
		},
		config.KV{
			Key:   target.KafkaSASL,
			Value: config.FormatBool(cfg.SASL.Enable),
		},
		config.KV{
			Key:   target.KafkaSASLUsername,
			Value: cfg.SASL.User,
		},
		config.KV{
			Key:   target.KafkaSASLPassword,
			Value: cfg.SASL.Password,
		},
	}
	return nil
}

// SetNotifyAMQP - helper for config migration from older config.
func SetNotifyAMQP(s config.Config, amqpName string, cfg target.AMQPArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyAMQPSubSys][amqpName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.AmqpURL,
			Value: cfg.URL.String(),
		},
		config.KV{
			Key:   target.AmqpExchange,
			Value: cfg.Exchange,
		},
		config.KV{
			Key:   target.AmqpRoutingKey,
			Value: cfg.RoutingKey,
		},
		config.KV{
			Key:   target.AmqpExchangeType,
			Value: cfg.ExchangeType,
		},
		config.KV{
			Key:   target.AmqpDeliveryMode,
			Value: strconv.Itoa(int(cfg.DeliveryMode)),
		},
		config.KV{
			Key:   target.AmqpMandatory,
			Value: config.FormatBool(cfg.Mandatory),
		},
		config.KV{
			Key:   target.AmqpInternal,
			Value: config.FormatBool(cfg.Immediate),
		},
		config.KV{
			Key:   target.AmqpDurable,
			Value: config.FormatBool(cfg.Durable),
		},
		config.KV{
			Key:   target.AmqpNoWait,
			Value: config.FormatBool(cfg.NoWait),
		},
		config.KV{
			Key:   target.AmqpAutoDeleted,
			Value: config.FormatBool(cfg.AutoDeleted),
		},
		config.KV{
			Key:   target.AmqpQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.AmqpQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

// SetNotifyES - helper for config migration from older config.
func SetNotifyES(s config.Config, esName string, cfg target.ElasticsearchArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyESSubSys][esName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.ElasticFormat,
			Value: cfg.Format,
		},
		config.KV{
			Key:   target.ElasticURL,
			Value: cfg.URL.String(),
		},
		config.KV{
			Key:   target.ElasticIndex,
			Value: cfg.Index,
		},
		config.KV{
			Key:   target.ElasticQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.ElasticQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

// SetNotifyRedis - helper for config migration from older config.
func SetNotifyRedis(s config.Config, redisName string, cfg target.RedisArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyRedisSubSys][redisName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.RedisFormat,
			Value: cfg.Format,
		},
		config.KV{
			Key:   target.RedisAddress,
			Value: cfg.Addr.String(),
		},
		config.KV{
			Key:   target.RedisPassword,
			Value: cfg.Password,
		},
		config.KV{
			Key:   target.RedisKey,
			Value: cfg.Key,
		},
		config.KV{
			Key:   target.RedisQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.RedisQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

// SetNotifyWebhook - helper for config migration from older config.
func SetNotifyWebhook(s config.Config, whName string, cfg target.WebhookArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyWebhookSubSys][whName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.WebhookEndpoint,
			Value: cfg.Endpoint.String(),
		},
		config.KV{
			Key:   target.WebhookAuthToken,
			Value: cfg.AuthToken,
		},
		config.KV{
			Key:   target.WebhookQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.WebhookQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
		config.KV{
			Key:   target.WebhookClientCert,
			Value: cfg.ClientCert,
		},
		config.KV{
			Key:   target.WebhookClientKey,
			Value: cfg.ClientKey,
		},
	}

	return nil
}

// SetNotifyPostgres - helper for config migration from older config.
func SetNotifyPostgres(s config.Config, psqName string, cfg target.PostgreSQLArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyPostgresSubSys][psqName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.PostgresFormat,
			Value: cfg.Format,
		},
		config.KV{
			Key:   target.PostgresConnectionString,
			Value: cfg.ConnectionString,
		},
		config.KV{
			Key:   target.PostgresTable,
			Value: cfg.Table,
		},
		config.KV{
			Key:   target.PostgresHost,
			Value: cfg.Host.String(),
		},
		config.KV{
			Key:   target.PostgresPort,
			Value: cfg.Port,
		},
		config.KV{
			Key:   target.PostgresUsername,
			Value: cfg.User,
		},
		config.KV{
			Key:   target.PostgresPassword,
			Value: cfg.Password,
		},
		config.KV{
			Key:   target.PostgresDatabase,
			Value: cfg.Database,
		},
		config.KV{
			Key:   target.PostgresQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.PostgresQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

// SetNotifyNSQ - helper for config migration from older config.
func SetNotifyNSQ(s config.Config, nsqName string, cfg target.NSQArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyNSQSubSys][nsqName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.NSQAddress,
			Value: cfg.NSQDAddress.String(),
		},
		config.KV{
			Key:   target.NSQTopic,
			Value: cfg.Topic,
		},
		config.KV{
			Key:   target.NSQTLS,
			Value: config.FormatBool(cfg.TLS.Enable),
		},
		config.KV{
			Key:   target.NSQTLSSkipVerify,
			Value: config.FormatBool(cfg.TLS.SkipVerify),
		},
		config.KV{
			Key:   target.NSQQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.NSQQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

// SetNotifyNATS - helper for config migration from older config.
func SetNotifyNATS(s config.Config, natsName string, cfg target.NATSArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyNATSSubSys][natsName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.NATSAddress,
			Value: cfg.Address.String(),
		},
		config.KV{
			Key:   target.NATSSubject,
			Value: cfg.Subject,
		},
		config.KV{
			Key:   target.NATSUsername,
			Value: cfg.Username,
		},
		config.KV{
			Key:   target.NATSPassword,
			Value: cfg.Password,
		},
		config.KV{
			Key:   target.NATSToken,
			Value: cfg.Token,
		},
		config.KV{
			Key:   target.NATSCertAuthority,
			Value: cfg.CertAuthority,
		},
		config.KV{
			Key:   target.NATSClientCert,
			Value: cfg.ClientCert,
		},
		config.KV{
			Key:   target.NATSClientKey,
			Value: cfg.ClientKey,
		},
		config.KV{
			Key:   target.NATSTLS,
			Value: config.FormatBool(cfg.Secure),
		},
		config.KV{
			Key:   target.NATSTLSSkipVerify,
			Value: config.FormatBool(cfg.Secure),
		},
		config.KV{
			Key:   target.NATSPingInterval,
			Value: strconv.FormatInt(cfg.PingInterval, 10),
		},
		config.KV{
			Key:   target.NATSQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.NATSQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
		config.KV{
			Key: target.NATSStreaming,
			Value: func() string {
				if cfg.Streaming.Enable {
					return config.EnableOn
				}
				return config.EnableOff
			}(),
		},
		config.KV{
			Key:   target.NATSStreamingClusterID,
			Value: cfg.Streaming.ClusterID,
		},
		config.KV{
			Key:   target.NATSStreamingAsync,
			Value: config.FormatBool(cfg.Streaming.Async),
		},
		config.KV{
			Key:   target.NATSStreamingMaxPubAcksInFlight,
			Value: strconv.Itoa(cfg.Streaming.MaxPubAcksInflight),
		},
	}

	return nil
}

// SetNotifyMySQL - helper for config migration from older config.
func SetNotifyMySQL(s config.Config, sqlName string, cfg target.MySQLArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyMySQLSubSys][sqlName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.MySQLFormat,
			Value: cfg.Format,
		},
		config.KV{
			Key:   target.MySQLDSNString,
			Value: cfg.DSN,
		},
		config.KV{
			Key:   target.MySQLTable,
			Value: cfg.Table,
		},
		config.KV{
			Key:   target.MySQLHost,
			Value: cfg.Host.String(),
		},
		config.KV{
			Key:   target.MySQLPort,
			Value: cfg.Port,
		},
		config.KV{
			Key:   target.MySQLUsername,
			Value: cfg.User,
		},
		config.KV{
			Key:   target.MySQLPassword,
			Value: cfg.Password,
		},
		config.KV{
			Key:   target.MySQLDatabase,
			Value: cfg.Database,
		},
		config.KV{
			Key:   target.MySQLQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.MySQLQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

// SetNotifyMQTT - helper for config migration from older config.
func SetNotifyMQTT(s config.Config, mqttName string, cfg target.MQTTArgs) error {
	if !cfg.Enable {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyMQTTSubSys][mqttName] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   target.MqttBroker,
			Value: cfg.Broker.String(),
		},
		config.KV{
			Key:   target.MqttTopic,
			Value: cfg.Topic,
		},
		config.KV{
			Key:   target.MqttQoS,
			Value: fmt.Sprintf("%d", cfg.QoS),
		},
		config.KV{
			Key:   target.MqttUsername,
			Value: cfg.User,
		},
		config.KV{
			Key:   target.MqttPassword,
			Value: cfg.Password,
		},
		config.KV{
			Key:   target.MqttReconnectInterval,
			Value: cfg.MaxReconnectInterval.String(),
		},
		config.KV{
			Key:   target.MqttKeepAliveInterval,
			Value: cfg.KeepAlive.String(),
		},
		config.KV{
			Key:   target.MqttQueueDir,
			Value: cfg.QueueDir,
		},
		config.KV{
			Key:   target.MqttQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}
