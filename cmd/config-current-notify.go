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

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event/target"
	xnet "github.com/minio/minio/pkg/net"
)

// Notification config constants.
const (
	notifyKafkaSubSys    = "notify.kafka"
	notifyMQTTSubSys     = "notify.mqtt"
	notifyMySQLSubSys    = "notify.mysql"
	notifyNATSSubSys     = "notify.nats"
	notifyNSQSubSys      = "notify.nsq"
	notifyESSubSys       = "notify.elasticsearch"
	notifyAMQPSubSys     = "notify.amqp"
	notifyPostgresSubSys = "notify.postgres"
	notifyRedisSubSys    = "notify.redis"
	notifyWebhookSubSys  = "notify.webhook"

	// Add new constants here if you add new fields to config.
)

// TestNotificationTargets tries to establish connections to all notification
// targets when enabled. This is a good way to make sure all configurations
// set by the user can work.
func (s serverConfig) TestNotificationTargets() error {
	for k, v := range s.GetNotifyAMQP() {
		if !v.Enable {
			continue
		}
		t, err := target.NewAMQPTarget(k, v)
		if err != nil {
			return fmt.Errorf("amqp(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyES() {
		if !v.Enable {
			continue
		}
		t, err := target.NewElasticsearchTarget(k, v, GlobalServiceDoneCh)
		if err != nil {
			return fmt.Errorf("elasticsearch(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyKafka() {
		if !v.Enable {
			continue
		}
		t, err := target.NewKafkaTarget(k, v, GlobalServiceDoneCh)
		if err != nil {
			return fmt.Errorf("kafka(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyMQTT() {
		if !v.Enable {
			continue
		}
		t, err := target.NewMQTTTarget(k, v, GlobalServiceDoneCh)
		if err != nil {
			return fmt.Errorf("mqtt(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyMySQL() {
		if !v.Enable {
			continue
		}
		t, err := target.NewMySQLTarget(k, v)
		if err != nil {
			return fmt.Errorf("mysql(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyNATS() {
		if !v.Enable {
			continue
		}
		t, err := target.NewNATSTarget(k, v)
		if err != nil {
			return fmt.Errorf("nats(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyNSQ() {
		if !v.Enable {
			continue
		}
		t, err := target.NewNSQTarget(k, v, GlobalServiceDoneCh)
		if err != nil {
			return fmt.Errorf("nsq(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyPostgres() {
		if !v.Enable {
			continue
		}
		t, err := target.NewPostgreSQLTarget(k, v)
		if err != nil {
			return fmt.Errorf("postgreSQL(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyRedis() {
		if !v.Enable {
			continue
		}
		t, err := target.NewRedisTarget(k, v)
		if err != nil {
			return fmt.Errorf("redis(%s): %s", k, err)
		}
		t.Close()

	}

	return nil
}

func (s serverConfig) SetNotifyKafka(kName string, cfg target.KafkaArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyKafkaSubSys][kName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key: "brokers",
			Value: func() string {
				var brokers []string
				for _, broker := range cfg.Brokers {
					brokers = append(brokers, broker.String())
				}
				return strings.Join(brokers, ",")
			}(),
		},
		{
			Key:   "topic",
			Value: cfg.Topic,
		},
		{
			Key:   "queueDir",
			Value: cfg.QueueDir,
		},
		{
			Key:   "queueLimit",
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
		{
			Key:   "tls.enable",
			Value: strconv.FormatBool(cfg.TLS.Enable),
		},
		{
			Key:   "tls.skipVerify",
			Value: strconv.FormatBool(cfg.TLS.SkipVerify),
		},
		{
			Key:   "tls.clientAuth",
			Value: strconv.Itoa(int(cfg.TLS.ClientAuth)),
		},
		{
			Key:   "sasl.enable",
			Value: strconv.FormatBool(cfg.SASL.Enable),
		},
		{
			Key:   "sasl.user",
			Value: cfg.SASL.User,
		},
		{
			Key:   "sasl.password",
			Value: cfg.SASL.Password,
		},
	}
	return nil
}

func (s serverConfig) GetNotifyKafka() map[string]target.KafkaArgs {
	kafkaTargets := make(map[string]target.KafkaArgs)
	for k, kvs := range s[notifyKafkaSubSys] {
		enabled := kvs.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		var err error
		var brokers []xnet.Host
		for _, s := range strings.Split(kvs.Get("brokers"), ",") {
			var host *xnet.Host
			host, err = xnet.ParseHost(s)
			if err != nil {
				break
			}
			brokers = append(brokers, *host)
		}
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		queueLimit, err := strconv.Atoi(kvs.Get("queueLimit"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		clientAuth, err := strconv.Atoi(kvs.Get("tls.clientAuth"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		kafkaArgs := target.KafkaArgs{
			Enable:     enabled,
			Brokers:    brokers,
			Topic:      kvs.Get("topic"),
			QueueDir:   kvs.Get("queueDir"),
			QueueLimit: uint64(queueLimit),
		}

		kafkaArgs.TLS.Enable = kvs.Get("tls.enable") == "true"
		kafkaArgs.TLS.SkipVerify = kvs.Get("tls.skipVerify") == "true"
		kafkaArgs.TLS.ClientAuth = tls.ClientAuthType(clientAuth)

		kafkaArgs.SASL.Enable = kvs.Get("sasl.enable") == "true"
		kafkaArgs.SASL.User = kvs.Get("sasl.user")
		kafkaArgs.SASL.Password = kvs.Get("sasl.password")

		if err = kafkaArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		kafkaTargets[k] = kafkaArgs
	}

	return kafkaTargets
}

func (s serverConfig) SetNotifyMQTT(mqttName string, cfg target.MQTTArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyMQTTSubSys][mqttName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   target.MqttBroker,
			Value: cfg.Broker.String(),
		},
		{
			Key:   target.MqttTopic,
			Value: cfg.Topic,
		},
		{
			Key:   target.MqttQoS,
			Value: fmt.Sprintf("%d", cfg.QoS),
		},
		{
			Key:   target.MqttUsername,
			Value: cfg.User,
		},
		{
			Key:   target.MqttPassword,
			Value: cfg.Password,
		},
		{
			Key:   target.MqttReconnectInterval,
			Value: cfg.MaxReconnectInterval.String(),
		},
		{
			Key:   target.MqttKeepAliveInterval,
			Value: cfg.KeepAlive.String(),
		},
		{
			Key:   target.MqttQueueDir,
			Value: cfg.QueueDir,
		},
		{
			Key:   target.MqttQueueLimit,
			Value: strconv.Itoa(int(cfg.QueueLimit)),
		},
	}

	return nil
}

func (s serverConfig) GetNotifyMQTT() map[string]target.MQTTArgs {
	mqttTargets := make(map[string]target.MQTTArgs)
	for k, kvs := range s[notifyMQTTSubSys] {
		enabled := kvs.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}

		brokerURL, err := xnet.ParseURL(kvs.Get(target.MqttBroker))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		reconnectInterval, err := time.ParseDuration(kvs.Get(target.MqttReconnectInterval))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		keepAliveInterval, err := time.ParseDuration(kvs.Get(target.MqttKeepAliveInterval))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kvs.Get(target.MqttQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		// Parse uint8 value
		qos, err := strconv.ParseUint(kvs.Get(target.MqttQoS), 10, 8)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		mqttArgs := target.MQTTArgs{
			Enable:               enabled,
			Broker:               *brokerURL,
			Topic:                kvs.Get(target.MqttTopic),
			QoS:                  byte(qos),
			User:                 kvs.Get(target.MqttUsername),
			Password:             kvs.Get(target.MqttPassword),
			MaxReconnectInterval: reconnectInterval,
			KeepAlive:            keepAliveInterval,
			RootCAs:              globalRootCAs,
			QueueDir:             kvs.Get(target.MqttQueueDir),
			QueueLimit:           uint64(queueLimit),
		}

		if err = mqttArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		mqttTargets[k] = mqttArgs
	}
	return mqttTargets
}

func (s serverConfig) SetNotifyMySQL(sqlName string, cfg target.MySQLArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyMySQLSubSys][sqlName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "format",
			Value: cfg.Format,
		},
		{
			Key:   "dsnString",
			Value: cfg.DSN,
		},
		{
			Key:   "table",
			Value: cfg.Table,
		},
		{
			Key:   "host",
			Value: cfg.Host.String(),
		},
		{
			Key:   "port",
			Value: cfg.Port,
		},
		{
			Key:   "user",
			Value: cfg.User,
		},
		{
			Key:   "password",
			Value: cfg.Password,
		},
		{
			Key:   "database",
			Value: cfg.Database,
		},
	}

	return nil
}

func (s serverConfig) GetNotifyMySQL() map[string]target.MySQLArgs {
	mysqlTargets := make(map[string]target.MySQLArgs)
	for k, kvs := range s[notifyMySQLSubSys] {
		enabled := kvs.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		host, err := xnet.ParseURL(kvs.Get("host"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		mysqlArgs := target.MySQLArgs{
			Enable:   enabled,
			Format:   kvs.Get("format"),
			DSN:      kvs.Get("dsnString"),
			Table:    kvs.Get("table"),
			Host:     *host,
			Port:     kvs.Get("port"),
			User:     kvs.Get("user"),
			Password: kvs.Get("password"),
			Database: kvs.Get("database"),
		}
		if err = mysqlArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		mysqlTargets[k] = mysqlArgs
	}
	return mysqlTargets
}

func (s serverConfig) SetNotifyNATS(natsName string, cfg target.NATSArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyNATSSubSys][natsName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "address",
			Value: cfg.Address.String(),
		},
		{
			Key:   "subject",
			Value: cfg.Subject,
		},
		{
			Key:   "username",
			Value: cfg.Username,
		},
		{
			Key:   "password",
			Value: cfg.Password,
		},
		{
			Key:   "token",
			Value: cfg.Token,
		},
		{
			Key:   "secure",
			Value: strconv.FormatBool(cfg.Secure),
		},
		{
			Key:   "pingInterval",
			Value: strconv.FormatInt(cfg.PingInterval, 10),
		},
		{
			Key: "streaming.enable",
			Value: func() string {
				if cfg.Streaming.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "streaming.clusterID",
			Value: cfg.Streaming.ClusterID,
		},
		{
			Key:   "streaming.async",
			Value: strconv.FormatBool(cfg.Streaming.Async),
		},
		{
			Key:   "streaming.maxPubAcksInflight",
			Value: strconv.Itoa(cfg.Streaming.MaxPubAcksInflight),
		},
	}

	return nil
}

func (s serverConfig) GetNotifyNATS() map[string]target.NATSArgs {
	natsTargets := make(map[string]target.NATSArgs)
	for k, kv := range s[notifyNATSSubSys] {
		enabled := kv.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}

		address, err := xnet.ParseHost(kv.Get("address"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		secure, err := strconv.ParseBool(kv.Get("secure"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		pingInterval, err := strconv.ParseInt(kv.Get("pingInterval"), 10, 64)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		natsArgs := target.NATSArgs{
			Enable:       true,
			Address:      *address,
			Subject:      kv.Get("subject"),
			Username:     kv.Get("username"),
			Password:     kv.Get("password"),
			Token:        kv.Get("token"),
			Secure:       secure,
			PingInterval: pingInterval,
		}
		streamingEnabled := kv.Get("streaming.enable") == stateEnabled
		if streamingEnabled {
			async, err := strconv.ParseBool(kv.Get("streaming.async"))
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			maxPubAcksInflight, err := strconv.Atoi(kv.Get("streaming.maxPubAcksInflight"))
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			natsArgs.Streaming.Enable = streamingEnabled
			natsArgs.Streaming.ClusterID = kv.Get("streaming.clusterID")
			natsArgs.Streaming.Async = async
			natsArgs.Streaming.MaxPubAcksInflight = maxPubAcksInflight
		}

		if err = natsArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		natsTargets[k] = natsArgs
	}
	return natsTargets
}

func (s serverConfig) GetNotifyNSQ() map[string]target.NSQArgs {
	nsqTargets := make(map[string]target.NSQArgs)
	for k, kv := range s[notifyNSQSubSys] {
		enabled := kv.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		nsqdAddress, err := xnet.ParseHost(kv.Get("nsqdAddress"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		tlsEnable, err := strconv.ParseBool(kv.Get("tls.enable"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		tlsSkipVerify, err := strconv.ParseBool(kv.Get("tls.skipVerify"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		nsqArgs := target.NSQArgs{
			Enable:      enabled,
			NSQDAddress: *nsqdAddress,
			Topic:       kv.Get("topic"),
		}
		nsqArgs.TLS.Enable = tlsEnable
		nsqArgs.TLS.SkipVerify = tlsSkipVerify
		if err = nsqArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		nsqTargets[k] = nsqArgs
	}
	return nsqTargets
}

func (s serverConfig) SetNotifyNSQ(nsqName string, cfg target.NSQArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyNSQSubSys][nsqName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "nsqdAddress",
			Value: cfg.NSQDAddress.String(),
		},
		{
			Key:   "topic",
			Value: cfg.Topic,
		},
		{
			Key:   "tls.enable",
			Value: strconv.FormatBool(cfg.TLS.Enable),
		},
		{
			Key:   "tls.skipVerify",
			Value: strconv.FormatBool(cfg.TLS.SkipVerify),
		},
	}

	return nil
}

func (s serverConfig) GetNotifyPostgres() map[string]target.PostgreSQLArgs {
	psqlTargets := make(map[string]target.PostgreSQLArgs)
	for k, kv := range s[notifyPostgresSubSys] {
		enabled := kv.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		host, err := xnet.ParseHost(kv.Get("host"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		psqlArgs := target.PostgreSQLArgs{
			Enable:           enabled,
			Format:           kv.Get("format"),
			ConnectionString: kv.Get("connectionString"),
			Table:            kv.Get("table"),
			Host:             *host,
			Port:             kv.Get("port"),
			User:             kv.Get("user"),
			Password:         kv.Get("password"),
			Database:         kv.Get("database"),
		}
		if err = psqlArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		psqlTargets[k] = psqlArgs
	}
	return psqlTargets
}

func (s serverConfig) SetNotifyPostgres(psqName string, cfg target.PostgreSQLArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyPostgresSubSys][psqName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "format",
			Value: cfg.Format,
		},
		{
			Key:   "connectionString",
			Value: cfg.ConnectionString,
		},
		{
			Key:   "table",
			Value: cfg.Table,
		},
		{
			Key:   "host",
			Value: cfg.Host.String(),
		},
		{
			Key:   "port",
			Value: cfg.Port,
		},
		{
			Key:   "user",
			Value: cfg.User,
		},
		{
			Key:   "password",
			Value: cfg.Password,
		},
		{
			Key:   "database",
			Value: cfg.Database,
		},
	}

	return nil
}

func (s serverConfig) GetNotifyRedis() map[string]target.RedisArgs {
	redisTargets := make(map[string]target.RedisArgs)
	for k, kv := range s[notifyRedisSubSys] {
		enabled := kv.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		addr, err := xnet.ParseHost(kv.Get("address"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		redisArgs := target.RedisArgs{
			Enable:   enabled,
			Format:   kv.Get("format"),
			Addr:     *addr,
			Password: kv.Get("password"),
			Key:      kv.Get("key"),
		}
		if err = redisArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		redisTargets[k] = redisArgs
	}
	return redisTargets
}

func (s serverConfig) SetNotifyRedis(redisName string, cfg target.RedisArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyRedisSubSys][redisName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "format",
			Value: cfg.Format,
		},
		{
			Key:   "address",
			Value: cfg.Addr.String(),
		},
		{
			Key:   "password",
			Value: cfg.Password,
		},
		{
			Key:   "key",
			Value: cfg.Key,
		},
	}

	return nil
}

func (s serverConfig) SetNotifyWebhook(whName string, cfg target.WebhookArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyWebhookSubSys][whName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "endpoint",
			Value: cfg.Endpoint.String(),
		},
	}

	return nil
}

func (s serverConfig) GetNotifyWebhook() map[string]target.WebhookArgs {
	webhookTargets := make(map[string]target.WebhookArgs)
	for k, kv := range s[notifyWebhookSubSys] {
		enabled := kv.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		url, err := xnet.ParseURL(kv.Get("endpoint"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		webhookArgs := target.WebhookArgs{
			Enable:   enabled,
			Endpoint: *url,
			RootCAs:  globalRootCAs,
		}
		if err = webhookArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		webhookTargets[k] = webhookArgs
	}
	return webhookTargets
}

func (s serverConfig) SetNotifyES(esName string, cfg target.ElasticsearchArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyESSubSys][esName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   "format",
			Value: cfg.Format,
		},
		{
			Key:   "url",
			Value: cfg.URL.String(),
		},
		{
			Key:   "index",
			Value: cfg.Index,
		},
	}

	return nil
}

func (s serverConfig) GetNotifyES() map[string]target.ElasticsearchArgs {
	esTargets := make(map[string]target.ElasticsearchArgs)
	for k, kvs := range s[notifyESSubSys] {
		enabled := kvs.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		url, err := xnet.ParseURL(kvs.Get("url"))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		esArgs := target.ElasticsearchArgs{
			Enable: enabled,
			Format: kvs.Get("format"),
			URL:    *url,
			Index:  kvs.Get("index"),
		}
		if err = esArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		esTargets[k] = esArgs
	}
	return esTargets
}

func (s serverConfig) GetNotifyAMQP() map[string]target.AMQPArgs {
	amqpTargets := make(map[string]target.AMQPArgs)
	for k, kvs := range s[notifyAMQPSubSys] {
		enabled := kvs.Get(stateKey) == stateEnabled
		if !enabled {
			continue
		}
		url, err := xnet.ParseURL(kvs.Get(target.AmqpURL))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		deliveryMode, err := strconv.Atoi(kvs.Get(target.AmqpDeliveryMode))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		amqpArgs := target.AMQPArgs{
			Enable:       enabled,
			URL:          *url,
			Exchange:     kvs.Get(target.AmqpExchange),
			RoutingKey:   kvs.Get(target.AmqpRoutingKey),
			ExchangeType: kvs.Get(target.AmqpExchangeType),
			DeliveryMode: uint8(deliveryMode),
			Mandatory:    kvs.Get(target.AmqpMandatory) == "true",
			Immediate:    kvs.Get(target.AmqpImmediate) == "true",
			Durable:      kvs.Get(target.AmqpDurable) == "true",
			Internal:     kvs.Get(target.AmqpInternal) == "true",
			NoWait:       kvs.Get(target.AmqpNoWait) == "true",
			AutoDeleted:  kvs.Get(target.AmqpAutoDeleted) == "true",
		}
		if err = amqpArgs.Validate(); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		amqpTargets[k] = amqpArgs
	}
	return amqpTargets
}

func (s serverConfig) SetNotifyAMQP(amqpName string, cfg target.AMQPArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[notifyAMQPSubSys][amqpName] = []KV{
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enable {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
		{
			Key:   target.AmqpURL,
			Value: cfg.URL.String(),
		},
		{
			Key:   target.AmqpExchange,
			Value: cfg.Exchange,
		},
		{
			Key:   target.AmqpRoutingKey,
			Value: cfg.RoutingKey,
		},
		{
			Key:   target.AmqpExchangeType,
			Value: cfg.ExchangeType,
		},
		{
			Key:   target.AmqpDeliveryMode,
			Value: strconv.Itoa(int(cfg.DeliveryMode)),
		},
		{
			Key:   target.AmqpMandatory,
			Value: strconv.FormatBool(cfg.Mandatory),
		},
		{
			Key:   target.AmqpInternal,
			Value: strconv.FormatBool(cfg.Immediate),
		},
		{
			Key:   target.AmqpDurable,
			Value: strconv.FormatBool(cfg.Durable),
		},
		{
			Key:   target.AmqpInternal,
			Value: strconv.FormatBool(cfg.Internal),
		},
		{
			Key:   target.AmqpNoWait,
			Value: strconv.FormatBool(cfg.NoWait),
		},
		{
			Key:   target.AmqpAutoDeleted,
			Value: strconv.FormatBool(cfg.AutoDeleted),
		},
	}

	return nil
}
