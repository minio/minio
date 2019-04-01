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
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	xnet "github.com/minio/minio/pkg/net"
	config "github.com/minio/minio/pkg/server-config"
)

// GetNotificationTargets - returns TargetList which contains enabled targets in serverConfig.
// A new notification target is added like below
// * Add a new target in pkg/event/target package.
// * Add newly added target configuration to serverConfig.Notify.<TARGET_NAME>.
// * Handle the configuration in this function to create/add into TargetList.
func (s serverConfig) GetNotificationTargets() *event.TargetList {
	targetList := event.NewTargetList()
	for id, args := range s.GetNotifyAMQP() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewAMQPTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyES() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewElasticsearchTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue

		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyKafka() {
		if !args.Enable {
			continue
		}
		args.TLS.RootCAs = globalRootCAs
		newTarget, err := target.NewKafkaTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyMQTT() {
		if !args.Enable {
			continue
		}
		args.RootCAs = globalRootCAs
		newTarget, err := target.NewMQTTTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyMySQL() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewMySQLTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyNATS() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewNATSTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyNSQ() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewNSQTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyPostgres() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewPostgreSQLTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyRedis() {
		if !args.Enable {
			continue
		}
		newTarget, err := target.NewRedisTarget(id, args, GlobalServiceDoneCh)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		if err = targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	for id, args := range s.GetNotifyWebhook() {
		if !args.Enable {
			continue
		}
		args.RootCAs = globalRootCAs
		newTarget := target.NewWebhookTarget(id, args, GlobalServiceDoneCh)
		if err := targetList.Add(newTarget); err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
	}

	return targetList
}

// TestNotificationTargets tries to establish connections to all notification
// targets when enabled. This is a good way to make sure all configurations
// set by the user can work.
func (s serverConfig) TestNotificationTargets() error {
	for k, v := range s.GetNotifyAMQP() {
		if !v.Enable {
			continue
		}
		t, err := target.NewAMQPTarget(k, v, GlobalServiceDoneCh)
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
		v.TLS.RootCAs = globalRootCAs
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
		v.RootCAs = globalRootCAs
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
		t, err := target.NewMySQLTarget(k, v, GlobalServiceDoneCh)
		if err != nil {
			return fmt.Errorf("mysql(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyNATS() {
		if !v.Enable {
			continue
		}
		t, err := target.NewNATSTarget(k, v, GlobalServiceDoneCh)
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
		t, err := target.NewPostgreSQLTarget(k, v, GlobalServiceDoneCh)
		if err != nil {
			return fmt.Errorf("postgreSQL(%s): %s", k, err)
		}
		t.Close()
	}

	for k, v := range s.GetNotifyRedis() {
		if !v.Enable {
			continue
		}
		t, err := target.NewRedisTarget(k, v, GlobalServiceDoneCh)
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

	s[config.NotifyKafkaSubSys][kName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.KafkaBrokers: func() string {
			var brokers []string
			for _, broker := range cfg.Brokers {
				brokers = append(brokers, broker.String())
			}
			return strings.Join(brokers, ",")
		}(),
		target.KafkaTopic:         cfg.Topic,
		target.KafkaQueueDir:      cfg.QueueDir,
		target.KafkaQueueLimit:    strconv.Itoa(int(cfg.QueueLimit)),
		target.KafkaTLSEnable:     strconv.FormatBool(cfg.TLS.Enable),
		target.KafkaTLSSkipVerify: strconv.FormatBool(cfg.TLS.SkipVerify),
		target.KafkaTLSClientAuth: strconv.Itoa(int(cfg.TLS.ClientAuth)),
		target.KafkaSASLEnable:    strconv.FormatBool(cfg.SASL.Enable),
		target.KafkaSASLUsername:  cfg.SASL.User,
		target.KafkaSASLPassword:  cfg.SASL.Password,
	}
	return nil
}

func (s serverConfig) GetNotifyKafka() map[string]target.KafkaArgs {
	kafkaTargets := make(map[string]target.KafkaArgs)
	for k, kv := range s[config.NotifyKafkaSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		var err error
		var brokers []xnet.Host
		for _, s := range strings.Split(kv.Get(target.KafkaBrokers), ",") {
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

		queueLimit, err := strconv.Atoi(kv.Get(target.KafkaQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		clientAuth, err := strconv.Atoi(kv.Get(target.KafkaTLSClientAuth))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		kafkaArgs := target.KafkaArgs{
			Enable:     enabled,
			Brokers:    brokers,
			Topic:      kv.Get(target.KafkaTopic),
			QueueDir:   kv.Get(target.KafkaQueueDir),
			QueueLimit: uint64(queueLimit),
		}

		kafkaArgs.TLS.Enable = kv.Get(target.KafkaTLSEnable) == "true"
		kafkaArgs.TLS.SkipVerify = kv.Get(target.KafkaTLSSkipVerify) == "true"
		kafkaArgs.TLS.ClientAuth = tls.ClientAuthType(clientAuth)

		kafkaArgs.SASL.Enable = kv.Get(target.KafkaSASLEnable) == "true"
		kafkaArgs.SASL.User = kv.Get(target.KafkaSASLUsername)
		kafkaArgs.SASL.Password = kv.Get(target.KafkaSASLPassword)

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

	s[config.NotifyMQTTSubSys][mqttName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.MqttBroker:            cfg.Broker.String(),
		target.MqttTopic:             cfg.Topic,
		target.MqttQoS:               fmt.Sprintf("%d", cfg.QoS),
		target.MqttUsername:          cfg.User,
		target.MqttPassword:          cfg.Password,
		target.MqttReconnectInterval: cfg.MaxReconnectInterval.String(),
		target.MqttKeepAliveInterval: cfg.KeepAlive.String(),
		target.MqttQueueDir:          cfg.QueueDir,
		target.MqttQueueLimit:        strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) GetNotifyMQTT() map[string]target.MQTTArgs {
	mqttTargets := make(map[string]target.MQTTArgs)
	for k, kv := range s[config.NotifyMQTTSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}

		brokerURL, err := xnet.ParseURL(kv.Get(target.MqttBroker))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		reconnectInterval, err := time.ParseDuration(kv.Get(target.MqttReconnectInterval))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		keepAliveInterval, err := time.ParseDuration(kv.Get(target.MqttKeepAliveInterval))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.MqttQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		// Parse uint8 value
		qos, err := strconv.ParseUint(kv.Get(target.MqttQoS), 10, 8)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		mqttArgs := target.MQTTArgs{
			Enable:               enabled,
			Broker:               *brokerURL,
			Topic:                kv.Get(target.MqttTopic),
			QoS:                  byte(qos),
			User:                 kv.Get(target.MqttUsername),
			Password:             kv.Get(target.MqttPassword),
			MaxReconnectInterval: reconnectInterval,
			KeepAlive:            keepAliveInterval,
			RootCAs:              globalRootCAs,
			QueueDir:             kv.Get(target.MqttQueueDir),
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

	s[config.NotifyMySQLSubSys][sqlName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.MySQLFormat:     cfg.Format,
		target.MySQLDSNString:  cfg.DSN,
		target.MySQLTable:      cfg.Table,
		target.MySQLHost:       cfg.Host.String(),
		target.MySQLPort:       cfg.Port,
		target.MySQLUsername:   cfg.User,
		target.MySQLPassword:   cfg.Password,
		target.MySQLDatabase:   cfg.Database,
		target.MySQLQueueDir:   cfg.QueueDir,
		target.MySQLQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) GetNotifyMySQL() map[string]target.MySQLArgs {
	mysqlTargets := make(map[string]target.MySQLArgs)
	for k, kv := range s[config.NotifyMySQLSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		host, err := xnet.ParseURL(kv.Get(target.MySQLHost))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.MySQLQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		mysqlArgs := target.MySQLArgs{
			Enable:     enabled,
			Format:     kv.Get(target.MySQLFormat),
			DSN:        kv.Get(target.MySQLDSNString),
			Table:      kv.Get(target.MySQLTable),
			Host:       *host,
			Port:       kv.Get(target.MySQLPort),
			User:       kv.Get(target.MySQLUsername),
			Password:   kv.Get(target.MySQLPassword),
			Database:   kv.Get(target.MySQLDatabase),
			QueueDir:   kv.Get(target.MySQLQueueDir),
			QueueLimit: uint64(queueLimit),
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

	s[config.NotifyNATSSubSys][natsName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.NATSAddress:      cfg.Address.String(),
		target.NATSSubject:      cfg.Subject,
		target.NATSUsername:     cfg.Username,
		target.NATSPassword:     cfg.Password,
		target.NATSToken:        cfg.Token,
		target.NATSSecure:       strconv.FormatBool(cfg.Secure),
		target.NATSPingInterval: strconv.FormatInt(cfg.PingInterval, 10),
		target.NATSQueueDir:     cfg.QueueDir,
		target.NATSQueueLimit:   strconv.Itoa(int(cfg.QueueLimit)),
		target.NATSStreamingEnable: func() string {
			if cfg.Streaming.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.NATSStreamingClusterID:          cfg.Streaming.ClusterID,
		target.NATSStreamingAsync:              strconv.FormatBool(cfg.Streaming.Async),
		target.NATSStreamingMaxPubAcksInFlight: strconv.Itoa(cfg.Streaming.MaxPubAcksInflight),
	}

	return nil
}

func (s serverConfig) GetNotifyNATS() map[string]target.NATSArgs {
	natsTargets := make(map[string]target.NATSArgs)
	for k, kv := range s[config.NotifyNATSSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}

		address, err := xnet.ParseHost(kv.Get(target.NATSAddress))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		secure, err := parseBool(kv.Get(target.NATSSecure))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		pingInterval, err := strconv.ParseInt(kv.Get(target.NATSPingInterval), 10, 64)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.NATSQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		natsArgs := target.NATSArgs{
			Enable:       true,
			Address:      *address,
			Subject:      kv.Get(target.NATSSubject),
			Username:     kv.Get(target.NATSUsername),
			Password:     kv.Get(target.NATSPassword),
			Token:        kv.Get(target.NATSToken),
			Secure:       secure,
			PingInterval: pingInterval,
			QueueDir:     kv.Get(target.NATSQueueDir),
			QueueLimit:   uint64(queueLimit),
		}
		streamingEnabled := kv.Get(target.NATSStreamingEnable) == config.StateEnabled
		if streamingEnabled {
			async, err := parseBool(kv.Get(target.NATSStreamingAsync))
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			maxPubAcksInflight, err := strconv.Atoi(kv.Get(target.NATSStreamingMaxPubAcksInFlight))
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			natsArgs.Streaming.Enable = streamingEnabled
			natsArgs.Streaming.ClusterID = kv.Get(target.NATSStreamingClusterID)
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
	for k, kv := range s[config.NotifyNSQSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		nsqdAddress, err := xnet.ParseHost(kv.Get(target.NSQAddress))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		tlsEnable, err := parseBool(kv.Get(target.NSQTLSEnable))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		tlsSkipVerify, err := parseBool(kv.Get(target.NSQTLSSkipVerify))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		queueLimit, err := strconv.Atoi(kv.Get(target.NSQQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		nsqArgs := target.NSQArgs{
			Enable:      enabled,
			NSQDAddress: *nsqdAddress,
			Topic:       kv.Get(target.NSQTopic),
			QueueDir:    kv.Get(target.NSQQueueDir),
			QueueLimit:  uint64(queueLimit),
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

	s[config.NotifyNSQSubSys][nsqName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.NSQAddress:       cfg.NSQDAddress.String(),
		target.NSQTopic:         cfg.Topic,
		target.NSQTLSEnable:     strconv.FormatBool(cfg.TLS.Enable),
		target.NSQTLSSkipVerify: strconv.FormatBool(cfg.TLS.SkipVerify),
		target.NSQQueueDir:      cfg.QueueDir,
		target.NSQQueueLimit:    strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) GetNotifyPostgres() map[string]target.PostgreSQLArgs {
	psqlTargets := make(map[string]target.PostgreSQLArgs)
	for k, kv := range s[config.NotifyPostgresSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		host, err := xnet.ParseHost(kv.Get(target.PostgresHost))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.PostgresQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		psqlArgs := target.PostgreSQLArgs{
			Enable:           enabled,
			Format:           kv.Get(target.PostgresFormat),
			ConnectionString: kv.Get(target.PostgresConnectionString),
			Table:            kv.Get(target.PostgresTable),
			Host:             *host,
			Port:             kv.Get(target.PostgresPort),
			User:             kv.Get(target.PostgresUsername),
			Password:         kv.Get(target.PostgresPassword),
			Database:         kv.Get(target.PostgresDatabase),
			QueueDir:         kv.Get(target.PostgresQueueDir),
			QueueLimit:       uint64(queueLimit),
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

	s[config.NotifyPostgresSubSys][psqName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.PostgresFormat:           cfg.Format,
		target.PostgresConnectionString: cfg.ConnectionString,
		target.PostgresTable:            cfg.Table,
		target.PostgresHost:             cfg.Host.String(),
		target.PostgresPort:             cfg.Port,
		target.PostgresUsername:         cfg.User,
		target.PostgresPassword:         cfg.Password,
		target.PostgresDatabase:         cfg.Database,
		target.PostgresQueueDir:         cfg.QueueDir,
		target.PostgresQueueLimit:       strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) GetNotifyRedis() map[string]target.RedisArgs {
	redisTargets := make(map[string]target.RedisArgs)
	for k, kv := range s[config.NotifyRedisSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		addr, err := xnet.ParseHost(kv.Get(target.RedisAddress))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.RedisQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		redisArgs := target.RedisArgs{
			Enable:     enabled,
			Format:     kv.Get(target.RedisFormat),
			Addr:       *addr,
			Password:   kv.Get(target.RedisPassword),
			Key:        kv.Get(target.RedisKey),
			QueueDir:   kv.Get(target.RedisQueueDir),
			QueueLimit: uint64(queueLimit),
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

	s[config.NotifyRedisSubSys][redisName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.RedisFormat:     cfg.Format,
		target.RedisAddress:    cfg.Addr.String(),
		target.RedisPassword:   cfg.Password,
		target.RedisKey:        cfg.Key,
		target.RedisQueueDir:   cfg.QueueDir,
		target.RedisQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) SetNotifyWebhook(whName string, cfg target.WebhookArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyWebhookSubSys][whName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.WebhookEndpoint:   cfg.Endpoint.String(),
		target.WebhookQueueDir:   cfg.QueueDir,
		target.WebhookQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) GetNotifyWebhook() map[string]target.WebhookArgs {
	webhookTargets := make(map[string]target.WebhookArgs)
	for k, kv := range s[config.NotifyWebhookSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		url, err := xnet.ParseURL(kv.Get(target.WebhookEndpoint))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.WebhookQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		webhookArgs := target.WebhookArgs{
			Enable:     enabled,
			Endpoint:   *url,
			RootCAs:    globalRootCAs,
			QueueDir:   kv.Get(target.WebhookQueueDir),
			QueueLimit: uint64(queueLimit),
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

	s[config.NotifyESSubSys][esName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.ElasticFormat:     cfg.Format,
		target.ElasticURL:        cfg.URL.String(),
		target.ElasticIndex:      cfg.Index,
		target.ElasticQueueDir:   cfg.QueueDir,
		target.ElasticQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

func (s serverConfig) GetNotifyES() map[string]target.ElasticsearchArgs {
	esTargets := make(map[string]target.ElasticsearchArgs)
	for k, kv := range s[config.NotifyESSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		url, err := xnet.ParseURL(kv.Get(target.ElasticURL))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		queueLimit, err := strconv.Atoi(kv.Get(target.ElasticQueueLimit))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		esArgs := target.ElasticsearchArgs{
			Enable:     enabled,
			Format:     kv.Get(target.ElasticFormat),
			URL:        *url,
			Index:      kv.Get(target.ElasticIndex),
			QueueDir:   kv.Get(target.ElasticQueueDir),
			QueueLimit: uint64(queueLimit),
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
	for k, kv := range s[config.NotifyAMQPSubSys] {
		enabled := kv.Get(config.State) == config.StateEnabled
		if !enabled {
			continue
		}
		url, err := xnet.ParseURL(kv.Get(target.AmqpURL))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		deliveryMode, err := strconv.Atoi(kv.Get(target.AmqpDeliveryMode))
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		amqpArgs := target.AMQPArgs{
			Enable:       enabled,
			URL:          *url,
			Exchange:     kv.Get(target.AmqpExchange),
			RoutingKey:   kv.Get(target.AmqpRoutingKey),
			ExchangeType: kv.Get(target.AmqpExchangeType),
			DeliveryMode: uint8(deliveryMode),
			Mandatory:    kv.Get(target.AmqpMandatory) == "true",
			Immediate:    kv.Get(target.AmqpImmediate) == "true",
			Durable:      kv.Get(target.AmqpDurable) == "true",
			Internal:     kv.Get(target.AmqpInternal) == "true",
			NoWait:       kv.Get(target.AmqpNoWait) == "true",
			AutoDeleted:  kv.Get(target.AmqpAutoDeleted) == "true",
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

	s[config.NotifyAMQPSubSys][amqpName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
		target.AmqpURL:          cfg.URL.String(),
		target.AmqpExchange:     cfg.Exchange,
		target.AmqpRoutingKey:   cfg.RoutingKey,
		target.AmqpExchangeType: cfg.ExchangeType,
		target.AmqpDeliveryMode: strconv.Itoa(int(cfg.DeliveryMode)),
		target.AmqpMandatory:    strconv.FormatBool(cfg.Mandatory),
		target.AmqpInternal:     strconv.FormatBool(cfg.Immediate),
		target.AmqpDurable:      strconv.FormatBool(cfg.Durable),
		target.AmqpNoWait:       strconv.FormatBool(cfg.NoWait),
		target.AmqpAutoDeleted:  strconv.FormatBool(cfg.AutoDeleted),
	}

	return nil
}
